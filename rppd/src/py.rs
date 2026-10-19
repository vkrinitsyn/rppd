#![allow(unused_variables, dead_code)]

use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::time::Instant;

use pyo3::{IntoPyObject, Py, PyAny, PyErr, PyResult, Python};
use pyo3::ffi::c_str;
use pyo3::prelude::PyModule;
use pyo3::types::{IntoPyDict, PyAnyMethods};
use uuid::Uuid;

use crate::rd_config::{RppdNodeCluster, SecretOpener, TIMEOUT_MS};
use crate::rd_fn::{RpFn, RpFnLog};
use crate::LP;

/// ManuallyDrop for PyModule and db: PyObject
pub struct PyContext {
    py_rt: ManuallyDrop<Py<PyModule>>,
    py_db: ManuallyDrop<Py<PyAny>>,
    rt_etcd: ManuallyDrop<Py<PyModule>>,
    py_etcd: Result<ManuallyDrop<Py<PyAny>>, PyErr>,
    /// Apache DataFusion (aka Ballista) BallistaSessionContext, exposed to python as `DF`.
    /// None if df_url is not configured or the "datafusion" feature is disabled; connection failures are logged and also become None
    #[cfg(feature = "datafusion")]
    py_df: Option<ManuallyDrop<Py<PyAny>>>,
    /// ClickHouse client, exposed to python as `CH`, for tables using
    /// on_commit_clickhouse. None if ch_url is not configured; a failed
    /// connection or a missing python package is logged and also becomes None
    py_ch: Option<ManuallyDrop<Py<PyAny>>>,
    // capture: Result<ManuallyDrop<Bound<PyModule>>, PyErr>,
    created: Instant,
    /// last tiem use
    ltu: Instant,
}


impl Drop for PyContext {
    fn drop(&mut self) {
        Python::attach(|_py| {
            unsafe {
                ManuallyDrop::drop(&mut self.py_rt);
                ManuallyDrop::drop(&mut self.py_db);
                ManuallyDrop::drop(&mut self.rt_etcd);
                if let Ok(etcd) = &mut self.py_etcd {
                    ManuallyDrop::drop(etcd);
                }
                if let Some(ch) = &mut self.py_ch {
                    ManuallyDrop::drop(ch);
                }
                #[cfg(feature = "datafusion")]
                if let Some(df) = &mut self.py_df {
                    ManuallyDrop::drop(df);
                }
            }
        });
    }
}

/// Queue configuration
#[derive(Debug, Clone)]
pub enum PyCall {
    /// scheduled local future call. this._fn must set
    Local(RpFnLog),
    /// to track a queue or able to return
    InProgressSaved(i64, Instant),
    /// if the fn exec progress not logged, than on master change the queue will lost
    InProgressUnSaved(Uuid, Instant),
    /// host id for future use
    RemoteSaved(i32, i64),
    RemoteUnSaved(i32, Uuid),
}


/// python module to import
pub const POSTGRES_PY: &str = "psycopg2";
pub const ETCD_PY: &str = "etcd3";
/// Apache DataFusion (aka Ballista) python client, see https://datafusion.apache.org/ballista/user-guide/python/quickstart.html
pub const DATAFUSION_PY: &str = "ballista";
/// ClickHouse python client, see https://clickhouse.com/docs/integrations/python
pub const CLICKHOUSE_PY: &str = "clickhouse_connect";

pub const DB: &str = "DB";
pub const ETCD: &str = "ETCD";
pub const DF: &str = "DF";
pub const CH: &str = "CH";
pub const TOPIC: &str = "TOPIC";
pub const TABLE: &str = "TABLE";
pub const TRIG: &str = "TRIG";
pub const VALUE: &str = "VALUE";
/// [skv] the secret store, for the names a function lists in `secret_names`
pub const SECRET: &str = "SECRET";
/// [otel] the call's own W3C traceparent, to continue its trace (rd_config.rs)
pub const TRACE: &str = "TRACE";

/// [skv] The host's secret opener (ytserv's secret store), set through
/// `ConfigurationRequest::Secrets`. One per process, like the embedded runtime.
static SECRET_OPENER: std::sync::RwLock<Option<SecretOpener>> = std::sync::RwLock::new(None);

pub(crate) fn set_secret_opener(o: Option<SecretOpener>) {
    *SECRET_OPENER.write().unwrap_or_else(|e| e.into_inner()) = o;
}

/// [skv] `SECRET` in a function's locals: `SECRET.get("name")` -> str,
/// `SECRET.get_bytes("name")` -> bytes. Only for the names the function lists in
/// `rppd_function.secret_names` - whoever may write that table decides, so its
/// GRANTs are the boundary (README). The value goes from the host's memory into
/// this interpreter and nowhere else: not logged, not in `rppd_function_log`.
#[pyo3::pyclass(name = "Secrets")]
pub struct Secrets {
    names: Vec<String>,
}

impl Secrets {
    fn open(&self, name: &str) -> PyResult<Vec<u8>> {
        if !self.names.iter().any(|n| n == name) {
            return Err(pyo3::exceptions::PyPermissionError::new_err(format!(
                "secret {:?} is not in this function's secret_names", name)));
        }
        let o = SECRET_OPENER.read().unwrap_or_else(|e| e.into_inner()).clone();
        let o = o.ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(
            "no secret store in this host (rppd runs outside ytserv, or ytserv has none)"))?;
        o(name).map_err(pyo3::exceptions::PyKeyError::new_err)
    }
}

#[pyo3::pymethods]
impl Secrets {
    /// The value as text (UTF-8).
    fn get(&self, name: &str) -> PyResult<String> {
        String::from_utf8(self.open(name)?).map_err(|_| pyo3::exceptions::PyValueError::new_err(format!(
            "secret {:?} is not UTF-8 text - use SECRET.get_bytes", name)))
    }

    /// The value as bytes.
    fn get_bytes<'py>(&self, py: Python<'py>, name: &str) -> PyResult<pyo3::Bound<'py, pyo3::types::PyBytes>> {
        Ok(pyo3::types::PyBytes::new(py, &self.open(name)?))
    }

    /// The names this function may open.
    fn names(&self) -> Vec<String> {
        self.names.clone()
    }
}

/// `secret_names` as written: names separated by commas or whitespace.
pub(crate) fn secret_names(s: Option<&str>) -> Vec<String> {
    s.unwrap_or("").split(|c: char| c == ',' || c.is_whitespace()).filter(|n| !n.is_empty()).map(|n| n.to_string()).collect()
}

impl RppdNodeCluster {
    /// create Python runtime and DB connection
    #[inline]
    pub(crate) async fn new_py_context(&self, f: &RpFn) -> Result<PyContext, PyErr> {
        let db_url = self.cfg.read().await.db_url();
        let module: Py<PyModule> = Python::attach(|py| -> PyResult<_> {
            Ok(PyModule::import(py, POSTGRES_PY)?.into())
        })?;

        // connect to DB
        // let libpq_kv = format!("host=localhost sslmode=disable user={} password={} dbname={}", username, password, db);
        let client: Py<PyAny> = Python::attach(|py| -> PyResult<_> {
            Ok(module.bind_borrowed(py).getattr("connect")?.call1((&db_url,))?.into())
        })?;

        let etcd_module: Py<PyModule> = Python::attach(|py| -> PyResult<_> {
            Ok(PyModule::import(py, ETCD_PY)?.into())
        })?;

        let (host, port) = {
            let etcd = self.etcd.read().await;
            (etcd.host.clone(), etcd.port.clone())
        };

        // connect to ETCD
        // see https://github.com/lupko/etcd3-client/blob/master/src/etcd3/client.py#L120
        let etcd_client: Result<Py<PyAny>, PyErr> = Python::attach(|py| -> PyResult<_> {
            Ok(etcd_module.bind_borrowed(py).getattr("client")?
                .call( (host.clone(), port.clone()), None)?.into())
        });
        
        if let Err(e) = &etcd_client {
            slog::error!(self.log, "{}failed to connect to etcd v3 [{}:{}]: {}", LP, host, port, e);
        }

        // connect to Apache DataFusion (aka Ballista) scheduler, only if configured
        #[cfg(feature = "datafusion")]
        let py_df: Option<ManuallyDrop<Py<PyAny>>> = match self.cfg.read().await.df_url() {
            None => None,
            Some(url) => match Python::attach(|py| -> PyResult<Py<PyAny>> {
                Ok(PyModule::import(py, DATAFUSION_PY)?
                    .getattr("BallistaSessionContext")?
                    .call1((url.as_str(),))?
                    .into())
            }) {
                Ok(ctx) => Some(ManuallyDrop::new(ctx)),
                Err(e) => {
                    slog::error!(self.log, "{}failed to connect to datafusion/ballista scheduler [{}]: {}", LP, url, e);
                    None
                }
            }
        };

        // connect to ClickHouse, only if configured
        let py_ch: Option<ManuallyDrop<Py<PyAny>>> = match self.cfg.read().await.ch_url() {
            None => None,
            Some(url) => match Python::attach(|py| -> PyResult<Py<PyAny>> {
                Ok(PyModule::import(py, CLICKHOUSE_PY)?
                    .getattr("get_client")?
                    .call((), Some(&[("dsn", url.as_str())].into_py_dict(py)?))?
                    .into())
            }) {
                Ok(c) => Some(ManuallyDrop::new(c)),
                Err(e) => {
                    slog::error!(self.log, "{}failed to connect to ClickHouse [{}]: {}", LP, url, e);
                    None
                }
            }
        };

        Ok(PyContext {
            py_rt: ManuallyDrop::new(module),
            py_db: ManuallyDrop::new(client),
            rt_etcd: ManuallyDrop::new(etcd_module),
            py_etcd: etcd_client.map(|p| ManuallyDrop::new(p)),
            #[cfg(feature = "datafusion")]
            py_df,
            py_ch,
            created: Instant::now(),
            ltu: Instant::now(),
        })
    }

}
const STDOUT_CAPTURE_CLASS: &str = r#"
class StdoutCapture:
    def __init__(self):
        self.output = ""

    def write(self, text):
        self.output += text

    def flush(self):
        pass
"#;

impl PyContext {
    #[inline]
    pub(crate) fn alive(&self) -> bool {
        self.ltu.elapsed().as_millis() < TIMEOUT_MS as u128
    }


    pub(crate) fn invoke(&mut self, x: &RpFnLog, fc: &RpFn) -> Result<String, String> {
        Python::attach(|py| {
            let locals = [
                (DB, self.py_db.bind_borrowed(py)),
                (TOPIC, fc.topic.clone().into_pyobject(py)?.into_any().as_borrowed()),
                (TABLE, fc.schema_table.clone().into_pyobject(py)?.into_any().as_borrowed()),
                (TRIG, x.trig_type.clone().into_pyobject(py)?.into_any().as_borrowed()),
            ].into_py_dict(py)?;
            
            if let Some(pks) = &x.trig_value {
                for (pk, pk_val) in pks.iter() {
                    let _ = locals.set_item(pk.to_ascii_uppercase(), pk_val.into_pyobject(py)?.into_any().as_borrowed())?;
                }
            }
            
            if let Some(pks) = &x.value {
                let _ = locals.set_item(VALUE, pks.into_pyobject(py)?.into_any().as_borrowed())?;
            }
            
            if let Ok(e) = &self.py_etcd {
                let _ = locals.set_item(ETCD, e.bind_borrowed(py))?;
            }

            #[cfg(feature = "datafusion")]
            if let Some(df) = &self.py_df {
                let _ = locals.set_item(DF, df.bind_borrowed(py))?;
            }

            // alongside DB, for a table with on_commit_clickhouse set
            if let Some(ch) = &self.py_ch {
                let _ = locals.set_item(CH, ch.bind_borrowed(py))?;
            }

            // [otel] the trace this call is a span of
            if let Some(tp) = &x.traceparent {
                let _ = locals.set_item(TRACE, tp.clone().into_pyobject(py)?.into_any().as_borrowed())?;
            }

            // [skv] the secret store, for the names this function lists
            let names = secret_names(fc.secret_names.as_deref());
            if !names.is_empty() {
                let _ = locals.set_item(SECRET, pyo3::Bound::new(py, Secrets { names })?)?;
            }

            let capture_instance = if fc.fn_logging {
                let capture_instance =   PyModule::from_code(
                    py,
                    CString::new(STDOUT_CAPTURE_CLASS)?.as_c_str(),
                    c_str!("capture.py"),
                    c_str!("capture"),
                )?.getattr("StdoutCapture")?.call0()?;

                // Set sys.stdout to the capture instance
                let sys = py.import("sys")?;
                sys.setattr("stdout", &capture_instance)?;
                Some(capture_instance)
            } else {
                None
            };

            let res = py.run(fc.code()?.as_c_str(), None, Some(&locals));
            self.ltu = Instant::now();

            let out = match capture_instance {
                Some(ci) => match ci.getattr("output") {
                    Ok(c) => c.extract::<String>()
                        .unwrap_or_else(|out| format!("err: {}", out)),
                    Err(e) => format!("err: {}", e)
                }
                None => "".to_string()
            };

            res.map(|_| out)
        }).map_err(|e| fc.err_msg(e, x))
    }
}


#[cfg(test)]
mod skv_tests {
    use super::*;

    #[test]
    fn secret_names_split_on_commas_and_spaces() {
        assert_eq!(secret_names(Some("db_password, api_token  llm_key")), vec!["db_password", "api_token", "llm_key"]);
        assert!(secret_names(None).is_empty());
        assert!(secret_names(Some(" , ")).is_empty());
    }

    #[test]
    fn a_function_opens_only_what_it_lists_and_only_with_an_opener() {
        set_secret_opener(None);
        let s = Secrets { names: vec!["db_password".into()] };
        assert!(s.open("db_password").is_err(), "no opener in this host");
        set_secret_opener(Some(std::sync::Arc::new(|n: &str| if n == "db_password" { Ok(b"s3cret".to_vec()) } else { Err(format!("no secret named {}", n)) })));
        assert_eq!(s.open("db_password").unwrap(), b"s3cret");
        assert!(s.open("api_token").is_err(), "not listed");
        set_secret_opener(None);
    }
}
