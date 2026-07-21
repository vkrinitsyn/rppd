#![allow(unused_variables, dead_code)]

use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::time::Instant;

use pyo3::{IntoPyObject, Py, PyAny, PyErr, PyResult, Python};
use pyo3::ffi::c_str;
use pyo3::prelude::PyModule;
use pyo3::types::{IntoPyDict, PyAnyMethods};
use uuid::Uuid;

use crate::rd_config::{RppdNodeCluster, TIMEOUT_MS};
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

pub const DB: &str = "DB";
pub const ETCD: &str = "ETCD";
pub const DF: &str = "DF";
pub const TOPIC: &str = "TOPIC";
pub const TABLE: &str = "TABLE";
pub const TRIG: &str = "TRIG";
pub const VALUE: &str = "VALUE";

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

        Ok(PyContext {
            py_rt: ManuallyDrop::new(module),
            py_db: ManuallyDrop::new(client),
            rt_etcd: ManuallyDrop::new(etcd_module),
            py_etcd: etcd_client.map(|p| ManuallyDrop::new(p)),
            #[cfg(feature = "datafusion")]
            py_df,
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

