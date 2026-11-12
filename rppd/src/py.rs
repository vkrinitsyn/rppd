#![allow(unused_variables, dead_code)]

use std::ffi::CString;
use std::mem::ManuallyDrop;
use std::time::Instant;

use pyo3::{IntoPyObject, Py, PyAny, PyErr, PyResult, Python};
use pyo3::ffi::c_str;
use pyo3::prelude::PyModule;
use pyo3::types::{IntoPyDict, PyAnyMethods};
use uuid::Uuid;

use crate::rd_config::{RppdNodeCluster, SecretChecker, SecretOpener, TIMEOUT_MS};
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
// a queue line holds mostly Local calls: a Box for each would cost more than the size it saves
#[allow(clippy::large_enum_variant)]
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

/// [rotate-key] The host's check (`ConfigurationRequest::SecretCheck`), as the opener
static SECRET_CHECKER: std::sync::RwLock<Option<SecretChecker>> = std::sync::RwLock::new(None);

pub(crate) fn set_secret_checker(c: Option<SecretChecker>) {
    *SECRET_CHECKER.write().unwrap_or_else(|e| e.into_inner()) = c;
}

/// [skv] `SECRET` in a function's locals: `SECRET.get("name")` -> str,
/// `SECRET.get_bytes("name")` -> bytes, [rotate-key] `SECRET.check("name", value)` ->
/// bool (is a presented str or bytes the secret's). Only for the names the function lists in
/// `rppd_function.secret_names` - whoever may write that table decides, so its
/// GRANTs are the boundary (README). The value goes from the host's memory into
/// this interpreter and nowhere else: not logged, not in `rppd_function_log`.
#[pyo3::pyclass(name = "Secrets")]
pub struct Secrets {
    names: Vec<String>,
}

impl Secrets {
    fn listed(&self, name: &str) -> PyResult<()> {
        if !self.names.iter().any(|n| n == name) {
            return Err(pyo3::exceptions::PyPermissionError::new_err(tr!("py.not_listed", name = format!("{:?}", name))));
        }
        Ok(())
    }

    fn open(&self, name: &str) -> PyResult<Vec<u8>> {
        self.listed(name)?;
        let o = SECRET_OPENER.read().unwrap_or_else(|e| e.into_inner()).clone();
        let o = o.ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(tr!("py.no_store")))?;
        o(name).map_err(pyo3::exceptions::PyKeyError::new_err)
    }

    /// [rotate-key] `presented` against the secret, by the host
    fn check_raw(&self, name: &str, presented: &[u8]) -> PyResult<bool> {
        self.listed(name)?;
        let c = SECRET_CHECKER.read().unwrap_or_else(|e| e.into_inner()).clone();
        let c = c.ok_or_else(|| pyo3::exceptions::PyRuntimeError::new_err(tr!("py.no_store")))?;
        c(name, presented).map_err(pyo3::exceptions::PyKeyError::new_err)
    }
}

#[pyo3::pymethods]
impl Secrets {
    /// The value as text (UTF-8).
    fn get(&self, name: &str) -> PyResult<String> {
        String::from_utf8(self.open(name)?).map_err(|_| pyo3::exceptions::PyValueError::new_err(tr!("py.not_text", name = format!("{:?}", name))))
    }

    /// The value as bytes.
    fn get_bytes<'py>(&self, py: Python<'py>, name: &str) -> PyResult<pyo3::Bound<'py, pyo3::types::PyBytes>> {
        Ok(pyo3::types::PyBytes::new(py, &self.open(name)?))
    }

    /// The names this function may open.
    fn names(&self) -> Vec<String> {
        self.names.clone()
    }

    /// [rotate-key] Is `value` - str or bytes - the secret's? While the secret rotates,
    /// the old and the new value both are. What a function checks a presented key with:
    /// `SECRET.get` gives the new value only.
    fn check(&self, name: &str, value: &pyo3::Bound<'_, pyo3::PyAny>) -> PyResult<bool> {
        let raw: Vec<u8> = match value.extract::<String>() {
            Ok(s) => s.into_bytes(),
            Err(_) => value.extract::<Vec<u8>>().map_err(|_| pyo3::exceptions::PyTypeError::new_err(tr!("py.check_type")))?,
        };
        self.check_raw(name, &raw)
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
            slog::error!(self.log, "{}{}", LP, tr!("py.etcd", host = host, port = port, err = e));
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
                    slog::error!(self.log, "{}{}", LP, tr!("py.df", url = url, err = e));
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
                    slog::error!(self.log, "{}{}", LP, tr!("py.clickhouse", url = url, err = e));
                    None
                }
            }
        };

        Ok(PyContext {
            py_rt: ManuallyDrop::new(module),
            py_db: ManuallyDrop::new(client),
            rt_etcd: ManuallyDrop::new(etcd_module),
            py_etcd: etcd_client.map(ManuallyDrop::new),
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
                (TRIG, x.trig_type.into_pyobject(py)?.into_any().as_borrowed()),
            ].into_py_dict(py)?;
            
            if let Some(pks) = &x.trig_value {
                for (pk, pk_val) in pks.iter() {
                    locals.set_item(pk.to_ascii_uppercase(), pk_val.into_pyobject(py)?.into_any().as_borrowed())?;
                }
            }
            
            if let Some(pks) = &x.value {
                locals.set_item(VALUE, pks.into_pyobject(py)?.into_any().as_borrowed())?;
            }
            
            if let Ok(e) = &self.py_etcd {
                locals.set_item(ETCD, e.bind_borrowed(py))?;
            }

            #[cfg(feature = "datafusion")]
            if let Some(df) = &self.py_df {
                let _ = locals.set_item(DF, df.bind_borrowed(py))?;
            }

            // alongside DB, for a table with on_commit_clickhouse set
            if let Some(ch) = &self.py_ch {
                locals.set_item(CH, ch.bind_borrowed(py))?;
            }

            // [otel] the trace this call is a span of
            if let Some(tp) = &x.traceparent {
                locals.set_item(TRACE, tp.clone().into_pyobject(py)?.into_any().as_borrowed())?;
            }

            // [skv] the secret store, for the names this function lists
            let names = secret_names(fc.secret_names.as_deref());
            if !names.is_empty() {
                locals.set_item(SECRET, pyo3::Bound::new(py, Secrets { names })?)?;
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

            // one namespace, as a module's: the code's own defs see its imports, DB and the
            // rest, as its statements do (a test sees the same, `test`)
            if !pyo3::types::PyDictMethods::contains(&locals, "__builtins__")? {
                locals.set_item("__builtins__", py.import("builtins")?)?;
            }
            locals.set_item("__name__", "__main__")?;
            let res = py.run(fc.code()?.as_c_str(), Some(&locals), None);
            self.ltu = Instant::now();

            let out = match capture_instance {
                Some(ci) => match ci.getattr("output") {
                    Ok(c) => c.extract::<String>()
                        .unwrap_or_else(|out| tr!("py.output", err = out)),
                    Err(e) => tr!("py.output", err = e)
                }
                None => "".to_string()
            };
            // its defs hold it as their globals: cleared, not left to the collector
            pyo3::types::PyDictMethods::clear(&locals);

            res.map(|_| out)
        }).map_err(|e| fc.err_msg(e, x))
    }
}

/// [test] What a check runs of a function's code: its definitions - the top-level
/// imports, defs, classes and literal constants (to plain names) - never its body; then
/// `test(level)` (`test(level=...)`, `test()` when it takes none; awaited when async). Its `print` writes to the check's own buffer - not
/// sys.stdout, which every call of the process shares. `(what it returned, a test was
/// found, its output, why it raised)`.
const TEST_RUNNER: &str = r#"
import ast
import asyncio
import inspect
import io
import traceback

KEEP = (ast.Import, ast.ImportFrom, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)


def named(t):
    return isinstance(t, ast.Name) or (isinstance(t, (ast.Tuple, ast.List)) and all(named(e) for e in t.elts))


def literal(n):
    """an assignment of a literal - a number, a string, a tuple, list, dict or set of them,
    as ast.literal_eval reads it, nothing executed - to plain names"""
    if isinstance(n, ast.Assign):
        targets, value = n.targets, n.value
    elif isinstance(n, ast.AnnAssign):
        targets, value = [n.target], n.value
    else:
        return False
    if value is None or not all(named(t) for t in targets):
        return False
    try:
        ast.literal_eval(value)
        return True
    except Exception:
        return False


def definitions(code, name):
    tree = ast.parse(code, name)
    tree.body = [n for n in tree.body if isinstance(n, KEEP) or literal(n)]
    return compile(tree, name, "exec")


def run(code, name, ns, level):
    out = io.StringIO()

    def own_print(*a, **k):
        k.setdefault("file", out)
        print(*a, **k)

    ns["print"] = own_print
    try:
        exec(definitions(code, name), ns)
        t = ns.get("test")
        if not callable(t):
            return None, False, out.getvalue(), None
        try:
            params = inspect.signature(t).parameters
        except (TypeError, ValueError):
            params = None
        if params is not None and "level" in params:
            r = t(level=level)
        elif params is None or len(params) > 0:
            r = t(level)
        else:
            r = t()
        if inspect.iscoroutine(r):
            r = asyncio.run(r)
        return r, True, out.getvalue(), None
    except BaseException as e:
        why = "%s: %s\n%s" % (type(e).__name__, e, "".join(traceback.format_tb(e.__traceback__)))
        return None, True, out.getvalue(), why.rstrip()
    finally:
        # its defs hold ns as their globals: a cycle that would keep DB, the clients and
        # what the test made until Python's collector ran
        ns.clear()
"#;

/// [test] The test of `code` (function `id`) at `level` in `ns`, a namespace of its own:
/// `ns` is its globals, so its defs see what `ns` holds (DB, ETCD, SECRET), as the body's
/// statements do. Passed when it returned 0; failed when it returned anything else, raised,
/// or there is no `test` in the code. (its outcome, what it printed)
pub(crate) fn run_test(py: Python<'_>, ns: &pyo3::Bound<'_, pyo3::types::PyDict>, code: &str, id: i32, level: i32) -> (Result<(), String>, String) {
    use pyo3::types::PyDictMethods;
    let r = (|| -> PyResult<(Result<(), String>, String)> {
        if !ns.contains("__builtins__")? {
            ns.set_item("__builtins__", py.import("builtins")?)?;
        }
        ns.set_item("__name__", "rppd_test")?;
        let runner = PyModule::from_code(py, CString::new(TEST_RUNNER)?.as_c_str(), c_str!("rppd_test.py"), c_str!("rppd_test"))?;
        let (got, found, out, why): (pyo3::Bound<'_, PyAny>, bool, String, Option<String>) =
            runner.getattr("run")?.call1((code, format!("rppd_fn_{}", id), ns, level))?.extract()?;
        Ok((match why {
            Some(why) => Err(why),
            None => test_outcome(&got, found),
        }, out))
    })();
    r.unwrap_or_else(|e| (Err(e.to_string()), String::new()))
}

/// [test] what `test` returned, as passed or failed: 0 is passed, any other value failed
fn test_outcome(got: &pyo3::Bound<'_, PyAny>, found: bool) -> Result<(), String> {
    if !found {
        return Err(tr!("py.no_test"));
    }
    match got.extract::<i64>() {
        Ok(0) => Ok(()),
        Ok(code) => Err(tr!("py.test_code", value = code)),
        Err(_) => Err(tr!("py.test_code", value = got.repr().map(|r| r.to_string()).unwrap_or_else(|_| "?".into()))),
    }
}

impl PyContext {
    /// [test] Its connections closed: a test's context is its own, made for it and gone
    /// after (rd_test.rs)
    pub(crate) fn close(&mut self) {
        Python::attach(|py| {
            let _ = self.py_db.bind_borrowed(py).call_method0("close");
            if let Ok(e) = &self.py_etcd {
                let _ = e.bind_borrowed(py).call_method0("close");
            }
            if let Some(ch) = &self.py_ch {
                let _ = ch.bind_borrowed(py).call_method0("close");
            }
        });
    }

    /// [test] The function's test(level) (rd_test.rs): its code's definitions run, never its
    /// body, in a namespace of its own that holds what a call is handed (DB, ETCD, DF, CH,
    /// SECRET, TOPIC, TABLE) - no event, so no TRIG, keys or VALUE. Whatever it left open
    /// in DB is rolled back. (its outcome, its stdout)
    pub(crate) fn test(&mut self, fc: &RpFn, level: i32) -> (Result<(), String>, String) {
        Python::attach(|py| {
            let ns = (|| -> PyResult<pyo3::Bound<'_, pyo3::types::PyDict>> {
                let ns = [
                    (DB, self.py_db.bind_borrowed(py)),
                    (TOPIC, fc.topic.clone().into_pyobject(py)?.into_any().as_borrowed()),
                    (TABLE, fc.schema_table.clone().into_pyobject(py)?.into_any().as_borrowed()),
                ].into_py_dict(py)?;
                if let Ok(e) = &self.py_etcd {
                    ns.set_item(ETCD, e.bind_borrowed(py))?;
                }
                #[cfg(feature = "datafusion")]
                if let Some(df) = &self.py_df {
                    ns.set_item(DF, df.bind_borrowed(py))?;
                }
                if let Some(ch) = &self.py_ch {
                    ns.set_item(CH, ch.bind_borrowed(py))?;
                }
                let names = secret_names(fc.secret_names.as_deref());
                if !names.is_empty() {
                    ns.set_item(SECRET, pyo3::Bound::new(py, Secrets { names })?)?;
                }
                Ok(ns)
            })();
            let r = match ns {
                Ok(ns) => run_test(py, &ns, &fc.code, fc.id, level),
                Err(e) => (Err(e.to_string()), String::new()),
            };
            let _ = self.py_db.bind_borrowed(py).call_method0("rollback");
            self.ltu = Instant::now();
            r
        })
    }
}


#[cfg(test)]
mod skv_tests {
    use super::*;

    /// [test] a check of `code` at `level`, with a DB that counts its rollbacks
    fn check(code: &str, level: i32) -> (Result<(), String>, String, Vec<String>) {
        Python::attach(|py| {
            let fake = PyModule::from_code(py, c_str!("class Db:\n    def __init__(self):\n        self.calls = []\n    def rollback(self):\n        self.calls.append('rollback')\n    def cursor(self):\n        self.calls.append('cursor')\n        return self\n"),
                c_str!("fake.py"), c_str!("fake")).unwrap();
            let db = fake.getattr("Db").unwrap().call0().unwrap();
            let ns = [(DB, db.clone())].into_py_dict(py).unwrap();
            ns.set_item("BODY", pyo3::types::PyList::empty(py)).unwrap();
            let (r, out) = run_test(py, &ns, code, 7, level);
            let calls: Vec<String> = db.getattr("calls").unwrap().extract().unwrap();
            (r, out, calls)
        })
    }

    /// [test] only the definitions run: the body never does, test() sees the imports, the
    /// constants and what a call is handed (DB), and gets the level
    #[test]
    fn a_check_runs_test_and_never_the_body() {
        let code = "import os\nLIMIT = 3\n\ndef test(level: int = 0) -> int:\n    DB.cursor()\n    print('level', level, os.sep, LIMIT)\n    return 0 if level < LIMIT else 9\n\nBODY.append('ran')\nprint('the body')\nraise SystemExit('the body ran')\n";
        let (r, out, calls) = check(code, 2);
        assert_eq!(r, Ok(()), "{}", out);
        assert_eq!(out.trim(), "level 2 / 3", "test's own output, none of the body's");
        assert_eq!(calls, ["cursor"], "test used DB");
        let (r, ..) = check(code, 5);
        assert_eq!(r, Err(tr!("py.test_code", value = 9)), "a code not 0 is a failure, the level reached it");
    }

    /// [test] 0 passes; another number, None, an exception or no test fails - with its text
    #[test]
    fn a_check_fails_on_anything_but_0() {
        assert_eq!(check("def test():\n    return 0\n", 1).0, Ok(()), "test() without a level");
        assert_eq!(check("def test(level):\n    return 4\n", 0).0, Err(tr!("py.test_code", value = 4)));
        assert_eq!(check("def test(level):\n    pass\n", 0).0, Err(tr!("py.test_code", value = "None")), "None is not 0");
        assert_eq!(check("x = 1\nprint(x)\n", 0).0, Err(tr!("py.no_test")), "no test, and the body not run");
        let e = check("def test(level):\n    raise ValueError('the sink is gone')\n", 0).0.unwrap_err();
        assert!(e.contains("ValueError: the sink is gone") && e.contains("line 2"), "{}", e);
        let e = check("def test(level):\n    return (\n", 0).0.unwrap_err();
        assert!(e.contains("SyntaxError"), "a syntax error fails the check: {}", e);
    }

    /// [test] literal constants of any shape run - a negative number, a tuple, a dict, a
    /// pair of names - and nothing that executes code; test(*, level), async test too
    #[test]
    fn a_check_keeps_literals_and_takes_every_test() {
        let code = "LIMIT = -1\nTABLES = ('a', 'b')\nMAP = {'k': [1, 2]}\nA, B = 1, 2\nN: int = 5\nSIDE = BODY.append('ran')\n\ndef test(level):\n    return 0 if (LIMIT, TABLES, MAP['k'], A + B, N) == (-1, ('a', 'b'), [1, 2], 3, 5) and 'SIDE' not in globals() else 1\n";
        assert_eq!(check(code, 0).0, Ok(()));
        assert_eq!(check("def test(*, level=0):\n    return level - 3\n", 3).0, Ok(()), "a keyword-only level");
        assert_eq!(check("async def test(level):\n    return level\n", 0).0, Ok(()), "an async test, awaited");
        assert_eq!(check("async def test(level):\n    return level\n", 7).0, Err(tr!("py.test_code", value = 7)));
    }

    /// [test] after a check its namespace is cleared: nothing it held stays, for the
    /// collector or otherwise
    #[test]
    fn a_check_leaves_its_namespace_empty() {
        Python::attach(|py| {
            let ns = [(DB, 1i32.into_pyobject(py).unwrap().into_any())].into_py_dict(py).unwrap();
            let _ = run_test(py, &ns, "def test(level):\n    return 0\n", 7, 0);
            assert_eq!(pyo3::types::PyDictMethods::len(&ns), 0);
        });
    }

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

    /// [rotate-key] SECRET.check: the host's answer, for the listed names only, and
    /// only with a checker
    #[test]
    fn a_function_checks_a_value_only_for_what_it_lists() {
        set_secret_checker(None);
        let s = Secrets { names: vec!["api_key".into()] };
        assert!(s.check_raw("api_key", b"old").is_err(), "no checker in this host");
        set_secret_checker(Some(std::sync::Arc::new(|n: &str, v: &[u8]| match n {
            "api_key" => Ok(v == b"old" || v == b"new"),
            _ => Err(format!("no secret named {}", n)),
        })));
        assert!(s.check_raw("api_key", b"old").unwrap(), "the old value, during a rotation");
        assert!(s.check_raw("api_key", b"new").unwrap());
        assert!(!s.check_raw("api_key", b"other").unwrap());
        assert!(s.check_raw("db_password", b"old").is_err(), "not listed");
        set_secret_checker(None);
    }

    /// [pyo3] SECRET as a function's code meets it: a Secrets in the interpreter, in
    /// the locals as `invoke` puts it there, its methods called from Python. Only the
    /// checks made before the secret store, so SECRET_OPENER stays the test above's
    #[test]
    fn a_function_sees_secret_in_the_interpreter() {
        let r: PyResult<(Vec<String>, String, String)> = Python::attach(|py| {
            let locals = pyo3::types::PyDict::new(py);
            locals.set_item(SECRET, pyo3::Bound::new(py, Secrets { names: vec!["db_password".into()] })?)?;
            py.run(c_str!("names = SECRET.names()\n\
                try:\n    SECRET.get('api_token')\n    text = 'opened'\nexcept PermissionError as e:\n    text = str(e)\n\
                try:\n    SECRET.get_bytes('api_token')\n    raw = 'opened'\nexcept PermissionError as e:\n    raw = str(e)\n"),
                None, Some(&locals))?;
            Ok((locals.get_item("names")?.extract()?, locals.get_item("text")?.extract()?, locals.get_item("raw")?.extract()?))
        });
        let (names, text, raw) = r.unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(names, vec!["db_password"]);
        // in the messages' language, whichever the environment names
        for m in [text, raw] {
            assert_eq!(m, tr!("py.not_listed", name = format!("{:?}", "api_token")));
        }
    }

    /// [rotate-key] SECRET.check from Python: an unlisted name refused, a value neither
    /// str nor bytes refused - both before the secret store, so no checker is needed
    #[test]
    fn a_function_meets_secret_check_in_the_interpreter() {
        let r: PyResult<(String, String)> = Python::attach(|py| {
            let locals = pyo3::types::PyDict::new(py);
            locals.set_item(SECRET, pyo3::Bound::new(py, Secrets { names: vec!["api_key".into()] })?)?;
            py.run(c_str!("try:\n    SECRET.check('db_password', 'x')\n    other = 'checked'\nexcept PermissionError as e:\n    other = str(e)\n\
                try:\n    SECRET.check('api_key', 42)\n    kind = 'checked'\nexcept TypeError as e:\n    kind = str(e)\n"),
                None, Some(&locals))?;
            Ok((locals.get_item("other")?.extract()?, locals.get_item("kind")?.extract()?))
        });
        let (other, kind) = r.unwrap_or_else(|e| panic!("{}", e));
        assert_eq!(other, tr!("py.not_listed", name = format!("{:?}", "db_password")));
        assert_eq!(kind, tr!("py.check_type"));
    }
}
