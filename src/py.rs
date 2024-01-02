use std::mem::ManuallyDrop;
use std::time::Instant;
use pyo3::prelude::PyModule;
use pyo3::{IntoPy, Py, PyErr, PyObject, PyResult, Python};
use pyo3::types::{IntoPyDict, PyDict};
use uuid::Uuid;
use crate::rd_config::TIMEOUT_MS;
use crate::rd_fn::{RpFn, RpFnLog};

/// ManuallyDrop for PyModule and db: PyObject
pub struct PyContext {
    py_rt: ManuallyDrop<Py<PyModule>>,
    py_db: ManuallyDrop<PyObject>,
    created: Instant,
    /// last tiem use
    ltu: Instant,
}


impl Drop for PyContext {
    fn drop(&mut self) {
        Python::with_gil(|_py| {
            unsafe {
                ManuallyDrop::drop(&mut self.py_rt);
                ManuallyDrop::drop(&mut self.py_db);
            }
        });
    }
}


#[derive(Debug, Clone)]
pub enum PyCall {
    /// scheduled local future call. this._fn must set
    Local(RpFnLog),
    /// to track a queue or able to return
    InProgressSaved(i64, Instant),
    InProgressUnSaved(Uuid, Instant),
    /// host id for future use
    Remote(i32, i64)
}


/// python module to import
pub const POSTGRES_PY: &str = "psycopg2";

pub const DB: &str = "DB";
pub const TOPIC: &str = "TOPIC";
pub const TABLE: &str = "TABLE";
pub const TRIG: &str = "TRIG";

impl PyContext {
    /// create Python runtime and DB connection
    #[inline]
    pub(crate) fn new(f: &RpFn, db_url: &String) -> Result<PyContext, PyErr> {

        let module: Py<PyModule> = Python::with_gil(|py| -> PyResult<_> {
            Ok(PyModule::import(py, POSTGRES_PY)?.into())
        })?;

        // let libpq_kv = format!("host=localhost sslmode=disable user={} password={} dbname={}", username, password, db);
        let client: PyObject = Python::with_gil(|py| -> PyResult<_> {
            Ok(module.as_ref(py).getattr("connect")?.call1((db_url,))?.into())
        })?;

        Ok(PyContext {
            py_rt: ManuallyDrop::new(module),
            py_db: ManuallyDrop::new(client),
            created: Instant::now(),
            ltu: Instant::now(),
        })
    }

    #[inline]
    pub(crate) fn alive(&self) -> bool {
        self.ltu.elapsed().as_millis() < TIMEOUT_MS as u128
    }


    pub fn dict<'a>(&self, x: &'a RpFnLog, fc: &'a RpFn, py: Python<'a>) -> &'a PyDict {
        let local = [
            (DB, self.py_db.as_ref(py)),
            // (TOPIC, fc.fns.topic.clone().into_py(py).as_ref(py)),
            // (TABLE, fc.fns.schema_table.clone().into_py(py).as_ref(py)),
            // (TRIG, x.trig_type.clone().into_py(py).as_ref(py)),
        ].to_vec();
        if let Some(pks) = &x.trig_value {
            for (pk, pk_val) in pks.iter() {
                // local.push((pk.to_ascii_uppercase().as_str(), pk_val.into_py(py).as_ref(py)));
            }
        }
        local[..].into_py_dict(py)
    }

    pub(crate) fn invoke(&mut self, x: &RpFnLog, fc: &RpFn) -> Result<(), String> {

        // pub fn invoke(&self, script: String,  fn_name: String,  table: String, env: HashMap<String, String>, pks: Vec<u64>) -> Result<(), String> {
        Python::with_gil(|py| {
            let mut locals = Vec::new();
            if let Some(pks) = &x.trig_value {
                for (pk, pk_val) in pks.iter() {
                    locals.push((pk.to_ascii_uppercase(), pk_val.into_py(py)));
                }
            }
            // println!("call with: {:?}", locals);
            for i in locals.len()..3 { locals.push((format!("_nil{}", i), "".into_py(py))); } // to avoid out of index
            let locals = [
                (DB, self.py_db.as_ref(py)),
                (TOPIC, fc.topic.clone().into_py(py).as_ref(py)),
                (TABLE, fc.schema_table.clone().into_py(py).as_ref(py)),
                (TRIG, x.trig_type.clone().into_py(py).as_ref(py)),
                (locals[0].0.as_str(), locals[0].1.as_ref(py)), // TODO fix the reference problem self.dict()
                (locals[1].0.as_str(), locals[1].1.as_ref(py)),
                (locals[2].0.as_str(), locals[2].1.as_ref(py)),
            ].into_py_dict(py);
            let local = self.dict(x, fc, py);

            let res = py.run(fc.code.as_str(), None, Some(locals));
            self.ltu = Instant::now();
            res
        }).map_err(|e| fc.err_msg(e, x))

    }

}

