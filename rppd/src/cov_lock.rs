//! The one lock every test that starts an embedded etcd takes for its whole run: the etcd's peer
//! port is fixed, so two clusters in one test binary would collide.

pub(crate) static ETCD_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Stand-in `psycopg2` and `etcd3` modules in `sys.modules`, once per process: the drivers are not
/// installed where the tests run, and a call made here never reaches a server. py.rs calls only
/// `cursor`, `rollback` and `close` on the database connection, and `close` on the etcd client.
pub(crate) fn install_py_stand_ins() {
    use pyo3::ffi::c_str;
    use pyo3::prelude::*;
    Python::attach(|py| {
        let modules = py.import("sys").unwrap().getattr("modules").unwrap();
        let db = PyModule::from_code(py, c_str!(
            "class Db:\n    def __init__(self, url):\n        self.url = url\n    def cursor(self):\n        return self\n    def rollback(self):\n        pass\n    def close(self):\n        pass\n\ndef connect(url):\n    return Db(url)\n"),
            c_str!("psycopg2.py"), c_str!("psycopg2")).unwrap();
        let etcd = PyModule::from_code(py, c_str!(
            "class Client:\n    def __init__(self, host, port):\n        self.host = host\n        self.port = port\n    def close(self):\n        pass\n\ndef client(host, port):\n    return Client(host, port)\n"),
            c_str!("etcd3.py"), c_str!("etcd3")).unwrap();
        modules.set_item("psycopg2", db).unwrap();
        modules.set_item("etcd3", etcd).unwrap();
    });
}
