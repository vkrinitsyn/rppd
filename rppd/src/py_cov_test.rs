#![cfg(all(feature = "etcd-embeded", not(feature = "lib-embedded")))]
//! [coverage] a Python context made by a running node: the connections it is given, a call
//! with its arguments and its output, a failing call, a check (test) and the close. The
//! PostgreSQL and etcd client modules are stand-ins (in sys.modules): the real drivers are not
//! installed where the tests run, and a call here never reaches a server.

use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::ffi::c_str;

use crate::rd_fn::{RpFn, RpFnId, RpFnLog};

include!("cov_cluster.rs");

/// The stand-ins, once per process: psycopg2.connect and etcd3.client
fn install_stand_ins() {
    Python::attach(|py| {
        let sys = py.import("sys").unwrap();
        let modules = sys.getattr("modules").unwrap();
        let db = PyModule::from_code(py, c_str!(
            "class Db:\n    def __init__(self, url):\n        self.url = url\n    def cursor(self):\n        return self\n    def rollback(self):\n        pass\n    def close(self):\n        pass\n\ndef connect(url):\n    return Db(url)\n"),
            c_str!("psycopg2.py"), c_str!("psycopg2")).unwrap();
        let etcd = PyModule::from_code(py, c_str!(
            "class Client:\n    def __init__(self, host, port):\n        self.host = host\n        self.port = port\n    def close(self):\n        pass\n\ndef client(host, port):\n    return Client(host, port)\n"),
            c_str!("etcd3.py"), c_str!("etcd3")).unwrap();
        let ballista = PyModule::from_code(py, c_str!(
            "class BallistaSessionContext:\n    def __init__(self, url):\n        self.url = url\n"),
            c_str!("ballista.py"), c_str!("ballista")).unwrap();
        let clickhouse = PyModule::from_code(py, c_str!(
            "def get_client(dsn):\n    return _Client()\n\nclass _Client:\n    def close(self):\n        pass\n"),
            c_str!("clickhouse_connect.py"), c_str!("clickhouse_connect")).unwrap();
        modules.set_item("psycopg2", db).unwrap();
        modules.set_item("etcd3", etcd).unwrap();
        modules.set_item("ballista", ballista).unwrap();
        modules.set_item("clickhouse_connect", clickhouse).unwrap();
    });
}

fn func(id: i32, code: &str, secrets: Option<&str>, logging: bool) -> RpFn {
    RpFn {
        id, code: code.into(), checksum: String::new(), schema_table: "public.covt".into(),
        topic: String::new(), queue: true, cleanup_logs_min: 0, priority: 1,
        fn_logging: logging, verbose_debug: false, secret_names: secrets.map(str::to_string),
        breaker_fails: 0, breaker_slow_ms: 0, breaker_open_sec: 30, breaker_open_max_sec: 900,
        max_attempts: 0, notify_q: None, dlq: None,
        max_parallel: 0, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0,
        test_level: None, test_timestamp: None,
    }
}

/// The node is given a DataFusion scheduler and a ClickHouse endpoint: their clients are made
async fn set_connections(c: &crate::rd_config::RppdNodeCluster) {
    let mut cfg = c.cfg.write().await;
    cfg.df_url = Some("df://scheduler:50050".into());
    cfg.ch_url = Some("ch:8123".into());
}

/// The modules of the two optional clients are not there, and etcd is down: the context is
/// made without them, and a call runs without them
#[tokio::test]
async fn a_context_without_its_optional_clients_and_etcd_still_runs() {
    let Some(n) = cov_node("pynone").await else { return };
    let c = &n.cluster;
    install_stand_ins();
    Python::attach(|py| {
        let modules = py.import("sys").unwrap().getattr("modules").unwrap();
        modules.del_item("ballista").unwrap();
        modules.del_item("clickhouse_connect").unwrap();
        let down = PyModule::from_code(py, c_str!("def client(host, port):\n    raise ConnectionError('etcd is down')\n"),
            c_str!("etcd3.py"), c_str!("etcd3")).unwrap();
        modules.set_item("etcd3", down).unwrap();
    });
    set_connections(c).await;
    let fc = func(34, "print('still runs')\n", None, false);
    let mut ctx = c.new_py_context(&fc).await.expect("a context without the optional clients");
    let out = ctx.invoke(&RpFnLog { id: 3, fn_id: 34, ..RpFnLog::default() }, &fc).expect("the call runs");
    assert_eq!(out, "");
    ctx.close();
}

#[tokio::test]
async fn a_function_runs_with_its_arguments_and_its_output_is_kept() {
    let Some(n) = cov_node("py").await else { return };
    let c = &n.cluster;
    install_stand_ins();
    set_connections(c).await;
    let fc = func(30, "print('hello', TRIG, TOPIC, TABLE, ID, VALUE, TRACE)\nprint(SECRET.names())\n", Some("tok"), true);
    let mut ctx = c.new_py_context(&fc).await.expect("a context on the stand-ins");
    assert!(ctx.alive());
    let mut trig = HashMap::new();
    trig.insert("id".to_string(), "1".to_string());
    let x = RpFnLog {
        id: 1, fn_id: 30, fn_idp: Some(RpFnId::fromf(&fc)), trig_type: 2,
        trig_value: Some(sqlx::types::Json(trig)), value: Some(b"v".to_vec()),
        traceparent: Some("tp".into()), ..RpFnLog::default()
    };
    let out = ctx.invoke(&x, &fc).expect("the call runs");
    assert!(out.contains("hello 2"), "{}", out);
    assert!(out.contains("tok"), "the function sees the secret names it lists: {}", out);
    // a call that raises fails, with its error
    let bad = func(31, "raise ValueError('boom')\n", None, false);
    let err = ctx.invoke(&RpFnLog { id: 2, fn_id: 31, ..RpFnLog::default() }, &bad).unwrap_err();
    assert!(err.contains("boom"), "{}", err);
    ctx.close();
}

#[tokio::test]
async fn a_check_runs_its_test_and_the_context_closes_its_connections() {
    let Some(n) = cov_node("pycheck").await else { return };
    let c = &n.cluster;
    install_stand_ins();
    set_connections(c).await;
    let good = func(32, "def test(level):\n    return 0\n", Some("tok"), false);
    let mut ctx = c.new_py_context(&good).await.expect("a context on the stand-ins");
    let (r, _) = ctx.test(&good, 0);
    assert_eq!(r, Ok(()));
    let failing = func(33, "def test(level):\n    return 4\n", None, false);
    let (r, _) = ctx.test(&failing, 0);
    assert!(r.is_err());
    ctx.close();
}
