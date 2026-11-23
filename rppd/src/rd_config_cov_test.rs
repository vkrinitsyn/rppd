//! Coverage of the node's start, its host and function reloads, prepare and execute, against a
//! throwaway PostgreSQL and an embedded-etcd node.
#![cfg(feature = "etcd-embeded")]

use super::*;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use rppd_common::protogen::rppc::{DbAction, DbEventRequest, PkColumn};
use rppd_common::protogen::rppc::pk_column::PkValue;

/// A throwaway PostgreSQL of the newest installed version (PG_BIN overrides), on 127.0.0.1 and a
/// free port, no unix socket. Stopped and removed when dropped. None without one: the test
/// says so and passes.
struct Pg {
    bin: PathBuf,
    dir: PathBuf,
    url: String,
}

impl Pg {
    fn start(name: &str) -> Option<Pg> {
        let bin = match std::env::var("PG_BIN") {
            Ok(b) => PathBuf::from(b),
            Err(_) => {
                let mut v: Vec<(u32, PathBuf)> = std::fs::read_dir("/usr/lib/postgresql").ok()?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| Some((e.file_name().to_str()?.parse().ok()?, e.path().join("bin"))))
                    .filter(|(_, b): &(u32, PathBuf)| b.join("initdb").exists())
                    .collect();
                v.sort();
                match v.pop() {
                    Some((_, b)) => b,
                    None => {
                        eprintln!("no PostgreSQL installed (PG_BIN, /usr/lib/postgresql): skipped");
                        return None;
                    }
                }
            }
        };
        let dir = std::env::temp_dir().join(format!("rppd-cfg-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let port = free_port();
        let run = |c: &mut std::process::Command| c.output().map(|o| o.status.success()).unwrap_or(false);
        assert!(run(std::process::Command::new(bin.join("initdb")).args(["-A", "trust", "-U", "postgres", "-E", "UTF8", "--no-locale", "-N", "-D"])
            .arg(dir.join("data"))), "initdb");
        let pg = Pg { bin, dir: dir.clone(), url: format!("postgres://postgres@127.0.0.1:{}/postgres", port) };
        let opts = format!("-c listen_addresses=127.0.0.1 -c port={} -c fsync=off -c unix_socket_directories=''", port);
        assert!(run(std::process::Command::new(pg.bin.join("pg_ctl")).args(["-w", "-t", "60", "-l"]).arg(dir.join("log"))
            .arg("-D").arg(dir.join("data")).arg("-o").arg(opts).arg("start")),
            "pg_ctl start: {}", std::fs::read_to_string(dir.join("log")).unwrap_or_default());
        Some(pg)
    }
}

impl Drop for Pg {
    fn drop(&mut self) {
        let _ = std::process::Command::new(self.bin.join("pg_ctl")).args(["-w", "-m", "immediate", "-D"])
            .arg(self.dir.join("data")).arg("stop").output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn log() -> Logger {
    Logger::root(slog::Discard, slog::o!())
}

/// the schema's tables made, and a node on `schema` started; its port is `port`
async fn node(pg: &Pg, schema: &str, port: u16) -> RppdNodeCluster {
    let pool = sqlx::PgPool::connect(&pg.url).await.expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("create schema {}", schema))).execute(&pool).await.unwrap();
    pool.close().await;
    crate::ensure_schema(&pg.url, schema).await.expect("tables");
    let _env = crate::cov_lock::env_read();   // [test] not while another test sets a variable
    let c = RppdConfig {
        name: "cov-cfg".into(),
        user: "postgres".into(),
        schema: schema.into(),
        db_url: pg.url.clone(),
        bind: "127.0.0.1".into(),
        port,
        ..Default::default()
    };
    drop(_env);
    let cl = RppdNodeCluster::init(c, log(), #[cfg(feature = "tracer")] None).await.expect("init");
    for _ in 0..400 {
        if cl.is_started() { break; }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    cl
}

/// a function row of the test schema, on table `public.t`
async fn add_fn(db: &Pool<Postgres>, schema: &str, id: i32, code: &str, topic: &str) {
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "insert into {}.rppd_function (id, code, checksum, schema_table, topic, queue, cleanup_logs_min, priority, fn_logging, verbose_debug) \
         values ($1, $2, 'c', 'public.t', $3, true, 0, 1, false, false)", schema)))
        .bind(id).bind(code).bind(topic)
        .execute(db).await.expect("function row");
}

#[tokio::test]
async fn a_node_that_cannot_reach_its_database_does_not_start() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("noconn") else { return };
    let closed = format!("postgres://postgres@127.0.0.1:{}/postgres", free_port());
    let _env = crate::cov_lock::env_read();   // [test] not while another test sets a variable
    let c = RppdConfig { db_url: closed, schema: "rppd_cfg0".into(), ..Default::default() };
    drop(_env);
    let r = tokio::time::timeout(Duration::from_secs(15), RppdNodeCluster::init(c, log(), #[cfg(feature = "tracer")] None)).await;
    assert!(!matches!(r, Ok(Ok(_))), "no database, no node");
    drop(pg);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_changed_function_merges_and_its_log_row_is_written() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("merge") else { return };
    let cl = node(&pg, "rppd_cfg2", free_port()).await;
    let db = cl.db();
    let schema = "rppd_cfg2";
    add_fn(&db, schema, 4, "def f():\n    return 1\n", "").await;
    cl.reload_fs(&None).await.expect("functions");
    // topic and table change: the merge re-indexes both
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "update {}.rppd_function set topic = '.id', schema_table = 'public.u' where id = 4", schema)))
        .execute(&db).await.unwrap();
    let one = DbEventRequest { pks: vec![PkColumn { column_name: "id".into(), column_type: 0, pk_value: Some(PkValue::IntValue(4)) }], ..Default::default() };
    cl.reload_fs(&Some(one)).await.expect("merge");
    assert!(!cl.check_fn(&"public.u".to_string()).await.is_empty(), "re-indexed under its new table");

    // a log row: written by the call, then its outcome set, then its error
    let id: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "insert into {}.rppd_function_log (node_id, fn_id, trig_type) values (1, 4, 1) returning id", schema)))
        .fetch_one(&db).await.expect("log row");
    let row = RpFnLog { id, ..RpFnLog::default() };
    row.update(7, Ok("out".into()), db.clone(), schema, &log()).await;
    row.update(8, Err("bad".into()), db.clone(), schema, &log()).await;
    row.update_err("again".into(), db.clone(), schema, &log()).await;
    // a call with no log row is not written
    RpFnLog::default().update(1, Ok(String::new()), db.clone(), schema, &log()).await;
    RpFnLog::default().update_err(String::new(), db.clone(), schema, &log()).await;
    // a second change: a new topic and table, the queue and the cleanup settings change
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "update {}.rppd_function set topic = 'fresh', schema_table = 'public.v', queue = false, cleanup_logs_min = 5 where id = 4", schema)))
        .execute(&db).await.unwrap();
    cl.reload_fs(&Some(DbEventRequest { pks: vec![PkColumn { column_name: "id".into(), column_type: 0, pk_value: Some(PkValue::IntValue(4)) }], ..Default::default() })).await.expect("merge again");
    assert!(!cl.check_fn(&"public.v".to_string()).await.is_empty());
    // a log row whose table is not there: the failure is logged, the call goes on
    row.update(9, Err("gone".into()), db.clone(), "no_such_schema", &log()).await;
    row.update_err("gone".into(), db.clone(), "no_such_schema", &log()).await;
    let took: Option<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(format!("select took_ms from {}.rppd_function_log where id = {}", schema, id)))
        .fetch_one(&db).await.unwrap();
    assert_eq!(took, Some(8));
    cl.stop().await;
}

// standalone mode only: a node built into its host (lib-embedded) neither registers in the
// roster nor takes its master role from the database
#[cfg(not(feature = "lib-embedded"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_loads_its_hosts_and_functions_and_runs_one() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("load") else { return };
    let cl = node(&pg, "rppd_cfg1", free_port()).await;
    let db = cl.db();
    let schema = "rppd_cfg1";

    // the node registered itself in the roster, as master, and knows its own id
    assert!(cl.is_master(), "a lone node is the master");
    assert!(cl.node_id.load(Ordering::Relaxed) > 0);
    // a peer row, not this node: its connection is tried and refused, and kept as an error
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "insert into {}.rppd_config (host, host_name, active_since) values ('127.0.0.1:1', 'peer', now())", schema)))
        .execute(&db).await.unwrap();
    cl.reload_hosts(&DbEventRequest::default()).await.expect("hosts");
    assert!(cl.node_connections.read().await.values().any(|c| c.is_err()), "the peer is refused");
    // a load of one host row by its key
    let pk = PkColumn { column_name: "id".into(), column_type: 0, pk_value: Some(PkValue::IntValue(cl.node_id.load(Ordering::Relaxed))) };
    cl.reload_hosts(&DbEventRequest { pks: vec![pk], ..Default::default() }).await.expect("one host");
    // a function: loaded, then changed by a load of its row, then gone
    add_fn(&db, schema, 1, "def f():\n    return 1\n", "").await;
    add_fn(&db, schema, 2, "def g():\n    return 2\n", ".id").await;
    cl.reload_fs(&None).await.expect("functions");
    assert!(!cl.check_fn(&"public.t".to_string()).await.is_empty(), "the table's function is found");
    sqlx::query(sqlx::AssertSqlSafe(format!("update {}.rppd_function set code = 'def f():\n    return 3\n' where id = 1", schema)))
        .execute(&db).await.unwrap();
    let one = DbEventRequest { pks: vec![PkColumn { column_name: "id".into(), column_type: 0, pk_value: Some(PkValue::IntValue(1)) }], ..Default::default() };
    cl.reload_fs(&Some(one)).await.expect("one function");
    cl.reload_stale().await;

    // a repeat (a dot topic with no value) and a plain event, prepared
    let fns = cl.check_fn(&"public.t".to_string()).await;
    let pks = vec![PkColumn { column_name: "id".into(), column_type: 0, pk_value: Some(PkValue::IntValue(5)) }];
    let prepared = cl.prepare(fns.clone(), &pks, DbAction::Insert, &Some(("k".into(), b"v".to_vec())), "").await;
    assert!(prepared.is_ok());
    let none = cl.prepare(fns.clone(), &Vec::new(), DbAction::Insert, &None, "00-trace").await;
    assert!(none.is_ok());

    // a queued call runs the function on this node, and its completion is taken
    let f = RpFnLog { fn_idp: fns.values().next().cloned(), fn_id: 1, trig_type: 1, ..RpFnLog::default() };
    cl.queueing(f.clone(), false).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    cl.execute(Some(RpFnLog { took_ms: Some(3), fn_idp: f.fn_idp.clone(), ..f.clone() })).await;
    cl.execute(None).await;
    cl.queueing(f, true).await;
    cl.execute(None).await;

    cl.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_function_with_a_breaker_and_a_shaper_holds_its_events() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    crate::cov_lock::install_py_stand_ins();
    let Some(pg) = Pg::start("breaker") else { return };
    let cl = node(&pg, "rppd_cfg3", free_port()).await;
    let db = cl.db();
    let schema = "rppd_cfg3";
    add_fn(&db, schema, 5, "def f():\n    raise ValueError('x')\n", "").await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "update {}.rppd_function set breaker_fails = 1, breaker_open_sec = 1, breaker_open_max_sec = 2, \
         max_parallel = 1, rate_per_sec = 1, burst = 1 where id = 5", schema)))
        .execute(&db).await.expect("breaker and shaper settings");
    cl.reload_fs(&None).await.expect("functions");
    let fns = cl.check_fn(&"public.t".to_string()).await;
    let f = RpFnLog { fn_idp: fns.values().next().cloned(), fn_id: 5, trig_type: 1, ..RpFnLog::default() };
    // events queued and taken while the breaker and the shaper hold them
    for _ in 0..4 {
        cl.queueing(f.clone(), false).await;
    }
    for _ in 0..6 {
        cl.execute(None).await;
        tokio::time::sleep(Duration::from_millis(60)).await;
    }
    cl.queueing(f.clone(), true).await;
    cl.execute(Some(f)).await;
    cl.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn queued_events_run_on_the_python_stand_ins_and_their_logs_are_written() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    crate::cov_lock::install_py_stand_ins();
    let Some(pg) = Pg::start("exec") else { return };
    let cl = node(&pg, "rppd_cfg4", free_port()).await;
    let db = cl.db();
    let schema = "rppd_cfg4";
    // one function that runs, one that raises and opens its breaker after one failure
    add_fn(&db, schema, 9, "def f():\n    print('ran')\n", "").await;
    add_fn(&db, schema, 10, "raise ValueError('boom')\n", "").await;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "update {}.rppd_function set fn_logging = true where id in (9, 10)", schema)))
        .execute(&db).await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "update {}.rppd_function set breaker_fails = 1, breaker_open_sec = 1, breaker_open_max_sec = 2 where id = 10", schema)))
        .execute(&db).await.unwrap();
    cl.reload_fs(&None).await.expect("functions");
    let fns = cl.check_fn(&"public.t".to_string()).await;
    let pks = vec![PkColumn { column_name: "id".into(), column_type: 0, pk_value: Some(PkValue::IntValue(1)) }];
    // three rounds: each prepares a log row per function and queues it
    for _ in 0..3 {
        if let Ok(crate::rd_rpc::ScheduleResult::Some(logs)) = cl.prepare(fns.clone(), &pks, DbAction::Insert, &None, "").await {
            for l in logs {
                cl.queueing(l, false).await;
            }
        }
    }
    // a call put back to its line, and one taken by the loop with no event
    cl.queueing(RpFnLog { fn_id: 9, fn_idp: fns.get(&9).cloned(), trig_type: 1, ..RpFnLog::default() }, true).await;
    cl.execute(None).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let done: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "select count(*) from {}.rppd_function_log where took_ms is not null", schema)))
        .fetch_one(&db).await.unwrap();
    assert!(done >= 1, "some events ran to their end: {}", done);
    cl.stop().await;
}
