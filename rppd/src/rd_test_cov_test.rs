#![cfg(feature = "etcd-embeded")]
//! Coverage of a function's own test: the master's tick, a deploy's and a cron's test, against a
//! throwaway PostgreSQL and an embedded-etcd cluster node.

use super::*;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;

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
        let dir = std::env::temp_dir().join(format!("rppd-test-{}-{}", name, std::process::id()));
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

fn log() -> slog::Logger {
    slog::Logger::root(slog::Discard, slog::o!())
}

/// A cluster node on `schema`, its tables created by the same pass as ytserv's, started
async fn node(pg: &Pg, schema: &str) -> RppdNodeCluster {
    let pool = sqlx::PgPool::connect(&pg.url).await.expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("create schema {}", schema))).execute(&pool).await.unwrap();
    pool.close().await;
    crate::ensure_schema(&pg.url, schema).await.expect("tables");
    let _env = crate::cov_lock::env_read();   // [test] not while another test sets a variable
    let c = crate::arg_config::RppdConfig {
        name: "cov-node".into(),
        user: "postgres".into(),
        schema: schema.into(),
        db_url: pg.url.clone(),
        bind: "127.0.0.1".into(),
        port: free_port(),
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

fn func(id: i32, code: &str, level: Option<i32>, stamped: bool) -> RpFn {
    RpFn { id, code: code.to_string(), checksum: String::new(), schema_table: "public.t".into(),
        topic: String::new(), queue: true, cleanup_logs_min: 0, priority: 1,
        fn_logging: false, verbose_debug: false, secret_names: None,
        breaker_fails: 0, breaker_slow_ms: 0, breaker_open_sec: 30, breaker_open_max_sec: 900,
        max_attempts: 0, notify_q: None, dlq: None,
        max_parallel: 0, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0,
        test_level: level, test_timestamp: stamped.then(chrono::Utc::now) }
}

const PASSING: &str = "def test(level):\n    return 0\n";
const FAILING: &str = "def test(level):\n    raise ValueError('no')\n";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_columns_and_runs_on_a_real_install() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    crate::cov_lock::install_py_stand_ins();
    let Some(pg) = Pg::start("rdtest") else { return };
    let cl = node(&pg, "rppd_t1").await;
    let db = cl.db();

    // the columns of a table, and none for a table that is not there
    let cols = table_columns(&db, "rppd_t1", "rppd_function").await.expect("columns");
    assert!(cols.contains("code") && cols.contains("test_level"), "{:?}", cols);
    assert!(table_columns(&db, "rppd_t1", "no_such_table").await.unwrap().is_empty());

    // a test that is no longer due: not run, and a test which is still due but whose row is gone
    assert!(cl.test_fn(func(1, PASSING, Some(1), true), 1, TestBy::Deploy, Duration::from_secs(5)).await.is_some());
    cl.testing.lock().unwrap().insert(7);
    assert!(cl.test_fn(func(7, PASSING, Some(1), false), 1, TestBy::Deploy, Duration::from_secs(5)).await.is_none(), "busy");
    cl.testing.lock().unwrap().remove(&7);
    assert!(!cl.test_still_due(&func(8, PASSING, Some(1), false)).await, "no row: not run");

    // a passing and a failing test, each a log row with its outcome
    let passed = cl.test_fn(func(2, PASSING, Some(1), false), 1, TestBy::Deploy, Duration::from_secs(30)).await;
    let failed = cl.test_fn(func(3, FAILING, Some(2), false), 2, TestBy::Cron(5), Duration::from_secs(30)).await;
    assert!(passed.is_some() && failed.is_some());

    // a cron's test marks its row started and finished, with the error of a failed test
    let cron: i32 = sqlx::query_scalar("insert into rppd_t1.rppd_cron (fn_id, cron, column_name, column_value, test_level) \
        values (3, '* * * * *', 'id', '1', 1) returning id")
        .fetch_one(&db).await.expect("cron row");
    cl.test_cron(func(3, FAILING, Some(1), false), 1, cron, Some(30)).await;
    cl.test_cron(func(2, PASSING, Some(1), false), 1, cron, None).await;
    cl.test_cron(func(2, PASSING, Some(1), false), 1, cron + 1000, None).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_master_tick_runs_only_the_due_tests() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    crate::cov_lock::install_py_stand_ins();
    let Some(pg) = Pg::start("rdtick") else { return };
    let cl = node(&pg, "rppd_t2").await;
    // not the master: nothing
    cl.master.store(false, std::sync::atomic::Ordering::Relaxed);
    cl.test_tick().await;
    // the master, a function due and one busy, one not due: the due one is stamped
    cl.master.store(true, std::sync::atomic::Ordering::Relaxed);
    {
        let mut fns = cl.fns.write().await;
        fns.insert(10, func(10, PASSING, Some(1), false));
        fns.insert(11, func(11, PASSING, Some(1), false));
        fns.insert(12, func(12, PASSING, None, false));
    }
    cl.testing.lock().unwrap().insert(11);
    cl.test_tick().await;
    assert!(cl.fns.read().await.get(&10).unwrap().test_timestamp.is_some(), "due: stamped at once");
    assert!(cl.fns.read().await.get(&12).unwrap().test_timestamp.is_none(), "no level: not due");
    // nothing due any more: the tick returns without taking the map
    cl.test_tick().await;
    // a test whose slow limit passes while it runs: said, and waited for
    let slow = cl.test_fn(func(13, "def test(level):\n    import time\n    time.sleep(1.5)\n    return 0\n", Some(1), true),
        1, TestBy::Deploy, Duration::from_millis(10)).await;
    assert!(slow.is_some());
}
