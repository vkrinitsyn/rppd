//! Coverage tests for rd_etcd.rs: a queue watched on the embedded etcd of a node with its
//! own throwaway PostgreSQL, its messages consumed, acknowledged and renamed.

#![cfg(feature = "etcd-embeded")]
use super::*;
use std::net::TcpListener;
use std::path::PathBuf;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// the newest PostgreSQL installed (PG_BIN overrides), on a TCP port; stopped and removed on drop
struct Pg {
    bin: PathBuf,
    dir: PathBuf,
    port: u16,
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
        let port = free_port();
        let dir = std::env::temp_dir().join(format!("rppd-etcd-{}-{}-{}", name, std::process::id(), port));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let ok = |c: &mut std::process::Command| c.output().map(|o| o.status.success()).unwrap_or(false);
        assert!(ok(std::process::Command::new(bin.join("initdb")).args(["-N", "-A", "trust", "-U", "postgres", "-E", "UTF8", "--no-locale", "-D"])
            .arg(dir.join("data"))), "initdb");
        let pg = Pg { bin, dir, port };
        assert!(ok(std::process::Command::new(pg.bin.join("pg_ctl")).args(["-w", "-t", "60", "-l"]).arg(pg.dir.join("log")).arg("-D").arg(pg.dir.join("data"))
            .arg("-o").arg(format!("-c listen_addresses=127.0.0.1 -c fsync=off -c port={} -k {}", pg.port, pg.dir.display())).arg("start")),
            "pg_ctl start");
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

/// a node of its own on a free port, with its own throwaway PostgreSQL
async fn node(pg: &Pg) -> RppdNodeCluster {
    let c = RppdConfig {
        name: "cov-etcd".to_string(),
        bind: "127.0.0.1".to_string(),
        port: free_port(),
        user: "postgres".to_string(),
        pwd: String::new(),
        db_url: format!("postgres://postgres@127.0.0.1:{}/postgres", pg.port),
        ..RppdConfig::default()
    };
    crate::ensure_schema(&c.db_url, "public").await.unwrap();
    let log = slog::Logger::root(slog::Discard, slog::o!());
    #[cfg(feature = "tracer")]
    let r = RppdNodeCluster::init(c, log, None).await;
    #[cfg(not(feature = "tracer"))]
    let r = RppdNodeCluster::init(c, log).await;
    r.unwrap()
}

fn queue_fn(id: i32, table: &str) -> RpFn {
    RpFn { id, code: String::new(), checksum: String::new(), schema_table: table.to_string(),
        topic: String::new(), queue: true, cleanup_logs_min: 0, priority: 1,
        fn_logging: false, verbose_debug: false, secret_names: None,
        breaker_fails: 0, breaker_slow_ms: 0, breaker_open_sec: 30, breaker_open_max_sec: 900,
        max_attempts: 0, notify_q: None, dlq: None,
        max_parallel: 0, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0,
        test_level: None, test_timestamp: None }
}

/// [etcd] A queue on a node that does not consume (rppd-spare) is not watched
#[tokio::test]
async fn a_node_that_does_not_consume_watches_nothing() {
    // the embedded etcd's peer port is fixed: one node at a time in this binary
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("spare") else { return };
    let cl = node(&pg).await;
    cl.set_consume(false).await;
    let mut f = queue_fn(3, "/q/cov/spare");
    f.watch_merge(None, &cl).await;
    assert!(cl.watchers.read().await.is_empty());
    cl.stop().await;
}
