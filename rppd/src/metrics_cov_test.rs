//! Coverage tests for metrics.rs: the instruments built on a meter and the recorders the
//! node calls, against the global (no-op) meter.

#![cfg(feature = "etcd-embeded")]
use super::*;
use crate::arg_config::RppdConfig;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;

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


/// [otel] every recorder with the instruments in place, and without them (nothing recorded)
#[tokio::test]
async fn recorders_record_on_the_instruments_and_do_nothing_without() {
    // the embedded etcd's peer port is fixed: one node at a time in this binary
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("metrics") else { return };
    let cl = node(&pg).await;
    cl.metric_call(1, "ok", Some(5));
    cl.metric_call(1, "error", None);
    cl.metric_shaper_wait(1, Duration::from_millis(3));
    cl.metric_transition(1, "open");

    let meter = opentelemetry::global::meter("rppd-cov");
    let inst = Instruments::new(&meter, &cl);
    *cl.metrics.write().unwrap() = Some(inst);
    cl.metric_call(1, "ok", Some(5));
    cl.metric_call(2, "dead", None);
    cl.metric_shaper_wait(1, Duration::from_millis(3));
    cl.metric_transition(1, "half");
    cl.metric_transition(2, "closed");
}
