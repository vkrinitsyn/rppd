//! Coverage tests for rd_monitor.rs: the monitor's pass over a node with its own
//! throwaway PostgreSQL, the stats it keeps, and taking over as master.

// standalone mode only: a node built into its host (lib-embedded) has no monitor of its roster
#![cfg(all(feature = "etcd-embeded", not(feature = "lib-embedded")))]
use super::*;
use std::net::TcpListener;
use std::path::PathBuf;
use std::time::Duration;
use rppd_common::protogen::rppc::StatusResponse;
use crate::arg_config::RppdConfig;

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
        let dir = std::env::temp_dir().join(format!("rppd-rpc-{}-{}-{}", name, std::process::id(), port));
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
        name: "cov-node".to_string(),
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

/// [monitor] The monitor's pass: it follows the master the database names (not this node),
/// runs its jobs, and keeps going until it is stopped
#[tokio::test]
async fn monitor_follows_the_database_master() {
    // the embedded etcd's peer port is fixed: one node at a time in this binary
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("monitor") else { return };
    let cl = node(&pg).await;
    for _ in 0..200 {
        if cl.is_started() { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    // another node the database names master
    sqlx::query("update public.rppd_config set master = null where master").execute(&cl.db()).await.unwrap();
    let other: i32 = sqlx::query_scalar("insert into public.rppd_config (host, host_name, master, active_since, max_db_connections) values ('10.0.0.9:1', 'other', true, now(), 1) returning id")
        .fetch_one(&cl.db()).await.unwrap();
    let m = cl.clone();
    let task = tokio::spawn(async move { m.start_monitoring().await });
    tokio::time::sleep(Duration::from_millis(1500)).await;
    task.abort();
    assert!(!cl.is_master(), "the database's word: this node is not master");
    assert_eq!(cl.master_id.load(Ordering::Relaxed), other);
    cl.cleanup_fn_logs().await;
    cl.stop().await;
}

/// [monitor] Taking over: this node marks itself master in the database, and unmarks the one
/// before it
#[cfg(not(feature = "lib-embedded"))]
#[tokio::test]
async fn becoming_master_marks_this_node() {
    // the embedded etcd's peer port is fixed: one node at a time in this binary
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("become") else { return };
    let cl = node(&pg).await;
    for _ in 0..200 {
        if cl.is_started() { break; }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    cl.become_master(Some(9_999)).await;
    assert!(cl.is_master());
    assert_eq!(cl.master_id.load(Ordering::Relaxed), cl.node_id.load(Ordering::Relaxed));
    cl.become_master(None).await;
    assert!(cl.is_master());
    cl.stop().await;
}

/// [monitor] The stats a node keeps of the others: weighted by the median of their queue and
/// in-process counts, and picked in weight order when they have a connection
#[cfg(not(feature = "lib-embedded"))]
#[tokio::test]
async fn stats_weight_the_nodes_and_pick_one_with_a_connection() {
    // the embedded etcd's peer port is fixed: one node at a time in this binary
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("stats") else { return };
    let cl = node(&pg).await;
    let mut st = ClusterStat::default();
    st.node_stat.insert(2, NodeStat::new_from(StatusResponse { node_id: 2, queued: 4, in_proc: 1, ..Default::default() }));
    st.apply(2);
    st.apply(2);
    st.node_stat.insert(3, NodeStat::default());
    st.apply(3);
    assert!(st.sort.values().any(|s| s.contains(&2)), "2 is sorted by its weight");
    st.this_stat.node_id = 1;
    assert!(st.sort.values().any(|s| s.contains(&3)));
    *cl.stat.write().await = st;
    assert_eq!(cl.pick_node().await, None, "no connection to any node");
    cl.node_connections.write().await.insert(2, Err("down".to_string()));
    assert_eq!(cl.pick_node().await, None, "an error is no connection");
    cl.stop().await;
}
