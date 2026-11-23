//! Coverage of the embedder's API: the schema entry points, serve, the master and consume switches,
//! stop and the peer call's database tag, against a throwaway PostgreSQL and an embedded-etcd node.
#![cfg(feature = "etcd-embeded")]

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
        let dir = std::env::temp_dir().join(format!("rppd-lib-{}-{}", name, std::process::id()));
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

/// A cluster node on `schema`, its tables created, started
async fn node(pg: &Pg, schema: &str, port: u16) -> RppdNodeCluster {
    let pool = sqlx::PgPool::connect(&pg.url).await.expect("connect");
    sqlx::query(sqlx::AssertSqlSafe(format!("create schema if not exists {}", schema))).execute(&pool).await.unwrap();
    pool.close().await;
    ensure_schema(&pg.url, schema).await.expect("tables");
    let _env = crate::cov_lock::env_read();   // [test] not while another test sets a variable
    let c = crate::arg_config::RppdConfig {
        name: "cov-lib".into(),
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

#[test]
fn the_schema_definition_takes_the_schema_name() {
    let s = get_schema_def("my_schema");
    assert!(s.contains("my_schema") && !s.contains("@extschema@"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unmaster_hands_the_master_to_a_serving_peer() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("peer") else { return };
    let a = node(&pg, "rppd_lib2", free_port()).await;
    let pb = free_port();
    let b = node(&pg, "rppd_lib2", pb).await;
    b.cfg.write().await.bind = format!("localhost:{}", pb);
    b.serve().await.expect("serve the peer");
    // a second server on the same port cannot bind: its failure is logged, the call returns
    let again = b.clone();
    again.serve().await.expect("the call itself is fine");
    tokio::time::sleep(Duration::from_millis(200)).await;
    let peer = crate::rd_config::RpHost {
        id: 999,
        host: format!("localhost:{}", pb),
        host_name: "peer".into(),
        active_since: None,
        master: None,
        max_db_connections: 1,
    }.connect().await;
    a.node_connections.write().await.insert(999, peer);
    assert!(a.is_master() || a.set_master(true).await);
    a.unmaster().await;
    a.stop().await;
    b.stop().await;
}

// standalone mode only: a node built into its host (lib-embedded) neither registers in the
// roster nor takes its master role from the database
#[cfg(not(feature = "lib-embedded"))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_serves_switches_its_roles_and_stops() {
    let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
    let Some(pg) = Pg::start("serve") else { return };
    let cl = node(&pg, "rppd_lib1", free_port()).await;

    // the role switches: a change is reported once, and the master flag follows
    // a lone node registered as master at start
    assert!(cl.is_master());
    assert!(!cl.set_master(true).await, "already master");
    assert!(cl.set_master(false).await, "gave it up");
    assert!(!cl.set_master(false).await, "already not");
    assert!(!cl.is_master());
    assert!(cl.set_master(true).await, "became master");

    // consume off and on: the watchers go and come back (no functions here, so none to open)
    cl.set_consume(false).await;
    cl.set_consume(false).await;
    cl.set_consume(true).await;
    cl.set_consume(true).await;

    // unmaster with no peer to switch to: nothing to do, and a tagged peer call carries its tag
    cl.set_master(true).await;
    cl.unmaster().await;
    cl.cfg.write().await.db_tag = "tag1".into();
    let req = cl.peer(rppd_common::protogen::rppd::SwitchRequest { node_id: 1 }).await;
    assert_eq!(req.metadata().get(crate::router::DB_HEADER).and_then(|v| v.to_str().ok()), Some("tag1"));
    cl.cfg.write().await.db_tag = "not a header\n".into();
    let untagged = cl.peer(rppd_common::protogen::rppd::SwitchRequest { node_id: 1 }).await;
    assert!(untagged.metadata().get(crate::router::DB_HEADER).is_none());
    cl.cfg.write().await.db_tag = String::new();

    // serve on a numeric address with a port, and on a host name that resolves
    let port = free_port();
    cl.cfg.write().await.bind = format!("127.0.0.1:{}", port);
    cl.cfg.write().await.port = port;
    cl.serve().await.expect("serve on a numeric address");
    tokio::time::sleep(Duration::from_millis(50)).await;

    let port2 = free_port();
    cl.cfg.write().await.bind = format!("http://localhost:{}", port2);
    cl.cfg.write().await.port = port2;
    cl.serve().await.expect("serve on a host name");

    // a bare address without its port is refused
    cl.cfg.write().await.bind = "127.0.0.1".into();
    assert!(cl.serve().await.is_err());


    // stop: nothing runs, the master flag goes
    cl.set_master(true).await;
    cl.stop().await;
    assert!(!cl.is_master());
}
