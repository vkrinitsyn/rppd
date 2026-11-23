//! Coverage tests for router.rs: a call goes to the instance its database header names.

#![cfg(feature = "etcd-embeded")]

use super::*;
use crate::arg_config::RppdConfig;
use std::net::TcpListener;
use std::path::PathBuf;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use rppd_common::protogen::rppc::StatusRequest;
use rppd_common::protogen::rppd::rppd_node_server::RppdNode;
use tonic::Request;

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



    /// [router] a call goes to the instance its database header names, else to the default; an
    /// unknown database is refused
    #[tokio::test]
    async fn router_picks_the_instance_by_header() {
        // the embedded etcd's peer port is fixed: one node at a time in this binary
        let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
        let Some(pg) = Pg::start("router") else { return };
        let default = node(&pg).await;
        let tagged_node = node(&pg).await;
        let mut map = BTreeMap::new();
        map.insert("alpha".to_string(), tagged_node.clone());
        let router = crate::router::RppdRouter::new(default.clone(), Arc::new(RwLock::new(map)));

        for _ in 0..200 {
            if default.is_started() && tagged_node.is_started() { break; }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let plain = RppdNode::status(&router, Request::new(StatusRequest { config_schema_table: String::new(),
            node_id: default.node_id.load(std::sync::atomic::Ordering::Relaxed), fn_log: None })).await;
        assert!(plain.is_ok());
        let mut tagged = Request::new(StatusRequest { config_schema_table: String::new(),
            node_id: tagged_node.node_id.load(std::sync::atomic::Ordering::Relaxed), fn_log: None });
        tagged.metadata_mut().insert(crate::router::DB_HEADER, "alpha".parse().unwrap());
        assert!(RppdNode::status(&router, tagged).await.is_ok());
        let mut missing = Request::new(StatusRequest::default());
        missing.metadata_mut().insert(crate::router::DB_HEADER, "beta".parse().unwrap());
        assert_eq!(RppdNode::status(&router, missing).await.unwrap_err().code(), tonic::Code::NotFound);
        let mut blank = Request::new(StatusRequest::default());
        blank.metadata_mut().insert(crate::router::DB_HEADER, "  ".parse().unwrap());
        let _ = RppdNode::status(&router, blank).await;
        default.stop().await;
        tagged_node.stop().await;
    }

