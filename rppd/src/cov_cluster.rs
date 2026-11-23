// [coverage] One throwaway PostgreSQL and one rppd node on it, for the tests that need a
// running cluster. Included (include!) by each cov test file, so its items are local to
// that module. The embedded etcd listens on its default peer port, so the tests that use
// it run one at a time, under the crate's one etcd lock (cov_lock::ETCD_LOCK).

/// A PostgreSQL cluster of the newest installed version - PG_BIN, else
/// /usr/lib/postgresql/<ver>/bin - on its unix socket alone; stopped and removed on drop
struct CovPg {
    bin: std::path::PathBuf,
    dir: std::path::PathBuf,
}

impl CovPg {
    fn start(name: &str) -> Option<CovPg> {
        let bin = match std::env::var("PG_BIN") {
            Ok(b) => std::path::PathBuf::from(b),
            Err(_) => {
                let mut v: Vec<(u32, std::path::PathBuf)> = std::fs::read_dir("/usr/lib/postgresql").ok()?
                    .filter_map(|e| e.ok())
                    .filter_map(|e| Some((e.file_name().to_str()?.parse().ok()?, e.path().join("bin"))))
                    .filter(|(_, b): &(u32, std::path::PathBuf)| b.join("initdb").exists())
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
        let dir = std::env::temp_dir().join(format!("rppd-cov-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok()?;
        let pg = CovPg { bin, dir };
        let ok = std::process::Command::new(pg.bin.join("initdb"))
            .args(["-N", "-A", "trust", "-U", "postgres", "-E", "UTF8", "--no-locale", "-D"])
            .arg(pg.dir.join("data"))
            .output().map(|o| o.status.success()).unwrap_or(false);
        if !ok {
            eprintln!("initdb failed: skipped");
            return None;
        }
        let ok = std::process::Command::new(pg.bin.join("pg_ctl"))
            .args(["-w", "-t", "60", "-l"]).arg(pg.dir.join("log")).arg("-D").arg(pg.dir.join("data"))
            .arg("-o").arg(format!("-c listen_addresses='' -c fsync=off -k {}", pg.dir.display()))
            .arg("start")
            .output().map(|o| o.status.success()).unwrap_or(false);
        if !ok {
            eprintln!("pg_ctl start failed: skipped");
            return None;
        }
        Some(pg)
    }

    fn url(&self) -> String {
        format!("postgresql://postgres@localhost/postgres?host={}", self.dir.display())
    }
}

impl Drop for CovPg {
    fn drop(&mut self) {
        let _ = std::process::Command::new(self.bin.join("pg_ctl"))
            .args(["-w", "-t", "60", "-m", "immediate", "-D"]).arg(self.dir.join("data")).arg("stop").output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A free loopback port: taken, then released for the server to bind
fn cov_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").and_then(|l| l.local_addr()).map(|a| a.port()).unwrap_or(0)
}

/// One rppd node on its own PostgreSQL. Field order: the cluster is dropped first.
struct CovNode {
    cluster: crate::rd_config::RppdNodeCluster,
    _pg: CovPg,
    _lock: tokio::sync::MutexGuard<'static, ()>,
}

/// A started node, or None when PostgreSQL is absent (the caller returns: the test skipped)
async fn cov_node(name: &str) -> Option<CovNode> {
    let lock = crate::cov_lock::ETCD_LOCK.lock().await;
    let pg = CovPg::start(name)?;
    let url = pg.url();
    crate::schema::migrate(&url, "public").await.expect("rppd tables from rppd.yaml");
    let _env = crate::cov_lock::env_read();   // [test] not while another test sets a variable
    let c = crate::arg_config::RppdConfig {
        db_url: url,
        user: "postgres".into(),
        pwd: String::new(),
        schema: "public".into(),
        bind: "127.0.0.1".into(),
        port: cov_free_port(),
        name: "cov".into(),
        node: uuid::Uuid::new_v4(),
        cluster: uuid::Uuid::new_v4(),
        ..crate::arg_config::RppdConfig::default()
    };
    drop(_env);
    let log = slog::Logger::root(slog::Discard, slog::o!());
    #[cfg(feature = "tracer")]
    let cluster = crate::rd_config::RppdNodeCluster::init(c, log, None).await.expect("cluster");
    #[cfg(not(feature = "tracer"))]
    let cluster = crate::rd_config::RppdNodeCluster::init(c, log).await.expect("cluster");
    Some(CovNode { cluster, _pg: pg, _lock: lock })
}


/// A function in the rppd_function table and in the node's map: a reload of the table keeps
/// it (one that is only in the map is dropped by the reload)
async fn cov_fn_add(c: &crate::rd_config::RppdNodeCluster, fc: &crate::rd_fn::RpFn) {
    sqlx::query("insert into public.rppd_function (id, code, checksum, schema_table, topic, queue, priority, \
        breaker_fails, breaker_slow_ms, breaker_open_sec, breaker_open_max_sec, max_attempts, notify_q, dlq, \
        max_parallel, min_parallel, target_ms, rate_per_sec, burst, max_backlog) \
        values ($1, '', '', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)")
        .bind(fc.id).bind(&fc.schema_table).bind(&fc.topic).bind(fc.queue).bind(fc.priority)
        .bind(fc.breaker_fails).bind(fc.breaker_slow_ms).bind(fc.breaker_open_sec).bind(fc.breaker_open_max_sec)
        .bind(fc.max_attempts).bind(&fc.notify_q).bind(&fc.dlq)
        .bind(fc.max_parallel).bind(fc.min_parallel).bind(fc.target_ms).bind(fc.rate_per_sec).bind(fc.burst)
        .bind(fc.max_backlog)
        .execute(&c.db()).await.expect("a function row");
    c.fns.write().await.insert(fc.id, fc.clone());
}
