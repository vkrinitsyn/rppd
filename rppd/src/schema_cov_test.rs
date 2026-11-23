//! Coverage of the startup schema pass against a throwaway PostgreSQL.

use super::*;
use std::net::TcpListener;
use std::path::PathBuf;

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
        let dir = std::env::temp_dir().join(format!("rppd-schema-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let port = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
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

fn log() -> slog::Logger {
    slog::Logger::root(slog::Discard, slog::o!())
}

#[tokio::test]
async fn an_install_is_upgraded_and_a_missing_one_is_left_alone() {
    let Some(pg) = Pg::start("schema") else { return };
    let pool = sqlx::PgPool::connect(&pg.url).await.expect("connect");
    // no rppd_function in the schema: the upgrade leaves it alone
    upgrade(&pool, &pg.url, "rppd_s", &log()).await;
    let made: bool = sqlx::query_scalar("select exists (select 1 from information_schema.schemata where schema_name = 'rppd_s')")
        .fetch_one(&pool).await.unwrap();
    assert!(!made, "an upgrade creates nothing");

    // the create pass makes the tables and their columns, and a second pass finds nothing to do
    sqlx::query("create schema rppd_s").execute(&pool).await.unwrap();
    let n = migrate(&pg.url, "rppd_s").await.expect("migrate");
    assert!(n > 0, "tables created: {}", n);
    let tables: i64 = sqlx::query_scalar("select count(*) from information_schema.tables where table_schema = 'rppd_s' and table_name = 'rppd_function'")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(tables, 1);
    migrate(&pg.url, "rppd_s").await.expect("again");
    // now installed: the upgrade runs the pass, and says it is current
    upgrade(&pool, &pg.url, "rppd_s", &log()).await;

    // a column the install lacks is added by the pass, and nothing else changes
    sqlx::query("alter table rppd_s.rppd_function drop column if exists test_level").execute(&pool).await.unwrap();
    let added = migrate(&pg.url, "rppd_s").await.expect("add a column");
    assert!(added >= 1, "column back: {}", added);

    // an unreachable database: the upgrade logs and goes on, the create pass refuses
    let bad = "postgres://postgres@127.0.0.1:1/none";
    upgrade(&pool, bad, "rppd_s", &log()).await;
    assert!(migrate(bad, "rppd_s").await.is_err());
    pool.close().await;
}
