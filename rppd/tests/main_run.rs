//! The rppd binary run as a child process: exit codes for bad arguments and an unreachable
//! database, and a start on a throwaway PostgreSQL that ends with SIGINT and a normal exit.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_rppd");

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn run(args: &[String]) -> (Option<i32>, String) {
    let out = Command::new(BIN).args(args).stdin(Stdio::null()).output().expect("run rppd");
    (out.status.code(), format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr)))
}

#[test]
fn bad_arguments_and_an_unreachable_database_exit_with_their_codes() {
    let many: Vec<String> = (0..7).map(|i| format!("a{}", i)).collect();
    assert_eq!(run(&many).0, Some(22));
    assert_eq!(run(&["127.0.0.1:notaport".to_string()]).0, Some(22));
    assert_eq!(run(&["a.b.c".to_string()]).0, Some(22));
    // a port that is no number: the connection is refused at once (a refused port is retried by the pool)
    let (code, _) = run(&["postgresql://postgres@127.0.0.1:notaport/postgres".to_string(), "--name=cov-none".to_string(), "--verbose".to_string()]);
    assert_eq!(code, Some(12));
}

/// A throwaway PostgreSQL (PG_BIN overrides the newest /usr/lib/postgresql/<ver>/bin); None without one.
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
        let dir = std::env::temp_dir().join(format!("rppd-run-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let port = free_port();
        let ok = |c: &mut Command| c.output().map(|o| o.status.success()).unwrap_or(false);
        assert!(ok(Command::new(bin.join("initdb")).args(["-A", "trust", "-U", "postgres", "-E", "UTF8", "--no-locale", "-N", "-D"])
            .arg(dir.join("data"))), "initdb");
        let pg = Pg { bin, dir: dir.clone(), url: format!("postgresql://postgres@127.0.0.1:{}/postgres", port) };
        let opts = format!("-c listen_addresses=127.0.0.1 -c port={} -c fsync=off -c unix_socket_directories=''", port);
        assert!(ok(Command::new(pg.bin.join("pg_ctl")).args(["-w", "-t", "60", "-l"]).arg(dir.join("log"))
            .arg("-D").arg(dir.join("data")).arg("-o").arg(opts).arg("start")), "pg_ctl start");
        Some(pg)
    }
}

impl Drop for Pg {
    fn drop(&mut self) {
        let _ = Command::new(self.bin.join("pg_ctl")).args(["-w", "-m", "immediate", "-D"])
            .arg(self.dir.join("data")).arg("stop").output();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[tokio::test]
async fn a_node_starts_on_a_database_and_exits_normally_on_interrupt() {
    let Some(pg) = Pg::start("start") else { return };
    let pool = sqlx::PgPool::connect(&pg.url).await.expect("connect");
    sqlx::query("create schema rppd_main").execute(&pool).await.unwrap();
    pool.close().await;
    rppde::ensure_schema(&pg.url, "rppd_main").await.expect("tables");


    let port = free_port();
    let mut child = Command::new(BIN)
        .args([pg.url.clone(), "rppd_main".to_string(), format!("localhost:{}", port), "--name=cov-main".to_string()])
        .env("USER", "postgres").env_remove("PGUSER")
        .stdin(Stdio::null()).stdout(std::fs::File::create(pg.dir.join("child.err")).unwrap()).stderr(Stdio::null())
        .spawn().expect("spawn rppd");
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline && TcpStream::connect(("localhost", port)).is_err() {
        if let Ok(Some(_)) = child.try_wait() { break; }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = Command::new("kill").args(["-INT", &child.id().to_string()]).status();
    let status = child.wait().expect("wait rppd");
    assert_eq!(status.code(), Some(0), "stopped cleanly on interrupt: {:?} {}", status, std::fs::read_to_string(pg.dir.join("child.err")).unwrap_or_default());
}
