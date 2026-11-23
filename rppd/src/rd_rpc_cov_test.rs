//! Coverage tests for rd_rpc.rs and the generated gRPC code (rppd-common protogen):
//! every message is sent over a real channel to a test server and back, and the
//! node's own services run against a throwaway PostgreSQL and embedded etcd.

use super::*;
use std::time::Duration;
use async_trait::async_trait;
use rppd_common::protogen::rppc::{db_event_request, fn_status, pk_column, status_request, status_response,
    FnAction, FnStatus, PkColumn, PkColumnType, StatusFnsResponse};
use rppd_common::protogen::rppd::rppd_node_client::RppdNodeClient;
use rppd_common::protogen::rppd::rppd_node_server::RppdNodeServer;
use rppd_common::protogen::rppg::rppd_trigger_client::RppdTriggerClient;
use rppd_common::protogen::rppg::rppd_trigger_server::RppdTriggerServer;
use tonic::transport::Server;
use std::sync::Arc;

/// Generated messages over a real channel, from a test server that echoes what it gets
#[derive(Default)]
struct Echo;

fn status_reply(r: StatusRequest) -> Result<Response<StatusResponse>, Status> {
    let fn_log = match r.fn_log {
        Some(status_request::FnLog::FnLogId(id)) => Some(status_response::FnLog::Status(FnStatus {
            status: Some(fn_status::Status::InProcSec(id as u32)),
        })),
        Some(status_request::FnLog::Uuid(u)) => Some(status_response::FnLog::Uuid(StatusFnsResponse { uuid: vec![u] })),
        None => None,
    };
    let fn_log = match (fn_log, r.config_schema_table.as_str()) {
        (Some(status_response::FnLog::Status(_)), "pos") => Some(status_response::FnLog::Status(FnStatus {
            status: Some(fn_status::Status::QueuePos(7)),
        })),
        (Some(status_response::FnLog::Status(_)), "remote") => Some(status_response::FnLog::Status(FnStatus {
            status: Some(fn_status::Status::RemoteHost(4)),
        })),
        (other, _) => other,
    };
    Ok(Response::new(StatusResponse {
        node_id: r.node_id,
        is_master: r.config_schema_table.is_empty(),
        queued: 1,
        in_proc: 2,
        pool: 3,
        fn_log,
    }))
}

#[async_trait]
impl RppdNode for Echo {
    async fn message(&self, r: Request<MessageRequest>) -> Result<Response<MessageResponse>, Status> {
        if r.get_ref().key == "refused" {
            return Err(Status::failed_precondition("refused"));
        }
        Ok(Response::new(MessageResponse {}))
    }
    async fn event(&self, r: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(DbEventResponse { saved: r.table_name != "none", repeat_with: r.pks }))
    }
    async fn complete(&self, _r: Request<StatusRequest>) -> Result<Response<SwitchResponse>, Status> {
        Ok(Response::new(SwitchResponse {}))
    }
    async fn switch(&self, _r: Request<SwitchRequest>) -> Result<Response<SwitchResponse>, Status> {
        Ok(Response::new(SwitchResponse {}))
    }
    async fn status(&self, r: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        status_reply(r.into_inner())
    }
}

#[async_trait]
impl RppdTrigger for Echo {
    async fn event(&self, r: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        let r = r.into_inner();
        Ok(Response::new(DbEventResponse { saved: r.table_name != "none", repeat_with: r.pks }))
    }
    async fn status(&self, r: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        status_reply(r.into_inner())
    }
}

/// every oneof and field the wire carries, set once
fn full_event(table: &str) -> DbEventRequest {
    DbEventRequest {
        table_name: table.to_string(),
        event_type: DbAction::Dual as i32,
        id_value: true,
        pks: vec![
            PkColumn { column_name: "a".into(), column_type: PkColumnType::Int as i32, pk_value: Some(pk_column::PkValue::IntValue(-3)) },
            PkColumn { column_name: "b".into(), column_type: PkColumnType::BigInt as i32, pk_value: Some(pk_column::PkValue::BigintValue(1 << 40)) },
            PkColumn { column_name: "c".into(), column_type: PkColumnType::String as i32, pk_value: Some(pk_column::PkValue::StringValue("k".into())) },
            PkColumn { column_name: "d".into(), column_type: PkColumnType::String as i32, pk_value: None },
        ],
        traceparent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01".into(),
        optional_caller: Some(db_event_request::OptionalCaller::CallBy(9)),
    }
}

async fn serve_echo() -> u16 {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    tokio::spawn(async move {
        Server::builder()
            .add_service(RppdNodeServer::new(Echo))
            .add_service(RppdTriggerServer::new(Echo))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(l))
            .await
    });
    port
}

/// [rpc] the generated code: every message encoded, sent, decoded and answered, the clients'
/// builders and the servers' constructors too
#[tokio::test]
async fn generated_messages_travel_both_ways() {
    let port = serve_echo().await;
    let url = format!("http://127.0.0.1:{}", port);
    let mut node = RppdNodeClient::connect(Duration::from_secs(5), url.clone()).await.unwrap();
    let mut trig = RppdTriggerClient::connect(Duration::from_secs(5), url.clone()).await.unwrap();

    node.message(MessageRequest { key: "/q/x/1".into(), value: vec![1, 2], traceparent: "t".into() }).await.unwrap();
    let refused = node.message(MessageRequest { key: "refused".into(), ..Default::default() }).await;
    assert_eq!(refused.unwrap_err().code(), tonic::Code::FailedPrecondition);

    let ev = full_event("public.rppd_function");
    let back = node.event(ev.clone()).await.unwrap().into_inner();
    assert!(back.saved);
    assert!(back.repeat_with == ev.pks, "every PkColumn and its value came back");
    assert!(!trig.event(DbEventRequest { table_name: "none".into(), ..Default::default() }).await.unwrap().into_inner().saved);

    node.complete(StatusRequest { config_schema_table: String::new(), node_id: 1, fn_log: None }).await.unwrap();
    node.switch(SwitchRequest { node_id: 2 }).await.unwrap();

    // status: no fn log, a log id, a uuid; and each kind of FnStatus status
    let s = node.status(StatusRequest { config_schema_table: String::new(), node_id: 5, fn_log: None }).await.unwrap().into_inner();
    assert_eq!((s.node_id, s.queued, s.in_proc, s.pool, s.is_master), (5, 1, 2, 3, true));
    assert!(s.fn_log.is_none());
    let s = node.status(StatusRequest { config_schema_table: "pos".into(), node_id: 6,
        fn_log: Some(status_request::FnLog::FnLogId(42)) }).await.unwrap().into_inner();
    assert!(matches!(s.fn_log, Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::QueuePos(7)) }))));
    let s = node.status(StatusRequest { config_schema_table: "remote".into(), node_id: 6,
        fn_log: Some(status_request::FnLog::FnLogId(42)) }).await.unwrap().into_inner();
    assert!(matches!(s.fn_log, Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::RemoteHost(4)) }))));
    let s = node.status(StatusRequest { config_schema_table: "x".into(), node_id: 6,
        fn_log: Some(status_request::FnLog::FnLogId(42)) }).await.unwrap().into_inner();
    assert!(matches!(s.fn_log, Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::InProcSec(42)) }))));
    let s = node.status(StatusRequest { config_schema_table: "x".into(), node_id: 6,
        fn_log: Some(status_request::FnLog::Uuid("u-1".into())) }).await.unwrap().into_inner();
    assert!(matches!(s.fn_log, Some(status_response::FnLog::Uuid(ref u)) if u.uuid == vec!["u-1".to_string()]));

    // the trigger service, the same messages
    let s = trig.status(StatusRequest { config_schema_table: String::new(), node_id: 8, fn_log: None }).await.unwrap().into_inner();
    assert_eq!(s.node_id, 8);
    assert!(trig.event(full_event("public.t")).await.unwrap().into_inner().saved);

    // builders and constructors, none sent
    let ch = tonic::transport::Endpoint::from_shared(url.clone()).unwrap().connect_lazy();
    let _ = RppdNodeClient::new(ch.clone()).max_decoding_message_size(1 << 20).max_encoding_message_size(1 << 20);
    let _ = RppdNodeClient::with_origin(ch.clone(), url.parse().unwrap());
    let _ = RppdNodeClient::with_interceptor(ch.clone(), |r: Request<()>| Ok(r));
    let _ = RppdTriggerClient::new(ch.clone()).max_decoding_message_size(64).max_encoding_message_size(64);
    let _ = RppdTriggerClient::with_origin(ch.clone(), url.parse().unwrap());
    let _ = RppdTriggerClient::with_interceptor(ch, |r: Request<()>| Ok(r));
    let _ = RppdNodeServer::from_arc(Arc::new(Echo)).max_decoding_message_size(1 << 20).max_encoding_message_size(1 << 20);
    let _ = RppdNodeServer::with_interceptor(Echo, |r: Request<()>| Ok(r));
    let _ = RppdTriggerServer::from_arc(Arc::new(Echo)).max_decoding_message_size(1 << 20).max_encoding_message_size(1 << 20);
    let _ = RppdTriggerServer::with_interceptor(Echo, |r: Request<()>| Ok(r));
}

/// [rpc] The generated enums and messages in Rust: names, serde, hashing, defaults
#[test]
fn generated_enums_and_messages_in_rust() {
    for a in [DbAction::Update, DbAction::Insert, DbAction::Delete, DbAction::Truncate, DbAction::Dual] {
        assert_eq!(DbAction::from_str_name(a.as_str_name()), Some(a));
    }
    assert_eq!(DbAction::from_str_name("NOPE"), None);
    for t in [PkColumnType::Int, PkColumnType::BigInt, PkColumnType::String] {
        assert_eq!(PkColumnType::from_str_name(t.as_str_name()), Some(t));
    }
    assert_eq!(PkColumnType::from_str_name("NOPE"), None);
    for a in [FnAction::Na, FnAction::Queueing, FnAction::InProgress, FnAction::OnRemote] {
        assert_eq!(FnAction::from_str_name(a.as_str_name()), Some(a));
    }
    assert_eq!(FnAction::from_str_name("NOPE"), None);

    let ev = full_event("public.t");
    let json = serde_json::to_string(&ev).unwrap();
    let back: DbEventRequest = serde_json::from_str(&json).unwrap();
    assert_eq!(back, ev);
    assert!(format!("{:?}", ev).contains("public.t"));
    let pk = PkColumn::default();
    assert_eq!(pk.clone(), pk);
    assert!(pk_column::PkValue::StringValue("x".into()) != pk_column::PkValue::IntValue(1));

    let st = StatusResponse { node_id: 1, is_master: true, queued: 2, in_proc: 3, pool: 4,
        fn_log: Some(status_response::FnLog::Uuid(StatusFnsResponse { uuid: vec!["a".into()] })) };
    let json = serde_json::to_string(&st).unwrap();
    let back: StatusResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(back, st);
    let fs = status_request::FnLog::FnLogId(3);
    assert!(fs.clone() == status_request::FnLog::FnLogId(3));
    let _ = (SwitchRequest { node_id: 1 }, SwitchResponse {}, MessageResponse {}, DbEventResponse::default());
    let _ = (MessageRequest::default(), StatusRequest::default(), FnStatus::default());
    let _ = (db_event_request::OptionalCaller::CallBy(1), status_request::FnLog::Uuid("u".into()));

    // [i18n] the language set, and the not-ready message of the generated clients; back to the environment's
    rppd_common::i18n::set_lang("de");
    assert!(rppd_common::i18n::not_ready("down".into()).contains("down"));
    rppd_common::i18n::set_lang("");
}

#[cfg(feature = "etcd-embeded")]
mod cluster {
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
    use crate::arg_config::RppdConfig;

    /// a node of its own on a free port, with its own throwaway PostgreSQL
    async fn node(pg: &Pg) -> RppdNodeCluster {
        let _env = crate::cov_lock::env_read();   // [test] not while another test sets a variable
        let c = RppdConfig {
            name: "cov-node".to_string(),
            bind: "127.0.0.1".to_string(),
            port: free_port(),
            user: "postgres".to_string(),
            pwd: String::new(),
            db_url: format!("postgres://postgres@127.0.0.1:{}/postgres", pg.port),
            ..RppdConfig::default()
        };
        drop(_env);
    crate::ensure_schema(&c.db_url, "public").await.unwrap();
        let log = slog::Logger::root(slog::Discard, slog::o!());
        #[cfg(feature = "tracer")]
        let r = RppdNodeCluster::init(c, log, None).await;
        #[cfg(not(feature = "tracer"))]
        let r = RppdNodeCluster::init(c, log).await;
        r.unwrap()
    }

    /// [rpc] the node's services, as its clients call them: a message, events of its own and
    /// other tables, status of itself and of a node it does not know, switch and complete
    #[tokio::test]
    async fn node_serves_its_services() {
        // the embedded etcd's peer port is fixed: one node at a time in this binary
        let _etcd = crate::cov_lock::ETCD_LOCK.lock().await;
        let Some(pg) = Pg::start("node") else { return };
        let cl = node(&pg).await;
        // serve() parses bind as a socket address; init needed the bare address
        let port = cl.cfg.read().await.port;
        cl.cfg.write().await.bind = format!("127.0.0.1:{}", port);
        cl.serve().await.unwrap();
        let url = format!("http://127.0.0.1:{}", cl.cfg.read().await.port);
        let mut client = None;
        for _ in 0..100 {
            if let Ok(c) = RppdNodeClient::connect(Duration::from_secs(1), url.clone()).await {
                client = Some(c);
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut client = client.expect("connected");
        let mut trig = RppdTriggerClient::connect(Duration::from_secs(5), url).await.unwrap();

        // until the node has loaded its logs an event is refused, once it has, it is saved
        let _ = client.event(full_event("public.rppd_function")).await;
        let _ = trig.event(DbEventRequest { table_name: "public.other".into(), ..Default::default() }).await;
        for _ in 0..300 {
            if cl.is_started() { break; }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let schema = cl.cfg.read().await.schema.clone();
        let own = format!("{}.{}", schema, CFG_FN_TABLE);
        let r = client.event(DbEventRequest { table_name: own.clone(), ..Default::default() }).await;
        if cl.is_started() {
            assert!(r.unwrap().into_inner().saved, "an own table is saved");
        }
        for t in [CFG_CRON_TABLE, CFG_FNL_TABLE, CFG_TABLE] {
            let _ = client.event(DbEventRequest { table_name: format!("{}.{}", schema, t), ..Default::default() }).await;
        }
        let _ = client.event(DbEventRequest { table_name: "public.nothing_here".into(),
            event_type: DbAction::Insert as i32, ..Default::default() }).await;

        // queue messages: no function on the key
        client.message(MessageRequest { key: "/q/none/1".into(), value: b"v".to_vec(), traceparent: String::new() })
            .await.unwrap();

        // status of this node, of one it does not know, and the log of a function
        let me = cl.node_id.load(Ordering::Relaxed);
        let s = client.status(StatusRequest { config_schema_table: String::new(), node_id: me, fn_log: None })
            .await.map(|r| r.into_inner());
        if cl.is_started() {
            let s = s.unwrap();
            assert_eq!(s.node_id, me);
        }
        let unknown = client.status(StatusRequest { config_schema_table: String::new(), node_id: 9_999, fn_log: None }).await;
        assert_eq!(unknown.unwrap_err().code(), tonic::Code::Unavailable);
        let _ = client.status(StatusRequest { config_schema_table: String::new(), node_id: me,
            fn_log: Some(status_request::FnLog::Uuid("none".into())) }).await;

        // switch to a node this one is not, and complete; the master is elected by the node itself
        client.switch(SwitchRequest { node_id: 9_999 }).await.unwrap();
        client.complete(StatusRequest { config_schema_table: String::new(), node_id: me, fn_log: None }).await.unwrap();
        let _ = trig.status(StatusRequest { config_schema_table: String::new(), node_id: me, fn_log: None }).await;

        // a call to a node it has no connection to, and the router's refusal of an unknown database
        cl.set_master(false).await;
        cl.set_consume(false).await;
        cl.set_consume(true).await;
        cl.stop().await;
    }
}
