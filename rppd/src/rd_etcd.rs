use slog::{error, warn, Logger};
use uuid::Uuid;
use rppd_common::protogen::rppd::MessageRequest;

#[cfg(not(feature = "etcd-external"))]
use etcds::{
    etcdpb::etcdserverpb::{
        WatchRequest,
        DeleteRangeRequest,
        kv_server::Kv,
        watch_request,
        WatchCancelRequest,
        WatchCreateRequest,
        PutRequest,
    },
    cluster::EtcdNode,
};


use etcds::queue::QueueNameKey;

#[cfg(feature = "etcd-external")] use tokio_stream::StreamExt;

#[cfg(not(feature = "etcd-external"))] use tokio::sync::mpsc::{channel, error::SendError, Receiver, Sender, WeakSender};
#[cfg(not(feature = "etcd-external"))] use std::{collections::HashMap, sync::Arc};
#[cfg(not(feature = "etcd-external"))] use etcds::etcdpb::etcdserverpb::WatchResponse;
#[cfg(not(feature = "etcd-external"))] use tonic::Request;
use crate::rd_config::WatcherW;

#[cfg(feature = "etcd-embeded")] use tonic::transport::server::Router;
#[cfg(feature = "etcd-embeded")] use etcds::cli::EtcdConfig;
#[allow(unused_imports)] #[cfg(feature = "tracer")] use opentelemetry::trace::*;
#[allow(unused_imports)] #[cfg(feature = "tracer")] use opentelemetry_sdk::trace::*;
use crate::arg_config::RppdConfig;
use crate::rd_config::RppdNodeCluster;
use crate::rd_fn::RpFn;
use crate::LP;

#[cfg(all(feature = "etcd-embeded", feature = "etcd-provided", feature = "etcd-external"))]
compile_error!("only single etcd-embeded or etcd-provided or etcd-external feature can be enabled at the same time");
#[cfg(all(feature = "etcd-embeded", feature = "etcd-provided"))]
compile_error!("only single etcd-embeded or etcd-provided feature can be enabled at the same time");
#[cfg(all(feature = "etcd-external", feature = "etcd-provided"))]
compile_error!("only single etcd-external or etcd-provided feature can be enabled at the same time");
#[cfg(all(feature = "etcd-embeded", feature = "etcd-external"))]
compile_error!("only single etcd-embeded or etcd-external feature can be enabled at the same time");


/// Adapter to various ETCD implementation for features (at least one must set):
/// - etcd-embeded -- use embeded etcd server with internal endpoint reuse, while rppd use as standalone app / node
/// - etcd-provided -- use provided etcd server instance, while rppd use as library and outsider endpoints re-used
/// - etcd-external -- use external etcd server, like valilla implementation
pub(crate) struct EtcdConnector {
    /// encapsulate an attempt to establishing a node start
    #[cfg(any(feature = "etcd-embeded", feature = "etcd-provided"))] etcd: Result<EtcdNode, String>,

    /// encapsulate an attempt to connect to existing cluster
    #[cfg(feature = "etcd-external")] etcd: Result<etcd_client::Client, String>,

    /// The node uuid
    client_id: Uuid,
    /// host used to bind a server or connect to existing etcd
    pub(crate) host: String,
    /// port used to bind a server or connect to existing etcd
    pub(crate) port: String,

    log: Logger,
}

#[cfg(feature = "etcd-external")]
const DEFAULT_ENDPOINT: &str = "localhost:2379";

/// client-assigned watch id: every queue uses its own watch stream, so a fixed id is unique per stream
#[cfg(feature = "etcd-external")]
const EXTERNAL_WATCH_ID: i64 = 1;

impl EtcdConnector {

    #[cfg(feature = "etcd-embeded")]
    pub fn add_all_services(&self, srv: Router) -> Router {
        if let Ok(etcd) = &self.etcd {
            etcd.add_all_services(srv)
        } else {
            srv
        }
    }

    #[cfg(feature = "etcd-embeded")]
    pub(crate) async fn init(cfg: &RppdConfig, log: &Logger,
         #[cfg(feature = "tracer")] tracer: Option<SdkTracer>,
    ) -> Self {
        let etcd = EtcdNode::init(EtcdConfig {
            node: cfg.node.to_string(),
            cluster: cfg.cluster.to_string(),
            name: cfg.name.clone(),
            listen_client_urls: format!("{}:{}", cfg.bind, cfg.port),
            ..Default::default()
        }, log.clone(),
          #[cfg(feature = "tracer")] tracer
        ).await;

        EtcdConnector {
            etcd,
            client_id: cfg.node,
            host: cfg.bind.clone(),
            port: cfg.port.to_string(),
            log: log.clone(),
        }
    }

    ///  use provided etcd server instance, while rppd use as library and re-used existing endpoints/binding port for either rppd and etcd
    #[cfg(feature = "etcd-provided")]
    pub(crate) async fn new(node: EtcdNode, c: &RppdConfig, log: &slog::Logger) -> Self {
        EtcdConnector {
            etcd: Ok(node),
            client_id: c.node,
            host: c.bind.clone(),
            port: c.port.to_string(),
            log: log.clone(),
        }
    }

    /// TODO implement etcd login&pwd capabilities to vanila implementation
    #[cfg(feature = "etcd-external")]
    pub(crate) async fn connect(cfg: &RppdConfig, log: &Logger) -> Self {
        let endpoint = std::env::var_os("ETCD_ENDPOINT")
            .map(|v| v.to_str().unwrap_or(DEFAULT_ENDPOINT).to_string())
            .unwrap_or(DEFAULT_ENDPOINT.to_string());

        let etcd = etcd_client::Client::connect([endpoint.as_str()], None).await
            .map_err(|e| tr!("etcd.connect", endpoint = endpoint, err = e));

        EtcdConnector {
            etcd,
            client_id: cfg.node,
            host: cfg.bind.clone(),
            port: cfg.port.to_string(),
            log: log.clone(),
        }
    }

    /// [cadence] Put a message on a queue: `key` is a producer key, `<q>/producer/<id>`.
    #[cfg(not(feature = "etcd-external"))]
    pub(crate) async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), String> {
        match &self.etcd {
            Ok(etcd) => etcd.put(Request::new(PutRequest {
                key: key.as_bytes().to_vec(), value, ..Default::default()
            })).await.map(|_| ()).map_err(|e| e.to_string()),
            Err(e) => Err(tr!("etcd.not_ready", err = e)),
        }
    }

    /// [cadence] Put a message on a queue of an external etcd.
    #[cfg(feature = "etcd-external")]
    pub(crate) async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), String> {
        match &mut self.etcd {
            Ok(etcd) => etcd.put(key, value, None).await.map(|_| ()).map_err(|e| e.to_string()),
            Err(e) => Err(tr!("etcd.not_ready", err = e)),
        }
    }

    /// Asknoledge the message receiving by deleting key from etcd queue.
    /// There is no queue in external i.e. no asknoledge required
    #[cfg(not(feature = "etcd-external"))]
    pub(crate) async fn aks(&self, key: &String) {
        if let Ok(etcd) = &self.etcd
            && let Err(e) = etcd.delete_range(Request::new(DeleteRangeRequest{
                key: key.clone().into_bytes(), ..Default::default()
            })).await {
                error!(self.log, "{}{}", LP, tr!("etcd.cleanup", key = key, err = e));
            }
    }

}

impl RpFn {
    pub(crate) async fn watch_init(mut self, cluster: &RppdNodeCluster) -> Self {
        self.watch_merge(None, cluster).await;
        self
    }

    /// update etcd watch if requered
    /// new_name - is a new schema_table value to be used to replase existing one
    pub(crate) async fn watch_merge(&mut self, new_name: Option<&String>, cluster: &RppdNodeCluster) {
        if !self.schema_table.starts_with("/") {
            return;
        }
        let q = QueueNameKey::new(self.schema_table.clone());
        if !q.is_queue() {
            return;
        }
        // [rppd-spare] no watcher, so etcd hands this node no message to lose
        if !cluster.consume.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }

        let mut con = cluster.etcd.write().await;
        let log = con.log.clone();
        
        {
            let watchers = cluster.watchers.read().await;
            if let Some(name) =  new_name { // in case of update queue for a function
                if watchers.contains_key(&self.schema_table) && name !=& self.schema_table{
                    // delete queue_name
                    #[allow(unused_mut)]
                    if let Some(mut w) = cluster.watchers.write().await
                        .remove(&self.schema_table)
                        && let Err(e) = w.cancel().await {
                            warn!(log, "{}{}", LP, tr!("etcd.unwatch", queue = &self.schema_table, err = e));
                        }
                }
                if watchers.contains_key(name) {
                    return; // already exists
                }
            } else {
                if watchers.contains_key(&self.schema_table) {
                    return; // already exists
                }
            }
        }

        let queue_name = new_name.unwrap_or(&self.schema_table).to_owned();
        let client_id = con.client_id;
        // [db-tag] the queue in this instance's namespace; the watcher is kept by its name
        let queue_consumer_name = format!("{}/consumer/{}", cluster.cfg.read().await.qkey(&queue_name),
            client_id.as_hyphenated());
        match &mut con.etcd {
            Ok(etcd) => {
                let sender = cluster.kv_sender.clone();

                #[cfg(not(feature = "etcd-external"))] {
                    let (request_sender, request_receiver) = channel(100);
                    let (response_sender, mut response_receiver) = channel(100);
                    etcd.new_watcher(request_receiver, response_sender.clone(), &client_id).await;

                    // [q-dead] kept to watch again, if etcd cancels the watch
                    let consumer_key = queue_consumer_name.clone();
                    match request_sender.send(WatchRequest {
                        request_union: Some(watch_request::RequestUnion::CreateRequest(
                            // [watch-id] One id per queue. The embedded etcd keys a client's
                            // watchers by watch_id and takes 0 literally - not as "assign one",
                            // as etcd does - so every queue after the first was refused with
                            // "Watcher#0 already on <the first queue>" and only one queue per
                            // node was ever consumed (topology 92c: /q/cad_watch and
                            // /q/cad_deadline never delivered).
                            WatchCreateRequest { key: queue_consumer_name.into_bytes(),
                                watch_id: next_watch_id(), ..Default::default() }
                        ))
                    }).await {
                        Ok(()) => {
                            // the first response is registration and should not have events
                            match response_receiver.recv().await {
                                Some(r) => {
                                    match r {
                                        Ok(watcher) => {
                                            // [q-dead] weak: the request side ends with the
                                            // WatcherW, as when rppd cancels the watch itself
                                            let requests = request_sender.downgrade();
                                            if let Some(w) = cluster.watchers.write().await
                                                .insert(queue_name.clone(), WatcherW {
                                                    sender: request_sender,
                                                    watch_id: watcher.watch_id,
                                                }) { // just in case is something is there
                                                if let Err(e) = w.cancel().await {
                                                    warn!(log, "{}{}", LP, tr!("etcd.unwatch", queue = queue_name, err = e));
                                                }
                                            }

                                            tokio::spawn(consume(response_receiver, requests, consumer_key,
                                                queue_name, cluster.watchers.clone(), sender, log));
                                        }
                                        Err(e) => {
                                            warn!(log, "{}{}", LP, tr!("etcd.register", queue = queue_name, err = e));
                                        }
                                    }
                                }
                                None => {
                                    warn!(log, "{}{}", LP, tr!("etcd.register_silent", queue = queue_name));
                                }
                            }
                        }
                        Err(e) => {
                            error!(cluster.log, "{}{}", LP, tr!("etcd.watch_failed", queue = queue_name, err = e));
                        }
                    }
                }

                #[cfg(feature = "etcd-external")]
                match etcd.watch(queue_consumer_name.clone().into_bytes(),
                                 Some(etcd_client::WatchOptions::new().with_watch_id(EXTERNAL_WATCH_ID))).await {
                    Ok(stream) => {
                        let (watch_sender, stream) = stream.split();
                        let token = next_watch_stream();
                        // register queue_name to watch_id
                        if let Some(mut w) = cluster.watchers.write().await
                            .insert(queue_name.clone(), WatcherW { sender: watch_sender, watch_id: EXTERNAL_WATCH_ID,
                                stream: token }) { // just in case is something there
                            if let Err(e) = w.cancel().await {
                                warn!(log, "{}{}", LP, tr!("etcd.unwatch", queue = queue_name, err = e));
                            }
                        }
                        tokio::spawn(consume_external(stream, token, queue_consumer_name, queue_name,
                            cluster.watchers.clone(), sender, log));
                    },
                    Err(e) => {
                        error!(log, "{}{}", LP, tr!("etcd.watch_failed", queue = queue_name, err = e));
                    }
                }
            }
            Err(e) => {
                error!(cluster.log, "{}{}", LP, tr!("etcd.watch_not_ready", queue = queue_name, err = e));
            }
        }
    }

    #[cfg(not(feature = "etcd-external"))]
    #[allow(dead_code)]
    pub(crate) fn is_etcd_queue(&self) -> bool {
        self.schema_table.starts_with("/q/")
        || self.schema_table.starts_with("/queue/")
    }
}

/// Read one queue's watch stream into rppd's message channel.
///
/// [q-dead] A watch etcd cancels from its side - its consumer did not take a
/// message in time (ETCD-E052), the reason in the frame - is made again on the
/// same stream under a new id, and the queue's `WatcherW` takes that id: else
/// rppd stopped consuming that queue for the life of the process. What it was
/// handed and has not acknowledged stays its own meanwhile. A watch rppd
/// cancelled itself - no reason in the frame, its `WatcherW` dropped with the
/// request side - is not made again.
#[cfg(not(feature = "etcd-external"))]
async fn consume(mut responses: Receiver<Result<WatchResponse, tonic::Status>>, requests: WeakSender<WatchRequest>,
    consumer_key: String, queue_name: String,
    watchers: Arc<tokio::sync::RwLock<HashMap<crate::rd_config::SchemaTableType, WatcherW>>>,
    sender: Sender<MessageRequest>, log: Logger)
{
    let mut again = Again::default();
    while let Some(resp) = responses.recv().await {
        match resp {
            Ok(w) => {
                if w.canceled && !w.cancel_reason.is_empty() {
                    warn!(log, "{}{}", LP, tr!("etcd.cancelled", queue = queue_name, reason = w.cancel_reason));
                    let Some(req) = requests.upgrade() else { continue };
                    // [q-dead] only the queue's current watch on this stream: a
                    // cancel of another - repeated, or an older one - would make a
                    // second watch that rppd's own cancel never removes
                    let current = watchers.read().await.get(&queue_name)
                        .filter(|ww| ww.sender.same_channel(&req)).map(|ww| ww.watch_id);
                    if current != Some(w.watch_id) {
                        continue;
                    }
                    again.wait().await;
                    let id = next_watch_id();
                    match req.send(WatchRequest { request_union: Some(watch_request::RequestUnion::CreateRequest(
                        WatchCreateRequest { key: consumer_key.clone().into_bytes(), watch_id: id, ..Default::default() })) }).await {
                        Ok(()) => {
                            if let Some(ww) = watchers.write().await.get_mut(&queue_name)
                                && ww.sender.same_channel(&req) {
                                    ww.watch_id = id;
                                }
                        }
                        Err(e) => error!(log, "{}{}", LP, tr!("etcd.rewatch", queue = queue_name, err = e)),
                    }
                    continue;
                }
                if !w.events.is_empty() {
                    again = Again::default(); // it is consuming again
                }
                for e in w.events {
                    if let Some(kv) = e.kv
                        && let Err(e) = sender.send(MessageRequest {
                            key: String::from_utf8_lossy(&kv.key).to_string(),
                            value: kv.value.to_vec(),
                            // [otel] etcds' KeyValue carries no trace (its proto
                            // dropped the field): the message's trace is not continued
                            traceparent: String::new(),
                        }).await {
                            error!(log, "{}{}", LP, tr!("etcd.consume", queue = queue_name, err = e));
                        }
                }
            }
            Err(e) => {
                error!(log, "{}{}", LP, tr!("etcd.watch_failed", queue = queue_name, err = e));
            }
        }
    }
}

#[cfg(not(feature = "etcd-external"))]
impl WatcherW {
    pub(crate) async fn cancel(&self) -> Result<(), SendError<WatchRequest>> {
        self.sender.send(
            WatchRequest{ request_union: Some(
                watch_request::RequestUnion::CancelRequest(
                    WatchCancelRequest { watch_id: self.watch_id}
                ))
            }).await
    }
}

/// Read one queue's watch stream from an external etcd into rppd's message
/// channel.
///
/// [q-dead] As the embedded path does (`consume`): a watch the server cancels
/// from its side - a reason in the frame - is made again on the same stream and
/// under the same id, the cancelled one being gone, through the queue's
/// `WatcherW` - while it is still this stream's (`stream`). One rppd cancelled
/// itself - no reason, its `WatcherW` taken out first - is not.
#[cfg(feature = "etcd-external")]
async fn consume_external<S>(mut stream: S, token: u64, consumer_key: String, queue_name: String,
    watchers: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<crate::rd_config::SchemaTableType, WatcherW>>>,
    sender: tokio::sync::mpsc::Sender<MessageRequest>, log: Logger)
where S: tokio_stream::Stream<Item = Result<etcd_client::WatchResponse, etcd_client::Error>> + Unpin
{
    let mut again = Again::default();
    while let Some(resp) = stream.next().await {
        match resp {
            Ok(v) => {
                if v.canceled() && !v.cancel_reason().is_empty() {
                    warn!(log, "{}{}", LP, tr!("etcd.cancelled", queue = queue_name, reason = v.cancel_reason()));
                    // [q-dead] only the queue's current watch on this stream, and
                    // not in a tight loop: a cancel that repeats (permission, a
                    // refused prefix) is retried further and further apart
                    let current = watchers.read().await.get(&queue_name)
                        .filter(|w| w.stream == token).map(|w| w.watch_id);
                    if current != Some(v.watch_id()) {
                        continue;
                    }
                    again.wait().await;
                    if let Some(w) = watchers.write().await.get_mut(&queue_name).filter(|w| w.stream == token) {
                        let opt = etcd_client::WatchOptions::new().with_watch_id(w.watch_id);
                        // bounded: the queue's watchers are held meanwhile
                        match tokio::time::timeout(std::time::Duration::from_secs(1),
                            w.sender.watch(consumer_key.clone(), Some(opt))).await {
                            Ok(Ok(())) => {}
                            Ok(Err(e)) => error!(log, "{}{}", LP, tr!("etcd.rewatch", queue = queue_name, err = e)),
                            Err(_) => error!(log, "{}{}", LP, tr!("etcd.rewatch_full", queue = queue_name)),
                        }
                    }
                    continue;
                }
                if !v.events().is_empty() {
                    again = Again::default(); // it is consuming again
                }
                for e in v.events() {
                    if let Some(kv) = e.kv()
                        && let Err(e) = sender.send(MessageRequest {
                            key: kv.key_str().unwrap_or("").to_string(),
                            value: kv.value().to_vec(),
                            // an external etcd keeps no trace with a message
                            traceparent: String::new(),
                        }).await {
                            error!(log, "{}{}", LP, tr!("etcd.consume", queue = queue_name, err = e));
                        }
                }
            }
            Err(e) => {
                error!(log, "{}{}", LP, tr!("etcd.watch_failed", queue = queue_name, err = e));
            }
        }
    }
}

/// [q-dead] The wait before watching a queue again after a server cancel: none
/// the first time, then 200 ms, doubling up to 30 s, until a message arrives.
#[derive(Default)]
struct Again(std::time::Duration);

impl Again {
    async fn wait(&mut self) {
        if !self.0.is_zero() {
            tokio::time::sleep(self.0).await;
        }
        self.0 = (self.0 * 2).max(std::time::Duration::from_millis(200)).min(std::time::Duration::from_secs(30));
    }
}

/// [q-dead] A process-wide counter: which watch stream a `WatcherW` is.
#[cfg(feature = "etcd-external")]
fn next_watch_stream() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(feature = "etcd-external")]
impl WatcherW {
    pub(crate) async fn cancel(&mut self) -> Result<(), etcd_client::Error> {
        self.sender.cancel(self.watch_id).await
    }
}

/// [watch-id] A process-wide counter: watch ids must differ per client, and every
/// queue of this node watches as the same client.
#[cfg(not(feature = "etcd-external"))]
fn next_watch_id() -> i64 {
    static NEXT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

#[cfg(all(test, not(feature = "etcd-external")))]
mod tests {
    use super::*;
    use etcds::etcdpb::mvccpb::{Event, KeyValue};
    use std::time::Duration;

    fn created(req: Option<WatchRequest>) -> WatchCreateRequest {
        match req.and_then(|r| r.request_union) {
            Some(watch_request::RequestUnion::CreateRequest(c)) => c,
            other => panic!("not a create: {:?}", other.is_some()),
        }
    }

    /// [q-dead] A watch etcd cancels (ETCD-E052) is made again on the same
    /// stream, under a new id the queue's WatcherW takes; messages after it
    /// are consumed. One cancelled without a reason - rppd's own - is not.
    #[tokio::test]
    async fn a_watch_etcd_cancels_is_made_again() {
        let (req_tx, mut req_rx) = channel(10);
        let (resp_tx, resp_rx) = channel(10);
        let (msg_tx, mut msg_rx) = channel(10);
        let watchers = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        watchers.write().await.insert("q".to_string(), WatcherW { sender: req_tx.clone(), watch_id: 7 });
        let log = Logger::root(slog::Discard, slog::o!());
        tokio::spawn(consume(resp_rx, req_tx.downgrade(), "/q/x/consumer/u".to_string(), "q".to_string(),
            watchers.clone(), msg_tx, log));

        resp_tx.send(Ok(WatchResponse { watch_id: 7, canceled: true, ..Default::default() })).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(200), req_rx.recv()).await.is_err(), "its own cancel");

        resp_tx.send(Ok(WatchResponse { watch_id: 7, canceled: true,
            cancel_reason: "ETCD-E052 too slow".to_string(), ..Default::default() })).await.unwrap();
        let c = created(tokio::time::timeout(Duration::from_secs(2), req_rx.recv()).await.ok().flatten());
        assert_eq!(c.key, b"/q/x/consumer/u");
        assert_ne!(c.watch_id, 7, "a new id");
        assert_eq!(watchers.read().await["q"].watch_id, c.watch_id, "the WatcherW takes it");

        resp_tx.send(Ok(WatchResponse { watch_id: c.watch_id, events: vec![Event { r#type: 0,
            kv: Some(KeyValue { key: b"/q/x/1/k".to_vec(), value: b"v".to_vec(), ..Default::default() }),
            prev_kv: None }], ..Default::default() })).await.unwrap();
        assert_eq!(msg_rx.recv().await.unwrap().key, "/q/x/1/k");

        // rppd dropped the watch: nothing to make again on
        watchers.write().await.remove("q");
        drop(req_tx);
        resp_tx.send(Ok(WatchResponse { watch_id: c.watch_id, canceled: true,
            cancel_reason: "ETCD-E052 too slow".to_string(), ..Default::default() })).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(200), req_rx.recv()).await.map_or(true, |r| r.is_none()));
    }
}

#[cfg(all(test, not(feature = "etcd-external")))]
mod again_tests {
    use super::*;
    use std::time::Duration;

    fn cancel(id: i64) -> Result<WatchResponse, tonic::Status> {
        Ok(WatchResponse { watch_id: id, canceled: true, cancel_reason: "ETCD-E052 too slow".to_string(), ..Default::default() })
    }

    /// [q-dead] Only the queue's current watch is made again - not another one
    /// cancelled on the same stream - and a cancel that repeats is retried
    /// further apart, not in a tight loop.
    #[tokio::test]
    async fn only_the_current_watch_is_made_again_and_not_in_a_tight_loop() {
        let (req_tx, mut req_rx) = channel(10);
        let (resp_tx, resp_rx) = channel(10);
        let (msg_tx, _msg_rx) = channel(10);
        let watchers = Arc::new(tokio::sync::RwLock::new(HashMap::new()));
        watchers.write().await.insert("q".to_string(), WatcherW { sender: req_tx.clone(), watch_id: 7 });
        tokio::spawn(consume(resp_rx, req_tx.downgrade(), "/q/x/consumer/u".to_string(), "q".to_string(),
            watchers.clone(), msg_tx, Logger::root(slog::Discard, slog::o!())));

        resp_tx.send(cancel(9)).await.unwrap(); // not its watch
        assert!(tokio::time::timeout(Duration::from_millis(200), req_rx.recv()).await.is_err());

        resp_tx.send(cancel(7)).await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(100), req_rx.recv()).await.is_ok(), "at once the first time");
        let id = watchers.read().await["q"].watch_id;
        let t = std::time::Instant::now();
        resp_tx.send(cancel(id)).await.unwrap(); // cancelled again
        assert!(tokio::time::timeout(Duration::from_secs(2), req_rx.recv()).await.is_ok());
        assert!(t.elapsed() >= Duration::from_millis(190), "then after a wait: {:?}", t.elapsed());
    }
}

#[cfg(all(test, feature = "etcd-external"))]
mod external_tests {
    use super::*;
    use etcd_client::proto::{PbWatchRequest, PbWatchResponse};

    fn frame(canceled: bool, reason: &str, key: Option<&str>) -> Result<etcd_client::WatchResponse, etcd_client::Error> {
        let events = key.map(|k| vec![etcd_client::proto::PbEvent { r#type: 0,
            kv: Some(etcd_client::proto::PbKeyValue { key: k.as_bytes().to_vec(), value: b"v".to_vec(), ..Default::default() }),
            prev_kv: None }]).unwrap_or_default();
        Ok(etcd_client::WatchResponse(PbWatchResponse { watch_id: 1, canceled, cancel_reason: reason.to_string(),
            events, ..Default::default() }))
    }

    /// [q-dead] Against an external etcd too: a watch the server cancels is
    /// made again on the same stream; one rppd cancelled itself is not, and
    /// nor through a `WatcherW` of another stream.
    #[tokio::test]
    async fn a_watch_an_external_etcd_cancels_is_made_again() {
        let (req_tx, mut req_rx) = tokio::sync::mpsc::channel::<PbWatchRequest>(10);
        let watchers = std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::new()));
        watchers.write().await.insert("q".to_string(),
            WatcherW { sender: etcd_client::WatchRequestSender(req_tx), watch_id: EXTERNAL_WATCH_ID, stream: 7 });
        let (msg_tx, mut msg_rx) = tokio::sync::mpsc::channel(10);
        let (resp_tx, resp_rx) = tokio::sync::mpsc::channel(10);
        let log = Logger::root(slog::Discard, slog::o!());
        tokio::spawn(consume_external(tokio_stream::wrappers::ReceiverStream::new(resp_rx), 7,
            "/q/x/consumer/u".to_string(), "q".to_string(), watchers.clone(), msg_tx, log));

        resp_tx.send(frame(true, "", None)).await.unwrap(); // its own cancel
        assert!(tokio::time::timeout(std::time::Duration::from_millis(200), req_rx.recv()).await.is_err());

        resp_tx.send(frame(true, "compacted", None)).await.unwrap();
        let again = tokio::time::timeout(std::time::Duration::from_secs(2), req_rx.recv()).await
            .ok().flatten().expect("made again");
        match again.request_union {
            Some(etcd_client::proto::PbWatchRequestUnion::CreateRequest(c)) => {
                assert_eq!(c.key, b"/q/x/consumer/u");
                assert_eq!(c.watch_id, EXTERNAL_WATCH_ID);
            }
            other => panic!("not a create: {:?}", other),
        }
        resp_tx.send(frame(false, "", Some("/q/x/1/k"))).await.unwrap();
        assert_eq!(msg_rx.recv().await.unwrap().key, "/q/x/1/k", "and consumed on");

        // a cancel of another watch id on this stream: not made again
        let other = Ok(etcd_client::WatchResponse(PbWatchResponse { watch_id: 5, canceled: true,
            cancel_reason: "compacted".to_string(), ..Default::default() }));
        resp_tx.send(other).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(200), req_rx.recv()).await.is_err());
        // cancelled twice in a row: the second time after a wait
        resp_tx.send(frame(true, "compacted", None)).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(100), req_rx.recv()).await.is_ok());
        let t = std::time::Instant::now();
        resp_tx.send(frame(true, "compacted", None)).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_secs(2), req_rx.recv()).await.is_ok());
        assert!(t.elapsed() >= std::time::Duration::from_millis(190), "not in a tight loop: {:?}", t.elapsed());

        // the queue watches on another stream now: not through that one
        watchers.write().await.get_mut("q").unwrap().stream = 8;
        resp_tx.send(frame(true, "compacted", None)).await.unwrap();
        assert!(tokio::time::timeout(std::time::Duration::from_millis(200), req_rx.recv()).await.is_err());
    }
}
