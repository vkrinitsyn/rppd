use slog::{error, warn, Logger};
use uuid::Uuid;
use rppd_common::protogen::rppd::MessageRequest;

#[cfg(not(feature = "etcd-external"))]
use etcd::{
    etcdpb::etcdserverpb::{
        WatchRequest,
        DeleteRangeRequest,
        kv_server::Kv,
        watch_request,
        WatchCancelRequest,
        WatchCreateRequest
    },
    cluster::EtcdNode,
};


use etcd::queue::QueueNameKey;

#[cfg(feature = "etcd-external")] use tokio_stream::StreamExt;

#[cfg(not(feature = "etcd-external"))] use tokio::sync::mpsc::{channel, SendError};
#[cfg(not(feature = "etcd-external"))] use tonic::Request;
#[cfg(not(feature = "etcd-external"))] use crate::rd_config::WatcherW;

#[cfg(feature = "etcd-embeded")] use tonic::transport::server::Router;
#[cfg(feature = "etcd-embeded")] use etcd::cli::EtcdConfig;

use crate::arg_config::RppdConfig;
use crate::rd_config::RppdNodeCluster;
use crate::rd_fn::RpFn;

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

    log: Logger,
}

#[cfg(feature = "etcd-external")]
const DEFAULT_ENDPOINT: &'static str = "localhost:2379";

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
    pub(crate) async fn init(cfg: &RppdConfig, log: &slog::Logger) -> Self {
        let etcd = EtcdNode::init(EtcdConfig {
            node: cfg.node.to_string(),
            cluster: cfg.cluster.to_string(),
            name: cfg.name.clone(),
            ..Default::default()
        }, log.clone()).await;

        EtcdConnector {
            etcd,
            client_id: cfg.node.clone(),
            log: log.clone(),
        }
    }

    ///  use provided etcd server instance, while rppd use as library and re-used existing endpoints/binding port for either rppd and etcd
    #[cfg(feature = "etcd-provided")]
    pub(crate) async fn new(node: EtcdNode, c: &RppdConfig, log: &slog::Logger) -> Self {
        EtcdConnector {
            etcd: Ok(node),
            client_id: c.node.clone(),
            log: log.clone(),
        }
    }

    /// TODO implement etcd login capabilities to vanila implementation
    #[cfg(feature = "etcd-external")]
    pub(crate) async fn connect(cfg: &RppdConfig, log: &Logger) -> Self {
        let endpoint = std::env::var_os("ETCD_ENDPOINT")
            .map(|v| v.to_str().unwrap_or(DEFAULT_ENDPOINT).to_string())
            .unwrap_or(DEFAULT_ENDPOINT.to_string());

        let etcd = etcd_client::Client::connect([endpoint.as_str()], None).await
            .map_err(|e| format!("trying to connect to etcd v3 server [{}] (as configured in env 'ETCD_ENDPOINT' or default), but: {}", endpoint, e));

        EtcdConnector {
            etcd,
            client_id: cfg.node.clone(),
            log: log.clone(),
        }
    }

    /// Asknoledge the message receiving by deleting key from etcd queue.
    /// There is no queue in external i.e. no asknoledge required
    #[cfg(not(feature = "etcd-external"))]
    pub(crate) async fn aks(&self, key: &String) {
        if let Ok(etcd) = &self.etcd {
            if let Err(e) = etcd.delete_range(Request::new(DeleteRangeRequest{
                key: key.clone().into_bytes(), ..Default::default()
            })).await {
                error!(self.log, "Cleanup queue: [{}] {}", key, e);
            }
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

        let mut con = cluster.etcd.write().await;
        let log = con.log.clone();
        
        {
            let watchers = cluster.watchers.read().await;
            if let Some(name) =  new_name { // in case of update queue for a function
                if watchers.contains_key(&self.schema_table) && name !=& self.schema_table{
                    // delete queue_name
                    #[allow(unused_mut)]
                    if let Some(mut w) = cluster.watchers.write().await
                        .remove(&self.schema_table) {
                        if let Err(e) = w.cancel().await {
                            warn!(log, "can't clean watcher for {} Etcd reply: {}", &self.schema_table, e);
                        }
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
        let client_id = con.client_id.clone();
        let queue_consumer_name = format!("{}/consumer/{}", queue_name, &client_id.as_hyphenated());
        match &mut con.etcd {
            Ok(etcd) => {
                let sender = cluster.kv_sender.clone();

                #[cfg(not(feature = "etcd-external"))] {
                    let (request_sender, request_receiver) = channel(100);
                    let (response_sender, mut response_receiver) = channel(100);
                    etcd.new_watcher(request_receiver, response_sender.clone(), &client_id).await;

                    match request_sender.send(WatchRequest {
                        request_union: Some(watch_request::RequestUnion::CreateRequest(
                            WatchCreateRequest { key: queue_consumer_name.into_bytes(), ..Default::default() }
                        ))
                    }).await {
                        Ok(()) => {
                            // the first response is registration and should not have events
                            match response_receiver.recv().await {
                                Some(r) => {
                                    match r {
                                        Ok(watcher) => {
                                            if let Some(w) = cluster.watchers.write().await
                                                .insert(queue_name.clone(), WatcherW {
                                                    sender: request_sender,
                                                    watch_id: watcher.watch_id,
                                                }) { // just in case is something is there
                                                if let Err(e) = w.cancel().await {
                                                    warn!(log, "can't clean watcher for {} Etcd reply: {}", queue_name, e);
                                                }
                                            }

                                            tokio::spawn(async move {
                                                while let Some(resp) = response_receiver.recv().await {
                                                    match resp {
                                                        Ok(w) => {
                                                            for e in w.events {
                                                                if let Some(kv) = e.kv {
                                                                    if let Err(e) = sender.send(MessageRequest {
                                                                        key: String::from_utf8_lossy(&kv.key).to_string(),
                                                                        value: kv.value.to_vec(),
                                                                    }).await {
                                                                        error!(log, "can't consume watcher notivy for {} Etcd watcher failed: {}", queue_name, e);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!(log, "can't create watcher for {} Etcd watcher failed: {}", queue_name, e);
                                                        }
                                                    }
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            warn!(log, "can't register watcher for {} Etcd reply: {}", queue_name, e);
                                        }
                                    }
                                }
                                None => {
                                    warn!(log, "can't register watcher for {} Etcd no reply", queue_name);
                                }
                            }
                        }
                        Err(e) => {
                            error!(cluster.log, "can't create watcher for {} Etcd watcher failed: {}", queue_name, e);
                        }
                    }
                }

                #[cfg(feature = "etcd-external")]
                match etcd.watch(queue_consumer_name.into_bytes(), None).await {
                    Ok((watcher, mut stream)) => {
                        // register queue_name to watch_id
                        if let Some(mut w) = cluster.watchers.write().await
                            .insert(queue_name.clone(), watcher) { // just in case is something there
                            if let Err(e) = w.cancel().await {
                                warn!(log, "can't clean watcher for {} Etcd reply: {}", queue_name, e);
                            }
                        }
                        tokio::spawn(async move {
                            while let Some(resp) = stream.next().await {
                                match resp {
                                    Ok(v) => {
                                        for e in v.events() {
                                            if let Some(kv) = e.kv() {
                                                if let Err(e) = sender.send(MessageRequest {
                                                    key: kv.key_str().unwrap_or("").to_string(),
                                                    value: kv.value().to_vec(),
                                                }).await {
                                                    error!(log, "can't notify watcher for {} Etcd reply: {}", queue_name, e);
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!(log, "can't create watcher for {} Etcd reply: {}", queue_name, e);
                                    }
                                }
                            }
                        });
                    },
                    Err(e) => {
                        error!(log, "can't create watcher for {} Etcd watcher failed: {}", queue_name, e);
                    }
                }
            }
            Err(e) => {
                error!(cluster.log, "can't create watcher for [{}] Etcd is not ready: {}", queue_name, e);
            }
        }
    }

    #[cfg(not(feature = "etcd-external"))]
    pub(crate) fn is_etcd_queue(&self) -> bool {
        self.schema_table.starts_with("/q/")
        || self.schema_table.starts_with("/queue/")
    }
}

#[cfg(not(feature = "etcd-external"))]
impl WatcherW {
    async fn cancel(&self) -> Result<(), SendError<WatchRequest>> {
        self.sender.send(
            WatchRequest{ request_union: Some(
                watch_request::RequestUnion::CancelRequest(
                    WatchCancelRequest { watch_id: self.watch_id}
                ))
            }).await
    }
}