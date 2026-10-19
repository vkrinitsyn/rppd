//! [db-tag] One rppd per database behind one port (yt-pgxt combined-config.md D11):
//! the embedder mounts this router instead of an instance's own services. A call
//! goes to the instance its `rppd-db` names - what a tagged instance sends its peers
//! (`RppdNodeCluster::peer`) - and an untagged call to the default instance, so a
//! peer that tags nothing reaches what it always did.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;
use tonic::transport::server::Router;
use tonic::{Request, Response, Status};
use rppd_common::protogen::rppc::{DbEventRequest, DbEventResponse, StatusRequest, StatusResponse};
use rppd_common::protogen::rppd::rppd_node_server::{RppdNode, RppdNodeServer};
use rppd_common::protogen::rppd::{MessageRequest, MessageResponse, SwitchRequest, SwitchResponse};
use rppd_common::protogen::rppg::rppd_trigger_server::{RppdTrigger, RppdTriggerServer};

use crate::rd_config::RppdNodeCluster;

/// The header a tagged instance's calls carry: its database
pub const DB_HEADER: &str = "rppd-db";

#[derive(Clone)]
pub struct RppdRouter {
    default: RppdNodeCluster,
    tagged: Arc<RwLock<BTreeMap<String, RppdNodeCluster>>>,
}

impl RppdRouter {
    /// `tagged`: the instances by database, shared with the embedder, which adds and
    /// stops them
    pub fn new(default: RppdNodeCluster, tagged: Arc<RwLock<BTreeMap<String, RppdNodeCluster>>>) -> Self {
        RppdRouter { default, tagged }
    }

    pub fn add_all_services(&self, srv: Router) -> Router {
        srv.add_service(RppdNodeServer::new(self.clone()))
            .add_service(RppdTriggerServer::new(self.clone()))
    }

    /// The instance a call is for: the one its header names, else the default
    async fn pick<T>(&self, r: &Request<T>) -> Result<RppdNodeCluster, Status> {
        match r.metadata().get(DB_HEADER).and_then(|v| v.to_str().ok()).map(str::trim).filter(|v| !v.is_empty()) {
            None => Ok(self.default.clone()),
            Some(db) => self.tagged.read().await.get(db).cloned()
                .ok_or_else(|| Status::not_found(format!("no rppd instance for database {} here", db))),
        }
    }
}

#[async_trait]
impl RppdNode for RppdRouter {
    async fn message(&self, r: Request<MessageRequest>) -> Result<Response<MessageResponse>, Status> {
        RppdNode::message(&self.pick(&r).await?, r).await
    }
    async fn event(&self, r: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        RppdNode::event(&self.pick(&r).await?, r).await
    }
    async fn complete(&self, r: Request<StatusRequest>) -> Result<Response<SwitchResponse>, Status> {
        RppdNode::complete(&self.pick(&r).await?, r).await
    }
    async fn switch(&self, r: Request<SwitchRequest>) -> Result<Response<SwitchResponse>, Status> {
        RppdNode::switch(&self.pick(&r).await?, r).await
    }
    async fn status(&self, r: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        RppdNode::status(&self.pick(&r).await?, r).await
    }
}

#[async_trait]
impl RppdTrigger for RppdRouter {
    async fn event(&self, r: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        RppdTrigger::event(&self.pick(&r).await?, r).await
    }
    async fn status(&self, r: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        RppdTrigger::status(&self.pick(&r).await?, r).await
    }
}
