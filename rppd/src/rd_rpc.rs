#![allow(unused_variables, dead_code)]

use std::sync::atomic::Ordering;

use async_trait::async_trait;
use etcds::queue::QueueNameKey;
use slog::{debug, info, trace};
use tonic::{Request, Response, Status};
use rppd_common::CFG_TABLE;
use rppd_common::protogen::rppc::{DbAction, DbEventRequest, DbEventResponse, PkColumn, PkColumnType, StatusRequest, StatusResponse};
use rppd_common::protogen::rppc::pk_column::PkValue;
use rppd_common::protogen::rppd::rppd_node_server::RppdNode;
use rppd_common::protogen::rppd::{MessageRequest, MessageResponse, SwitchRequest, SwitchResponse};
use rppd_common::protogen::rppg::rppd_trigger_server::RppdTrigger;
use crate::arg_config::{CFG_CRON_TABLE, CFG_FN_TABLE};
#[cfg(feature = "lib-embedded")]
use crate::arg_config::CFG_FNL_TABLE;
use crate::rd_config::RppdNodeCluster;
use crate::rd_fn::RpFnLog;
use crate::LP;
#[cfg(feature = "lib-embedded")]
use slog::warn;
#[cfg(feature = "lib-embedded")]
use rppd_common::protogen::rppc::db_event_request;

pub(crate) enum ScheduleResult {
    // fn not found, no need schedule 
    // None,
    /// Ok
    Some(Vec<RpFnLog>),
    /// repeat a call from DB with column's data taken by name
    Repeat(Vec<PkColumn>),
}

#[async_trait]
impl RppdNode for RppdNodeCluster {
    async fn message(&self, request: Request<MessageRequest>) -> Result<Response<MessageResponse>, Status> {
        let request = request.into_inner();
        // [db-tag] the queue as this instance's functions name it; KEY keeps the key
        // the message has, which its acknowledgement deletes
        let qn = QueueNameKey::new(self.cfg.read().await.unqkey(&request.key));

        trace!(self.log, "get notify on {}", &request.key);
        // [cadence] the pp watching this queue saw a message
        self.cadence_seen(qn.name().as_str()).await;

        let fns = self.check_fn(&qn.name()).await;
        if fns.len() > 0 {
            if let ScheduleResult::Some(fn_logs) = self.prepare(fns, &vec![
                PkColumn {
                    column_name: "KEY".to_string(),
                    column_type: PkColumnType::String as i32,
                    pk_value: Some(PkValue::StringValue(request.key.clone())),
                }
            ], DbAction::Dual, &Some((request.key, request.value)), &request.traceparent).await.map_err(|e| Status::internal(e))? {
                for fn_log in fn_logs {
                    let _ = self.queueing(fn_log, false).await; // build a queue
                }
            }
        } else {
            debug!(self.log, "{}no consumer for {}", LP, &request.key);
        }

        Ok(Response::new(MessageResponse { }))
    }

    async fn event(&self, request: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        self.event_impl(request.into_inner()).await
    }

    ///
    async fn complete(&self, request: Request<StatusRequest>) -> Result<Response<SwitchResponse>, Status> {
        let _r = request.into_inner();
        // TODO use r.fn_log
        self.sender.send(None).await
            .map(|_| Response::new(SwitchResponse {}))
            .map_err(|e| Status::internal(e.to_string()))
    }

    async fn switch(&self, request: Request<SwitchRequest>) -> Result<Response<SwitchResponse>, Status> {
        if !self.master.load(Ordering::Relaxed) {
            let ctx = self.clone();
            let master = request.into_inner();

            #[cfg(not(feature = "lib-embedded"))]
            tokio::spawn(async move {
                ctx.become_master(Some(master.node_id)).await;
            });
        }
        Ok(Response::new(SwitchResponse {}))
    }

    async fn status(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        self.status_impl(request).await
    }
}

#[async_trait]
impl RppdTrigger for RppdNodeCluster {
    async fn event(&self, request: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        self.event_impl(request.into_inner()).await
    }
    async fn status(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        self.status_impl(request).await
    }
}

impl RppdNodeCluster {

    pub(crate) async fn event_impl(&self, request: DbEventRequest) -> Result<Response<DbEventResponse>, Status> {
        let schema = self.cfg.read().await.schema.clone();
        // [embedded] A forwarded event (below) reaches the master WITH a caller:
        // it is the one call a host makes to the master, so the master takes it.
        #[cfg(not(feature = "lib-embedded"))]
        if request.optional_caller.is_some() && self.master.load(Ordering::Relaxed) {
            return Ok(Response::new(DbEventResponse { saved: false, repeat_with: vec![] })); // no reverse call from host to master
        }
        // [embedded] The embedder (ytserv) hands every commit's event to the rppd
        // of the node that committed it, which then ran the function itself - so
        // functions ran wherever a client happened to write, not on the master
        // the cluster elected (topology 25: the marker row on a follower). A host
        // passes a fresh event for a users table on to the master; only when no
        // master is known or reachable does it still run it here.
        #[cfg(feature = "lib-embedded")]
        if request.optional_caller.is_none() && !self.master.load(Ordering::Relaxed)
            && !self.is_own_table(&schema, &request.table_name) {
            if let Some(r) = self.forward_to_master(&schema, &request).await {
                return r;
            }
        }

        if !self.started.load(Ordering::Relaxed) {
            // not started
            return if request.table_name.starts_with(&schema) && request.table_name.ends_with(CFG_TABLE) {
                Ok(Response::new(DbEventResponse { saved: true, repeat_with: vec![] })) // init stage to set master
            } else {
                if request.optional_caller.is_some() {
                    Ok(Response::new(DbEventResponse { saved: false, repeat_with: vec![] })) // tell master, node is not ready
                } else {
                    Err(Status::internal("not a master"))
                }
            };
        }
        
        let fns = self.check_fn(&request.table_name).await;
        if fns.len() > 0 {
            match self.prepare(fns, &request.pks, request.event_type(), &None, &request.traceparent).await.map_err(|e| Status::internal(e))? {
                ScheduleResult::Some(fn_logs) => {
                    for fn_log in fn_logs {
                        let _ = self.queueing(fn_log, false).await; // build a queue
                    }
                }
                ScheduleResult::Repeat(repeat_with) => {
                    info!(self.log, "{}re-requesting to repeat: {:?}", LP, repeat_with);
                    return Ok(Response::new(DbEventResponse { saved: false, repeat_with }));
                }
            }

            if request.table_name.starts_with(&schema) {
                if request.optional_caller.is_none() {
                    // TODO broadcast to cluster, set optional_caller to this as master
                    
                }

                if let Err(e) = if request.table_name.ends_with(CFG_TABLE) {
                    self.reload_hosts(&request).await
                } else if request.table_name.ends_with(CFG_FN_TABLE) {
                    self.reload_fs(&Some(request)).await
                } else if request.table_name.ends_with(CFG_CRON_TABLE) {
                    self.reload_cron(&Some(request)).await
                } else { // if request.table_name.ends_with(CFG_FNL_TABLE) {
                    // no trigger should be there
                    Ok(())
                } {
                    return Err(Status::internal(e));
                }
            }
        }

        Ok(Response::new(DbEventResponse { saved: true, repeat_with: vec![] }))
    }

    /// one of rppd's own config tables (roster, functions, cron, log)
    #[cfg(feature = "lib-embedded")]
    fn is_own_table(&self, schema: &str, table_name: &str) -> bool {
        table_name.starts_with(schema) && [CFG_TABLE, CFG_FN_TABLE, CFG_CRON_TABLE, CFG_FNL_TABLE]
            .iter().any(|t| table_name.ends_with(t))
    }

    /// [embedded] Hand an event to the master the roster names. `None`: this node
    /// is that master, none is named, or it cannot be reached - run it here.
    #[cfg(feature = "lib-embedded")]
    async fn forward_to_master(&self, schema: &str, request: &DbEventRequest)
        -> Option<Result<Response<DbEventResponse>, Status>> {
        let table = self.cfg.read().await.table.clone();
        let sql = format!("select id, host from {}.{} where master", schema, table);
        let (master, host) = match sqlx::query_as::<_, (i32, String)>(sqlx::AssertSqlSafe(sql.as_str())).fetch_optional(&self.db()).await {
            Ok(Some(row)) => row,
            Ok(None) => return None,
            Err(e) => { warn!(self.log, "{}forward to master: {}", LP, e); return None; }
        };
        let me = self.node_id.load(Ordering::Relaxed);
        if master == me {
            return None;
        }
        // The embedder rewrites the roster's rows as nodes move (a restart hands
        // a container a new address, and its row a new id), so the master's id is
        // often one this node never connected to: connect to its host now, and
        // keep the connection under that id.
        let connected = matches!(self.node_connections.read().await.get(&master), Some(Ok(_)));
        if !connected {
            let h = crate::rd_config::RpHost { id: master, host: host.clone(), host_name: String::new(),
                active_since: None, master: Some(true), max_db_connections: 0 };
            match h.connect().await {
                Ok(c) => { self.node_connections.write().await.insert(master, Ok(c)); }
                Err(e) => { warn!(self.log, "{}forward to master {} at {}: {}, running here", LP, master, host, e); return None; }
            }
        }
        let forwarded = DbEventRequest {
            optional_caller: Some(db_event_request::OptionalCaller::CallBy(me)),
            ..request.clone()
        };
        let r = match self.node_connections.read().await.get(&master) {
            Some(Ok(node)) => node.lock().await.event(self.peer(forwarded).await).await,
            _ => { warn!(self.log, "{}forward to master {}: no connection, running here", LP, master); return None; }
        };
        match r {
            Ok(resp) => Some(Ok(resp)),
            Err(e) => { warn!(self.log, "{}forward to master {}: {}, running here", LP, master, e); None }
        }
    }

    async fn status_impl(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        let r = request.into_inner();
        if !self.started.load(Ordering::Relaxed) {
            Err(Status::unavailable("starting"))
        } else {
            if r.node_id == self.node_id.load(Ordering::Relaxed)
                || (r.node_id < 0 && !self.node_connections.read().await.contains_key(&r.node_id))
            {
                let pool = self.exec.read().await.len() as i32;
                let is_master = self.master.load(Ordering::Relaxed);
                let queue = self.queue.read().await;
                let (queued, in_proc) = queue.size();
                Ok(Response::new(StatusResponse {
                    node_id: r.node_id,
                    queued: queued as i32,
                    in_proc: in_proc as i32,
                    fn_log: queue.status(r.fn_log).await,
                    pool,
                    is_master,
                }))
            } else { // do call to the node
                if let Some(node) = self.node_connections.read().await.get(&r.node_id) {
                    match node {
                        Ok(n) => n.lock().await.status(self.peer(r).await).await,
                        Err(e) => Err(Status::unavailable(format!("on call: {}", e)))
                    }
                } else {
                    Err(Status::unavailable(format!("no such node: {}", r.node_id)))
                }
            }
        }
    }

}


