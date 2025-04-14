#![allow(unused_variables, dead_code)]

use std::sync::atomic::Ordering;

use async_trait::async_trait;
use slog::info;
use tonic::{Request, Response, Status};
use rppd_common::protogen::rppc::{DbAction, DbEventRequest, DbEventResponse, PkColumn, PkColumnType, StatusRequest, StatusResponse};
use rppd_common::protogen::rppc::pk_column::PkValue;
use rppd_common::protogen::rppd::rppd_node_server::RppdNode;
use rppd_common::protogen::rppd::{MessageRequest, MessageResponse, SwitchRequest, SwitchResponse};
use rppd_common::protogen::rppg::rppd_trigger_server::RppdTrigger;
use crate::arg_config::{CFG_CRON_TABLE, CFG_FN_TABLE, CFG_TABLE};
use crate::rd_config::RppdNodeCluster;
use crate::rd_fn::RpFnLog;

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
        let fns = self.check_fn(&request.key).await;
        if fns.len() > 0 {
            if let ScheduleResult::Some(fn_logs) = self.prepare(fns, &vec![
                PkColumn {
                    column_name: "KEY".to_string(),
                    column_type: PkColumnType::String as i32,
                    pk_value: Some(PkValue::StringValue(request.key.clone())),
                }
            ], DbAction::Dual, &Some(request.value)).await.map_err(|e| Status::internal(e))? {
                for fn_log in fn_logs {
                    let _ = self.queueing(fn_log, false).await; // build a queue
                }
            }
        }
        Ok(Response::new(MessageResponse { }))
    }

    async fn event(&self, request: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        self.event_impl(request).await
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
        self.event_impl(request).await
    }
    async fn status(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        self.status_impl(request).await
    }
}

impl RppdNodeCluster {

    async fn event_impl(&self, request: Request<DbEventRequest>) -> Result<Response<DbEventResponse>, Status> {
        let request = request.into_inner();
        if request.optional_caller.is_some() && self.master.load(Ordering::Relaxed) {
            return Ok(Response::new(DbEventResponse { saved: false, repeat_with: vec![] })); // no reverse call from host to master
        }

        if !self.started.load(Ordering::Relaxed) {
            // not started
            return if request.table_name.starts_with(&self.cfg.schema) && request.table_name.ends_with(CFG_TABLE) {
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
            match self.prepare(fns, &request.pks, request.event_type(), &None).await.map_err(|e| Status::internal(e))? {
                ScheduleResult::Some(fn_logs) => {
                    for fn_log in fn_logs {
                        let _ = self.queueing(fn_log, false).await; // build a queue
                    }
                }
                ScheduleResult::Repeat(repeat_with) => {
                    info!(self.log, "re-requesting to repeat: {:?}", repeat_with);
                    return Ok(Response::new(DbEventResponse { saved: false, repeat_with }));
                }
            }

            if request.table_name.starts_with(&self.cfg.schema) {
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
                        Ok(n) => n.lock().await.status(tonic::Request::new(r)).await,
                        Err(e) => Err(Status::unavailable(format!("on call: {}", e)))
                    }
                } else {
                    Err(Status::unavailable(format!("no such node: {}", r.node_id)))
                }
            }
        }
    }

}


