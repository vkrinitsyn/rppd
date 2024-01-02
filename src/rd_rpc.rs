use std::sync::atomic::Ordering;
use async_trait::async_trait;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};
use tonic::transport::Channel;
use crate::arg_config::{CFG_CRON_TABLE, CFG_FN_TABLE, CFG_TABLE};
use crate::cron::RpFnCron;
use crate::gen::rg::{EventRequest, EventResponse, FnAction, PkColumn, StatusRequest, StatusResponse};
use crate::gen::rg::grpc_client::GrpcClient;
use crate::gen::rg::grpc_server::Grpc;
use crate::gen::rg::status_request::FnLog;
use crate::rd_config::Cluster;
use crate::rd_fn::RpFnLog;


pub(crate) enum ScheduleResult {
    // no need schedule
    None,
    // Ok
    Some(Vec<RpFnLog>),
    // repeat a call with column name
    Repeat(Vec<PkColumn>)
}

#[async_trait]
impl Grpc for Cluster {

    async fn event(&self, request: Request<EventRequest>) -> Result<Response<EventResponse>, Status> {
        let request = request.into_inner();
        if request.optional_caller.is_some() && self.master.load(Ordering::Relaxed) {
            return Ok(Response::new(EventResponse{ saved: false, repeat_with: vec![] })); // no reverse call from host to master
        }

        if !self.started.load(Ordering::Relaxed) {
            // not started
            return if request.table_name.starts_with(&self.cfg.schema) && request.table_name.ends_with(CFG_TABLE) {
                Ok(Response::new(EventResponse { saved: true, repeat_with: vec![] })) // init stage to set master
            } else {
                if request.optional_caller.is_some() {
                    Ok(Response::new(EventResponse { saved: false, repeat_with: vec![] })) // tell master, node is not ready
                } else {
                    Err(Status::internal("not a master"))
                }
            };
        }

        match self.prepare(&request).await.map_err(|e| Status::internal(e))? {
            ScheduleResult::None => {
                // no function for a trigger
            }
            ScheduleResult::Some(fn_logs) => {
                for fn_log in fn_logs {
                    let _ = self.queueing(fn_log, false).await; // build a queue
                }
            }
            ScheduleResult::Repeat(repeat_with) => {
                println!("re-requesting to repeat: {:?}", repeat_with);
                return Ok(Response::new(EventResponse{ saved: false, repeat_with }));
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
                // self.reload_crons(&Some(request)).await
                RpFnCron::load(&self.cfg.schema, self.db(), &mut *self.cron.write().await).await
            } else { // if request.table_name.ends_with(CFG_FNL_TABLE) {
                // no trigger should be there
                Ok(())
            } {
                return Err(Status::internal(e));
            }
        }

        Ok(Response::new(EventResponse{ saved: true,  repeat_with: vec![] }))
    }

    async fn status(&self, request: Request<StatusRequest>) -> Result<Response<StatusResponse>, Status> {
        let r = request.into_inner();
        if !self.started.load(Ordering::Relaxed) {
            Err(Status::unavailable("starting"))
        } else {

            if r.node_id == self.node_id.load(Ordering::Relaxed) {
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
                    is_master
                } ))

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


