use std::sync::atomic::Ordering;
use async_trait::async_trait;
use tonic::{Request, Response, Status};
use crate::arg_config::{CFG_CRON_TABLE, CFG_FN_TABLE, CFG_TABLE};
use crate::gen::rg::{EventRequest, EventResponse, PkColumn, StatusRequest, StatusResponse};
use crate::gen::rg::grpc_server::Grpc;
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

        // println!(">>started:{}, cfgtable: {}: {}", self.started.load(Ordering::Relaxed),
        //          request.table_name,
        //          request.table_name.starts_with(&self.cfg.schema) && request.table_name.ends_with(CFG_TABLE));

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
                self.reload_crons(&Some(request)).await
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
        let _request = request.into_inner();
        if !self.started.load(Ordering::Relaxed) {
            Err(Status::unavailable("starting"))
        } else if false // TODO inconsistent master
        {
            Err(Status::data_loss("inconsistent master"))
        } else {
            Ok(Response::new(StatusResponse { master: 0, status: 0, queued: 0, in_proc: 0 }))
        }
    }
}


