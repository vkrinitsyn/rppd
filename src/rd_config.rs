/*
RPPD - Rust Python Postgres Discovery

This file is part of RPPD.

RPPD is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version OR the.

RPPD is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with RPPD.  If not, see <http://www.gnu.org/licenses/>.

The portion above shall not be modified.
*/

/*
CREATE table if not exists @extschema@.rppd_config (
id serial primary key,
host varchar(64) not null,
active_since timestamp,
master bool uniq
);
*/


use std::collections::{BTreeMap, HashMap, VecDeque};
use sqlx::{Database, PgPool, Pool, Postgres};
use crate::gen::rg::grpc_client::GrpcClient;
use crate::gen::rg::*;

use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;use rust_embed::RustEmbed;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use pgrx::PgTriggerError;
use tonic::{Request, Response, Status};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::transport::{Channel, Server};

use crate::gen::rg::grpc_server::{Grpc, GrpcServer};

use tokio::sync::{mpsc, Mutex, RwLock};
use tonic::codegen::Body;
use crate::arg_config::*;
use crate::gen::rg::*;
use crate::gen::rg::event_request::PkValue;
use crate::gen::rg::event_response::EventResponse::RepeatWith;
use crate::rd_fn::*;

const SELECT: &str = "select id, host, alias, active_since, master from %SCHEMA%.rppd_config where active_since is not null";

/// Rust Python Host
///
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct RpHost {
  pub(crate) id: i32,
  pub(crate) host: String,
  pub(crate) alias: String,
  pub(crate) active_since: chrono::DateTime<chrono::Utc>,
  pub(crate) master: Option<bool>
}

impl RpHost {
  pub(crate) fn select(schema:&String) -> String {
    SELECT.replace("%SCHEMA%", schema.as_str())
  }

  pub(crate) async fn connect(&self) -> Result<Mutex<GrpcClient<Channel>>, String> {
    unimplemented!();

    Err("na".into())
  }

  /// try to self register as master if needed, return node_id
  pub(crate) async fn insert(master: bool, c: &ArgConfig, pool: Pool<Postgres>) -> i32 {
    unimplemented!();
  }

}


/// on column @extschema@.rppd_function.topic
/// is '"": queue per table; => schema.table
/// ".{column_name}": queue per value of the column in this table; => schema.table.column
/// "{any_name}": global queue'; => ..name

pub(crate) type Topic = String;


#[derive(Clone)]
pub(crate) struct Cluster {
  /// parsed configs
  cfg: ArgConfig,
  /// is the instance play master, receive trigger calls, schedule and call others
  master: Arc<AtomicBool>,
  /// is the node loaded all logs
  started: Arc<AtomicBool>,
  node_id: Arc<AtomicI32>,

  /// postgres connection pool use by sqlx on init
  db: Pool<Postgres>,

  /// to nodes, mostly use by master
  nodes: Arc<RwLock<BTreeMap<i32, RpHost>>>,
  /// connections to nodes
  node_connections: Arc<RwLock<BTreeMap<i32, Result<Mutex<GrpcClient<Channel>>, String>>>>,

  /// connections to nodes, mostly use by master
  /// host to node_id
  node_ids: Arc<RwLock<HashMap<String, i32>>>,

  /// common queue to trigger bg dispatcher
  sender: Sender<RpFnLog>,
  /// log.id might be unsaved
  queue: Arc<RwLock<VecDeque<RpFnLog>>>,

  /// Python functions execution by fn id
  /// internal fn topics and queues
  fns: Arc<RwLock<BTreeMap<i32, RpFnCtx>>>,

  // TODO: multiple topics to single function copy
  // topics_value: Arc<RwLock<BTreeMap<Topic, i32>>>,

  /// tablename -> topic
  fn_tt: Arc<RwLock<HashMap<String, Topic>>>,

  /// topic -> function id
  fn_id: Arc<RwLock<HashMap<Topic, i32>>>,

  // Python connection pool to postgres and python context to run by functions topic
  // trigger -> tablename -> function def -> _topic_ -> function call
  // pys: Arc<RwLock<BTreeMap<i32, PyFnCtx>>>,
}

impl Cluster {
  pub(crate) fn db(&self) -> Pool<Postgres> {
    self.db.clone()
  }

  /// init
  pub(crate) async fn init(c: ArgConfig) -> Result<Self, String> {
    let pool = PgPoolOptions::new()
        .max_connections(1)  // use only on init
        .connect(c.db_url().as_str()).await
        .map_err(|e| e.to_string())?;
    // getting cluster
    let mut nodes = BTreeMap::new();
    let mut node_connections = BTreeMap::new();
    let mut node_id = HashMap::new();

    let this = format!("{}:{}", c.bind, c.port);
    let r = sqlx::query_as::<_, RpHost>(RpHost::select(&c.schema).as_str())
        .fetch_all(&pool).await
        .map_err(|e| e.to_string())?;
    let mut master = false; // master is present and up
    let mut found_self = false; // is self registered
    let r_len = r.len();
    let mut id = 0;
    for n in r {
      if n.host == this {
        found_self = true;
        id = n.id;
      } else {
        let node = n.connect().await;
        master = master || n.master.unwrap_or(false) && node.is_ok();
        node_connections.insert(n.id,  node);
        node_id.insert(n.host.clone(), n.id);
        nodes.insert(n.id, n);
      }
    }

    let (sender, mut rsvr) = mpsc::channel(c.max_queue_size);

    let cluster = Cluster {
      cfg: c,
      master: Arc::new(AtomicBool::new(false)),
      started: Arc::new(AtomicBool::new(false)),
      node_id: Arc::new(AtomicI32::new(id)),
      db: pool,
      nodes: Arc::new(RwLock::new(nodes)),
      node_connections: Arc::new(RwLock::new(node_connections)),
      node_ids: Arc::new(RwLock::new(node_id)),
      sender,
      queue: Arc::new(Default::default()),
      fns: Arc::new(Default::default()),
      fn_tt: Arc::new(Default::default()),
      fn_id: Arc::new(Default::default()),
      // pys: Arc::new(Default::default()),
    };

    cluster.run(rsvr, r_len == 0 || !master  || !found_self, !master).await;

    Ok(cluster)
  }

  /// prepare to run code on startup and background
  async fn run(&self, mut rsvr: Receiver<RpFnLog>, self_register: bool, self_master: bool) {
    let master = self.master.clone();
    let started = self.started.clone();
    let c = self.cfg.clone();
    let pool = self.db();
    let ctx = self.clone();
    // let ctx = RpFnBgCtx {
    //   fns: self.fns.clone(),
    //   pys: self.pys.clone(),
    //   db: self.db(),
    // };

    tokio::spawn(async move { // will execute as background, but will try to connect to this host
      RpHost::insert(self_master, &c, pool).await;

      if self_master {
        master.store(true, Ordering::Relaxed);
      }
      // load functions
      let _ = ctx.load().await;
      // load messages to internal queue from fn_log, check timeout and execution status,

      // set STARTED
      started.store(true, Ordering::Relaxed);
      loop {
        match rsvr.recv().await {
          None => {
            break
          },
          Some(ci) => {
            if let Err(e) = ctx.execute(ci).await {
              // todo store log if present
              eprintln!("{}", e);
            }
          }
        }
      }

    });
  }

  // async fn find(&self, table_name: &String) -> Result<RpFn, String> {
  //   let topic = self.fn_tt.read().await.get(table_name).unwrap_or(&"".into());
  //   let f_id = self.fn_id.read().await.get(topic).ok_or(format!("No function for table: {}", table_name))?;
  //   self.fns.read().await.get(f_id).ok_or(format!("No function for table: {}", table_name))
  // }

  /// find topic, fnb_id and node_id by table
  /// synchroniously on client DB transaction store log, if configured
  async fn schedule(&self, r: &EventRequest) -> Result<ScheduleResult, String> {
    let f_id = match self.fn_tt.read().await.get(&r.table_name) {
      None => {return Ok(ScheduleResult::None);}
      Some(topic) => {
        match self.fn_id.read().await.get(topic) {
          None => {return Ok(ScheduleResult::None);}
          Some(t) => *t
        }
      }
    };
    let (fl,recovery_logs, is_dot) =  match self.fns.read().await.get(&f_id) {
      None => {
        return Ok(ScheduleResult::None);
      },
      Some(f) => {
        if !r.id_value && r.pk_value.is_none() &&f.fns.is_dot() {
          return Ok(f.fns.to_repeat_result());
        }

        (RpFnLog {
          id: 0,
          node_id: 0, // TODO this host or another node
          fn_id: f.fns.id,
          took_sec: 0,
          trig_type: r.event_type,
          trig_value: r.pk_value.as_ref().map(|e| match e {
            PkValue::IntValue(v) => *v as i64,
            PkValue::BigintValue(v) => *v,
          }) ,
          finished_at: None,
          output_msg: None,
          error_msg: None,
         }, f.fns.recovery_logs, f.fns.is_dot()
        )}
      };

      if !recovery_logs {
         return Ok(ScheduleResult::Some(fl));
      }
      let r = if is_dot {
        sqlx::query_scalar::<_, i64>(RpFnLog::insert_v(&self.cfg.schema).as_str())
            .bind(fl.node_id)
            .bind(fl.fn_id)
            .bind(fl.trig_value.unwrap_or(0))
            .bind(fl.trig_type)
            .fetch_one(&self.db()).await.map_err(|e| e.to_string())?
      } else {
        sqlx::query_scalar::<_, i64>(RpFnLog::insert(&self.cfg.schema).as_str())
            .bind(fl.node_id)
            .bind(fl.fn_id)
            .bind(fl.trig_type)
            .fetch_one(&self.db()).await.map_err(|e| e.to_string())?
      };

    Ok(ScheduleResult::Some(RpFnLog{id: r, ..fl}))
  }


  pub(crate) async fn load(&self) -> Result<(), String> {

    Ok(())
  }


  /// primary loop of event execution
  pub(crate) async fn execute(&self, f: RpFnLog) -> Result<(), String> {

    match self.fns.read().await.get(&f.fn_id) {
      None => Err(format!("no function by id: {}", f.fn_id)),
      Some(fc) => {
        // TODO store to reuse
        let p = PyContext::new(&fc.fns, &self.cfg.db_url()).map_err(|e| e.to_string())?;

        p.invoke(&f, fc)
      }
    }

  }
}


pub(crate) enum ScheduleResult {
  // no need schedule
  None,
  // Ok
  Some(RpFnLog),
  // repeat a call with column name
  Repeat(PkColumn)
}

#[async_trait]
impl Grpc for Cluster {


  async fn event(&self, request: Request<EventRequest>) -> Result<Response<EventResponse>, Status> {
    let request = request.into_inner();
    if request.optional_caller.is_some() && self.master.load(Ordering::Relaxed) {
      return Ok(Response::new(EventResponse{ event_response: None })); // no reverse call from host to master
    }

    if !self.started.load(Ordering::Relaxed) {
      // not started
      return if request.table_name.starts_with(&self.cfg.schema)
      && request.table_name.ends_with(CFG_FN_TABLE) {
        Ok(Response::new(EventResponse { event_response: Some(event_response::EventResponse::Saved(true)) })) // init stage to set master
      } else {
        if request.optional_caller.is_some() {
          Ok(Response::new(EventResponse { event_response: None })) // tell master, node is not ready
        } else {
          Err(Status::internal("not a master"))
        }
      };
    }

    match self.schedule(&request).await.map_err(|e| Status::internal(e))? {
      ScheduleResult::None => {}
      ScheduleResult::Some(fn_log) => {
        let _ = self.sender.send(fn_log).await;
      }
      ScheduleResult::Repeat(pk) => {
        return Ok(Response::new(EventResponse{ event_response: Some(RepeatWith(pk)) }));
      }
    }

    if request.table_name.starts_with(&self.cfg.schema) {
      if request.optional_caller.is_none() {
        // TODO broadcast to cluster, set optional_caller to this as master
      }
      if request.table_name.ends_with(CFG_TABLE) {
        // TODO register a node or self register, includes set as master

      } else if request.table_name.ends_with(CFG_FN_TABLE) {
        // TODO update functions

      } else if request.table_name.ends_with(CFG_QUEUE_TABLE) {
        // TODO update queues

      } else if request.table_name.ends_with(CFG_FNL_TABLE) {
        // no trigger should be there

      }
    }

    Ok(Response::new(EventResponse{ event_response: Some(event_response::EventResponse::Saved(true))  }))
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
