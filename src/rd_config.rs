/*%LPH%*/


use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use sqlx::{Database, PgPool, Pool, Postgres};
use crate::gen::rg::grpc_client::GrpcClient;
use crate::gen::rg::*;

use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;use rust_embed::RustEmbed;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::Instant;
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
use crate::gen::rg::pk_column::PkValue;
use crate::gen::rg::pk_column::PkValue::{BigintValue, IntValue};
use crate::rd_fn::*;

const SELECT: &str = "select id, host, host_name, active_since, master, max_db_connections from %SCHEMA%.rppd_config where ()";
const INSERT_HOST: &str = "insert into %SCHEMA%.rppd_config (host, host_name) values ($1, $2) returning id";
const UP_MASTER: &str = "update %SCHEMA%.rppd_config set master = true where id = $1";

/// Rust Python Host
///
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct RpHost {
  pub(crate) id: i32,
  pub(crate) host: String,
  pub(crate) host_name: String,
  pub(crate) active_since:  Option<chrono::DateTime<chrono::Utc>>,
  pub(crate) master: Option<bool>,
  pub(crate) max_db_connections: i32,
}

impl RpHost {
  pub(crate) fn select(schema: &String, condition: &str) -> String {
    format!("{} {}", SELECT.replace("%SCHEMA%", schema.as_str()), condition)
  }

  pub(crate) async fn connect(&self) -> Result<Mutex<GrpcClient<Channel>>, String> {
    unimplemented!();

    Err("na".into())
  }

}


/// on column @extschema@.rppd_function.topic
/// is '"": queue per table; => schema.table
/// ".{column_name}": queue per value of the column in this table; => schema.table.column
/// "{any_name}": global queue'; => ..name

pub(crate) type TopicType = String;


/// String of schema.table
pub(crate) type SchemaTableType = String;

/// String of host:port
pub(crate) type HostType = String;


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
  node_ids: Arc<RwLock<HashMap<HostType, i32>>>,

  /// common queue to trigger bg dispatcher
  sender: Sender<RpFnLog>,
  /// log.id might be unsaved
  queue: Arc<RwLock<VecDeque<RpFnLog>>>,

  /// Python functions execution by fn id
  /// internal fn topics and queues
  fns: Arc<RwLock<BTreeMap<i32, RpFnCtx>>>,

  /// tablename -> topic
  pub(crate) fn_tt: Arc<RwLock<HashMap<SchemaTableType, HashSet<TopicType>>>>,

  /// topic -> function id
  pub(crate) fn_id: Arc<RwLock<HashMap<TopicType, BTreeSet<i32>>>>,

  /// todo will dispose when unused
  /// max is max_concurrent
  pub(crate) exec: Arc<RwLock<VecDeque<Mutex<PyContext>>>>,
  
  pub(crate) cron: Arc<RwLock<Vec<RpFnCron>>>,
}
#[inline]
fn make_sql_cnd(input: &Vec<PkColumn>) -> String {
  let mut sql = String::new();
  for idx in 0..input.len() {
    if idx > 0 { sql.push_str(" and "); }
    sql.push_str(format!("{} = ${}", input[idx].column_name, idx).as_str());
  }
  sql
}

impl Cluster {
  pub(crate) fn db(&self) -> Pool<Postgres> {
    self.db.clone()
  }


  #[inline]
  async fn reload_hosts(&self, x: &EventRequest) -> Result<(), String> {
    let id = self.node_id.load(Ordering::Relaxed);
    let sql = RpHost::select(&self.cfg.schema, "active_since is not null");
    let sql = if x.pks.len() == 0 { sql } else { format!("{} and {}", sql, make_sql_cnd(&x.pks))};
    let mut r = sqlx::query_as::<_, RpHost>(sql.as_str());
    // PK ID set for Hosts is redundant, because config table has only one int PK, but use of common struct dictate to perform this way
    for x in &x.pks {
      if let Some(x) = &x.pk_value {
        match x {
          IntValue(x) => { r = r.bind(*x); }
          BigintValue(x) => { r = r.bind(*x); }
        }
      }
    }

    let rs = r.fetch_all(&self.db()).await
        .map_err(|e| e.to_string())?;
    let mut loaded = BTreeSet::new();
    for n in rs {
      loaded.insert(n.id);
      if n.id == id { // this host
        // self.cfg.bind

      } else {
        if n.active_since.is_none() { // remove?
          //
        }
        let mut nodes = self.nodes.write().await;
        match nodes.get(&n.id) {
          None => {
              self.node_connections.write().await.insert(n.id.clone(), n.connect().await);
              self.nodes.write().await.insert(n.id.clone(), n);
          }
          Some(nx) => { // check if node changed
            if nx.host != n.host {
              if let Some(nx) = self.node_connections.write().await.insert(n.id.clone(), n.connect().await) {
                if let Ok(nx) = nx {
                  // nx.lock().close()
                }
              }
            }
          }
        }
      }
    }

    self.nodes.write().await.retain(|k, _v| loaded.contains(k));
    self.node_connections.write().await.retain(|k, _v| loaded.contains(k));

    Ok(())
  }

  /// load all or reload one or remove one function, include load/remove fs_logs
  #[inline]
  async fn reload_fs(&self, x: &Option<EventRequest>) -> Result<(), String>{
    let sql = SELECT_FN.replace("%SCHEMA%", self.cfg.schema.as_str());
    let sql =  if x.is_none() || x.as_ref().unwrap().pks.len() == 0 { sql } else {
      format!("{} where {}", sql, make_sql_cnd(&x.as_ref().unwrap().pks))
    };
    let mut r = sqlx::query_as::<_, RpFn>(sql.as_str());

    if let Some(x) = &x {
      for column in &x.pks {
        if let Some(pk) = &column.pk_value {
          match pk {
            IntValue(x) => { r = r.bind(*x); }
            BigintValue(x) => { r = r.bind(*x); }
          }
        }
      }
    }

    let r = r.fetch_all(&self.db()).await
        .map_err(|e| e.to_string())?;
    let mut loaded = BTreeSet::new();
    for f in r {
      loaded.insert(f.id);
      let mut map = self.fns.write().await;
      match map.get_mut(&f.id) {
        None => {
          let mut fmap = self.fn_tt.write().await;
          match fmap.get_mut(&f.schema_table) {
            None => {
              let mut submap = HashSet::new();
              submap.insert(f.topic.clone());
              fmap.insert(f.schema_table.clone(), submap);
            }
            Some(submap) => {
              submap.insert(f.topic.clone());
            }
          }
          //
          let mut fmap = self.fn_id.write().await;
          match fmap.get_mut(&f.topic) {
            None => {
              let mut submap = BTreeSet::new();
              submap.insert(f.id.clone());
              fmap.insert(f.topic.clone(), submap);
            }
            Some(submap) => {
              submap.insert(f.id.clone());
            }
          }
              // .insert(f.topic.clone(), f.id.clone())
          map.insert(f.id.clone(), RpFnCtx{ fns: f, topics: Default::default() });
        }
        Some(fx) => {
          fx.fns.merge(f, &self);
        }
      }


    }
    self.fns.write().await.retain(|k, _v| loaded.contains(k));

    Ok(())
  }

  /// load all or reload one or remove one schedules
  #[inline]
  async fn reload_crons(&self, x: &Option<EventRequest>) -> Result<(), String> {
    let mut cl = sqlx::query_as::<_, RpFnCron>(SELECT_CRON.replace("%SCHEMA%", self.cfg.schema.as_str()).as_str())
        .fetch_all(&self.db()).await.map_err(|e| e.to_string())?;
    let mut crons = self.cron.write().await;
    crons.clear();
    crons.append(&mut cl);
    Ok(())
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

    let r = sqlx::query_as::<_, RpHost>(RpHost::select(&c.schema, "active_since is not null").as_str())
        .fetch_all(&pool).await
        .map_err(|e| e.to_string())?;
    let mut master = false; // master is present and up
    let mut found_self = false; // is self registered
    let r_len = r.len();
    let mut id = 0;
    for n in r {
      if n.host == c.this {
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
      exec: Arc::new(Default::default()),
      cron: Arc::new(Default::default()),
    };

    cluster.run(rsvr, r_len == 0 || !found_self, !master).await;

    Ok(cluster)
  }

  /// prepare to run code on startup and background
  async fn run(&self, mut rsvr: Receiver<RpFnLog>, self_register: bool, self_master: bool) {
    let ctx = self.clone();

    tokio::spawn(async move { // will execute as background, but will try to connect to this host
      if let Err(e) = ctx.start_bg(self_register, self_master).await {
        eprintln!("{}", e);
        std::process::exit(10);
      }

      loop {
        match rsvr.recv().await {
          None => { break },
          Some(ci) => ctx.execute(ci).await
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
  async fn prepare(&self, r: &EventRequest) -> Result<ScheduleResult, String> {
    let mut fn_ids: BTreeSet<i32> = BTreeSet::new();
    if let Some(topics) = self.fn_tt.read().await.get(&r.table_name) {
        for topic in topics {
          if let Some(t) = self.fn_id.read().await.get(topic) {
            fn_ids.append(&mut t.clone());
          }
        }
    };
    if fn_ids.len() == 0 {
      return Ok(ScheduleResult::None);
    }

    let mut pkk = vec![];
    let mut logs = vec![];

    let map = self.fns.read().await;
    for fn_id in &fn_ids {
      if let Some(f) = map.get(fn_id) {
        if !r.id_value && f.fns.is_dot() {
          pkk.push(f.fns.to_repeat_result());
        }
        if pkk.len() == 0 {
          for p in &r.pks {
            if let Some(pk) = &p.pk_value {
              //
            }
          }
          logs.push((RpFnLog {
            id: 0,
            node_id: 0, // TODO this host or another node
            fn_id: f.fns.id,
            took_sec: 0,
            trig_type: r.event_type,
            trig_value: Default::default(), // TODO FIXME NOW set value
            /*
            r.pk_value.as_ref().map(|e| match e {
              IntValue(v) => v as i64,
              BigintValue(v) => v,
            }),
             */
            finished_at: None,
            error_msg: None,
          }, f.fns.cleanup_logs_min, f.fns.is_dot())
          );
        }
      }
    }
    if pkk.len() > 0 {
       return Ok(ScheduleResult::Repeat(pkk));
    }

    let mut res = Vec::with_capacity(logs.len());
    for (fl, cleanup_logs_min, is_dot) in logs {
      if cleanup_logs_min > 0 {
        let r = if is_dot {
          sqlx::query_scalar::<_, i64>(RpFnLog::insert_v(&self.cfg.schema).as_str())
              .bind(fl.node_id)
              .bind(fl.fn_id)
              // .bind(fl.trig_value.clone()) // TODO no value set yet
              .bind(fl.trig_type)
              .fetch_one(&self.db()).await.map_err(|e| e.to_string())?
        } else {
          sqlx::query_scalar::<_, i64>(RpFnLog::insert(&self.cfg.schema).as_str())
              .bind(fl.node_id)
              .bind(fl.fn_id)
              .bind(fl.trig_type)
              .fetch_one(&self.db()).await.map_err(|e| e.to_string())?
        };
        res.push(RpFnLog {id: r, ..fl});
      } else {
        res.push(fl);
      };
    }
    Ok(ScheduleResult::Some(res))
  }


  /// try to self register as master if needed, return node_id
  async fn start_bg(&self, insert: bool, master: bool) -> Result<(), String> {
    let pool = self.db();
    if insert {
      let sql = INSERT_HOST.replace("%SCHEMA%", &self.cfg.schema.as_str());
      let id = sqlx::query_scalar::<_, i32>(sql.as_str())
              .bind(&self.cfg.bind)
              .bind(&self.cfg.this)
              .fetch_one(&self.db()).await.map_err(|e| e.to_string())?;

      self.node_id.store(id, Ordering::Relaxed);
    }
    let id = self.node_id.load(Ordering::Relaxed);

    let sql = UP_MASTER.replace("%SCHEMA%", &self.cfg.schema.as_str());
    if let Err(e) = sqlx::query(sql.as_str()).bind(id).execute(&pool).await {
      eprintln!("{}", e);
    }

    let sql = "select master from %SCHEMA%.rppd_config where id = $1".replace("%SCHEMA%", &self.cfg.schema.as_str());

    let master = sqlx::query_scalar::<_, bool>(sql.as_str())
        .bind(id)
        .fetch_one(&pool).await
        .map_err(|e| e.to_string())?;
    if master {
      self.master.store(true, Ordering::Relaxed);
    }

    let sql = RpHost::select(&self.cfg.schema, "id = $1");

    // load functions
    let _ = self.reload_fs(&None).await?;

    // load messages to internal queue from fn_log, check timeout and execution status,
    for l in sqlx::query_as::<_, RpFnLog>(RpFnLog::select(&self.cfg.schema).as_str())
        .fetch_all(&self.db()).await.map_err(|e| e.to_string())? {
      let _ = self.sender.send(l).await.map_err(|e| e.to_string())?;
    }

    let _ = self.reload_crons(&None).await?;

    // set STARTED
    self.started.store(true, Ordering::Relaxed);
    Ok(())
  }


  /// primary loop of event execution
  async fn execute(&self, f: RpFnLog) {
    match self.fns.read().await.get(&f.fn_id) {
      None => {
        eprintln!("no function by id: {}", f.fn_id);
      },
      Some(fc) => {
        let started = Instant::now();

        // TODO store PyContext to reuse
        let r = PyContext::new(&fc.fns, &self.cfg.db_url())
            .map(|p| p.invoke(&f, fc))
            .map_err(|e| e.to_string());

          f.update(started.elapsed().as_secs(), self.db(), &self.cfg.schema).await;
      }
    }
  }
}


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
      return if request.table_name.starts_with(&self.cfg.schema)
      && request.table_name.ends_with(CFG_FN_TABLE) {
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
      ScheduleResult::None => {}
      ScheduleResult::Some(fn_logs) => {
        for fn_log in fn_logs {
          let _ = self.sender.send(fn_log).await;
        }
      }
      ScheduleResult::Repeat(repeat_with) => {
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
