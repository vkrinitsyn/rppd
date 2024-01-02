/*%LPH%*/


use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::Deref;
use sqlx::{Database, PgPool, Pool, Postgres};
use crate::gen::rg::grpc_client::GrpcClient;
use crate::gen::rg::*;

use std::path::Path;
use std::str::FromStr;

use async_trait::async_trait;use rust_embed::RustEmbed;
use lazy_static::lazy_static;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, Ordering};
use std::time::{Duration, Instant};
use chrono::{format, Utc};
use pgrx::{Json, PgTriggerError};
use pgrx::pg_catalog::pg_proc::ProArgMode::In;
use tonic::{Request, Response, Status};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::transport::{Channel, Endpoint, Server};

use crate::gen::rg::grpc_server::{Grpc, GrpcServer};

use tokio::sync::{mpsc, Mutex, RwLock, RwLockWriteGuard};
use tonic::codegen::Body;
use uuid::Uuid;
use crate::arg_config::*;
use crate::cron::RpFnCron;
use crate::gen::rg::*;
use crate::gen::rg::pk_column::PkValue;
use crate::gen::rg::pk_column::PkValue::{BigintValue, IntValue};
use crate::gen::rg::status_request::FnLog::FnLogId;
use crate::py::PyContext;
use crate::rd_fn::*;
use crate::rd_queue::QueueType;
use crate::rd_rpc::ScheduleResult;

const SELECT: &str = "select id, host, host_name, active_since, master, max_db_connections from %SCHEMA%.rppd_config where ";
const INSERT_HOST: &str = "insert into %SCHEMA%.rppd_config (host, host_name, master) values ($1, $2, $3) returning id";
const UP_MASTER: &str = "update %SCHEMA%.rppd_config set master = true where id = $1";
pub const TIMEOUT_MS: u64 = 1000;

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
    let path = format!("http://{}", self.host);
      GrpcClient::connect(Duration::from_millis(TIMEOUT_MS),
                          Endpoint::from_str(path.as_str())
                              .map_err(|e| format!("connecting to {} {}", path, e))?)
          .await
          .map(|c| Mutex::new(c))
          .map_err(|e| format!("connecting {}", e))
  }

}


/// on column @extschema@.rppd_function.topic
/// is '"": queue per table; => schema.table
/// ".{column_name}": queue per value of the column in this table; => schema.table.column
/// "{any_name}": global queue'; => ..name

pub(crate) type TopicType = String;

/// Topic definition extracted from topic type and event
pub(crate) type TopicDef = String;


/// String of schema.table
pub(crate) type SchemaTableType = String;

/// String of host:port
pub(crate) type HostType = String;


#[derive(Clone)]
pub(crate) struct Cluster {
  /// parsed configs
  pub(crate) cfg: ArgConfig,
  /// is the instance play master, receive trigger calls, schedule and call others
  pub(crate) master: Arc<AtomicBool>,
  /// is the node loaded all logs
  pub(crate) started: Arc<AtomicBool>,
  pub(crate) node_id: Arc<AtomicI32>,
  /// configured
  pub(crate) max_db_connections: Arc<AtomicU16>,
  /// contents created
  pub(crate) max_context: Arc<AtomicU16>,
  /// currently running functions
  pub(crate) running: Arc<AtomicI32>,

  /// postgres connection pool use by sqlx on init
  pub(crate) db: Pool<Postgres>,

  /// to nodes, mostly use by master
  pub(crate) nodes: Arc<RwLock<BTreeMap<i32, RpHost>>>,
  /// connections to nodes
  pub(crate) node_connections: Arc<RwLock<BTreeMap<i32, Result<Mutex<GrpcClient<Channel>>, String>>>>,

  /// connections to nodes, mostly use by master
  /// host to node_id
  pub(crate) node_ids: Arc<RwLock<HashMap<HostType, i32>>>,

  /// common queue to trigger bg dispatcher
  pub(crate) sender: Sender<Option<RpFnLog>>,

  /// Python functions execution by fn id
  /// internal fn topics and queues
  pub(crate) fns: Arc<RwLock<BTreeMap<i32, RpFn>>>,

  /// tablename -> topic
  pub(crate) fn_tt: Arc<RwLock<HashMap<SchemaTableType, HashSet<TopicType>>>>,

  /// topic -> function id (priority)
  pub(crate) fn_id: Arc<RwLock<HashMap<TopicType, BTreeSet<RpFnId>>>>,

  /// max is max_concurrent
  pub(crate) exec: Arc<RwLock<VecDeque<Mutex<PyContext>>>>,

  /// currently executing queues per topic as generated name. None value means the RpFnLog is in progress as a queue set
  pub(crate) queue: Arc<RwLock<QueueType>>,

  /// TODO implement cron
  pub(crate) cron: Arc<RwLock<Vec<RpFnCron>>>,
}

#[inline]
fn make_sql_cnd(input: &Vec<PkColumn>) -> String {
  let mut sql = String::new();
  for idx in 0..input.len() {
    if idx > 0 { sql.push_str(" and "); }
    sql.push_str(format!("{} = ${}", input[idx].column_name, idx+1).as_str());
  }
  sql
}

impl Cluster {
  pub(crate) fn db(&self) -> Pool<Postgres> {
    self.db.clone()
  }


  #[inline]
  pub(crate) async fn reload_hosts(&self, x: &EventRequest) -> Result<(), String> {
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
    let host = format!("{}:{}", self.cfg.bind, self.cfg.port);
    for n in rs {
      loaded.insert(n.id);
      if n.host == host { // this host
        self.node_id.store(n.id, Ordering::Relaxed);
        self.max_db_connections.store(n.max_db_connections as u16, Ordering::Relaxed);
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

    self.nodes.write().await.retain(|k, _v| !loaded.contains(k));
    self.node_connections.write().await.retain(|k, _v| !loaded.contains(k));

    Ok(())
  }

  /// load all or reload one or remove one function, include load/remove fs_logs
  #[inline]
  pub(crate) async fn reload_fs(&self, x: &Option<EventRequest>) -> Result<(), String>{
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
      loaded.insert(RpFnId::fromf(&f));
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
              submap.insert(RpFnId::fromf(&f));
              fmap.insert(f.topic.clone(), submap);
            }
            Some(submap) => {
              submap.insert(RpFnId::fromf(&f));
            }
          }
              // .insert(f.topic.clone(), f.id.clone())
          map.insert(f.id.clone(), f);
        }
        Some(fx) => {
          fx.merge(f, &self);
        }
      }


    }
    self.fns.write().await.retain(|k, _v| loaded.contains(&RpFnId{id: *k, ..RpFnId::default()}));
    println!("loaded {} function(s) for {} table(s) and {} topic(s)", self.fns.read().await.len(), self.fn_tt.read().await.len(), self.fn_id.read().await.len(), );
    Ok(())
  }


  /// init
  pub(crate) async fn init(c: ArgConfig) -> Result<Self, String> {
    let pool = PgPoolOptions::new()
        .max_connections(1)  // use only on init
        .connect(c.db_url().as_str()).await
        .map_err(|e| format!("Connection error to: {}", c.db_url()))?;
    // getting cluster
    let mut nodes = BTreeMap::new();
    let mut node_connections = BTreeMap::new();
    let mut node_id = HashMap::new();

    let r = sqlx::query_as::<_, RpHost>(RpHost::select(&c.schema, "active_since is not null").as_str())
        .fetch_all(&pool).await
        .map_err(|e| format!("Loading cluster of active nodes: {}", e))?;
    let mut master = false; // master is present and up
    let mut found_self = false; // is self registered
    let mut found_self_master = false; // is self registered
    let r_len = r.len();
    let mut id = 0;
    for n in r {
      if n.host_name == c.this {
        found_self = true;
        id = n.id;
        found_self_master = n.master.unwrap_or(false);
        master = master || found_self_master
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
      max_db_connections: Arc::new(AtomicU16::new(10)),
      max_context: Arc::new(AtomicU16::new(0)),
      running: Arc::new(AtomicI32::new(0)),
      db: pool,
      nodes: Arc::new(RwLock::new(nodes)),
      node_connections: Arc::new(RwLock::new(node_connections)),
      node_ids: Arc::new(RwLock::new(node_id)),
      sender,
      queue: Arc::new(Default::default()),
      fns: Arc::new(Default::default()),
      fn_tt: Arc::new(Default::default()),
      fn_id: Arc::new(Default::default()),
      exec: Arc::new(Default::default()),
      cron: Arc::new(Default::default()),
    };

    cluster.run(rsvr, r_len == 0 || !found_self, !master && !found_self_master).await;
    println!("Cluster instance created");
    Ok(cluster)
  }

  /// prepare to run code on startup and background
  async fn run(&self, mut rsvr: Receiver<Option<RpFnLog>>, self_register: bool, self_master: bool) {
    let ctx = self.clone();

    tokio::spawn(async move { // will execute as background, but will try to connect to this host
      if let Err(e) = ctx.start_bg(self_register, self_master).await {
        eprintln!("Failed to start:{}", e);
        std::process::exit(11);
      }

      loop {

        match rsvr.recv().await {
          None => { break },
          Some(ci) => ctx.execute(ci).await
        }
      }

    });
  }


  /// find topic, fnb_id and node_id by table
  /// synchroniously on client DB transaction store log, if configured
  #[inline]
  pub(crate) async fn prepare(&self, r: &EventRequest) -> Result<ScheduleResult, String> {
    let mut fn_ids: BTreeSet<i32> = BTreeSet::new();
    if let Some(topics) = self.fn_tt.read().await.get(&r.table_name) {
        for topic in topics {
          if let Some(t) = self.fn_id.read().await.get(topic) {
            for fid in t {
              fn_ids.insert(fid.id);
            }
          }
        }
    };
    if fn_ids.len() == 0 {
      return Ok(ScheduleResult::None);
    }

    let mut pkk = vec![];
    let mut logs = vec![];
    let mut trig_value = HashMap::new();
    for p in &r.pks {
      if let Some(pk) = &p.pk_value {
        trig_value.insert(p.column_name.clone(), match pk {
          IntValue(v) => v.to_string(),
          BigintValue(v) => v.to_string()
        });
      }
    }
    let node_id = self.node_id.load(Ordering::Relaxed);
    let map = self.fns.read().await;
    for fn_id in &fn_ids {
      if let Some(f) = map.get(fn_id) {
        let is_dot = f.is_dot();
        if is_dot && !trig_value.contains_key(&f.topic.clone().split_off(1)) {
          pkk.push(f.to_repeat_result());
        }
        if pkk.len() == 0 {
          logs.push((RpFnLog {
            id: 0,
            node_id: node_id.clone(),
            fn_id: f.id,
            took_sec: None,
            uuid: None,
            trig_type: r.event_type,
            trig_value: Some(sqlx::types::Json::from(trig_value.clone())),
            started_at: Utc::now(),
            error_msg: None,
            fn_idp: Some(RpFnId::fromf(f)),
            started: Some(Instant::now()),
          }, f.cleanup_logs_min, is_dot)
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
              .bind(fl.trig_type)
              .bind(fl.trig_value.clone())
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
        res.push(RpFnLog {uuid: Some(Uuid::new_v4()), ..fl});
      };
    }
    Ok(ScheduleResult::Some(res))
  }


  /// try to self register as master if needed, return node_id
  async fn start_bg(&self, insert: bool, master: bool) -> Result<(), String> {
    let pool = self.db();
    let host = format!("{}:{}", self.cfg.bind, self.cfg.port);
    if insert {
      println!("Self registering: {} by name: {} {}", host, self.cfg.this, if master { "as master"} else {""});
      let sql = INSERT_HOST.replace("%SCHEMA%", &self.cfg.schema.as_str());
      let id = sqlx::query_scalar::<_, i32>(sql.as_str())
          .bind(&host)
          .bind(&self.cfg.this)
          .bind(if master { Some(true) } else {None})
          .fetch_one(&self.db()).await.map_err(|e| e.to_string())?;

      self.node_id.store(id, Ordering::Relaxed);
      println!("Registered: {}", host);
    }
    let id = self.node_id.load(Ordering::Relaxed);

    if !insert && master {
      println!("Self mark as master: {} by name: {} ", host, self.cfg.this);
      let sql = UP_MASTER.replace("%SCHEMA%", &self.cfg.schema.as_str());
      if let Err(e) = sqlx::query(sql.as_str()).bind(id).execute(&pool).await {
        eprintln!("{}", e);
      }
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
    for l in sqlx::query_as::<_, RpFnLog>(
      format!("{} where took_sec is null order by id", RpFnLog::select(&self.cfg.schema)).as_str())
        .fetch_all(&self.db()).await.map_err(|e| e.to_string())? {
      self.queueing(l, false).await; // append loaded on startup
    }

    // let _ = self.reload_crons(&None).await?;
    RpFnCron::load(&self.cfg.schema, self.db(), &mut *self.cron.write().await);

    // set STARTED
    self.started.store(true, Ordering::Relaxed);
    Ok(())
  }


  /// primary loop of event execution. If None, then take from queue
  /// call by receive
  #[inline]
  async fn execute(&self, f: Option<RpFnLog>) {
    let f = match f {
      None => match self.queue.write().await.pick_one() {
          None => { // nothing to execute from topics or not ready as a queue
            let mut rm = 0;
            self.exec.write().await
                .retain(|p| if let Ok(pp) = p.try_lock() { if pp.alive() {true} else { rm += 1; false} } else { true });
            let max = self.max_context.load(Ordering::Relaxed);
            let max = if max>rm {max - rm} else {0};
            self.max_context.store(max, Ordering::Relaxed);
            return;
          }
          Some(f) => f
        }
      Some(f) => f
    };

    let max_con_cfg = self.max_db_connections.load(Ordering::Relaxed);
    match self.fns.read().await.get(&f.fn_id) {
      None => {
        eprintln!("no function by id: {}", f.fn_id);
      },
      Some(fc) => {
        let started = Instant::now();

        let c = self.exec.write().await.pop_back();
        if c.is_none() && max_con_cfg <= self.max_context.load(Ordering::Relaxed) {
          let _ = self.queueing(f, true).await; // re-queing, put back to queue
          return;
        }
        let p = match c {
          None => {
            let p = match PyContext::new(&fc, &self.cfg.db_url())
                .map_err(|e| format!("connecting python to {}: {}", self.cfg.db_url(), e)) {
              Ok(p) => p,
              Err(e) => {
                f.update_err(e, self.db(), &self.cfg.schema).await;
                let _ = self.sender.send(Some(f)).await; // re-queing
                return;
              }
            };
            self.max_context.fetch_add(1, Ordering::Relaxed);
            Mutex::new(p)
          }
          Some(p) => p
        };
        self.running.fetch_add(1, Ordering::Relaxed);

        // thread this >
        let exec = self.exec.clone();
        let db = self.db();
        let schema = self.cfg.schema.clone();
        let queue = self.sender.clone();
        let fc = fc.clone();
        let f = f.clone();
        let run = self.running.clone();
        tokio::spawn(async move {
          let r = p.lock().await.invoke(&f, &fc);

          println!("executed {}#{} {}", fc.schema_table, fc.topic,
                   match &r {
                     Ok(_) => "OK".to_string(),
                     Err(e) => e.clone(),
                   });
          f.update(started.elapsed().as_secs(), r.err(), db, &schema).await;
          exec.write().await.push_front(p);
          run.fetch_sub(1, Ordering::Relaxed);
          queue.send(None).await;
        });
      }
    }
  }


  /// prepare to execute: put into queue or send to
  // TODO link the queue (if use) to the node, if that node no longer available, than link to another one
  #[inline]
  pub(crate) async fn queueing(&self, l: RpFnLog, push_back: bool) {
    let (topic, fn_id) = if let Some(f) = self.fns.read().await.get(&l.fn_id) {
      (f.to_topic(&l), l.fn_idp.to_owned().unwrap_or(RpFnId::fromf(f)))
    } else { ("".to_string(), RpFnId::default())}; // should not happens

    if push_back {
      let mut queues = self.queue.write().await;
      match &l.started {
        None => queues.put_one(l, topic, fn_id, true),
        Some(_uid) => queues.return_back(l, topic)
      }
    } else {
      if self.max_db_connections.load(Ordering::Relaxed) > self.max_context.load(Ordering::Relaxed) {
        let _ = self.sender.send(Some(l)).await; // queueing()
      } else {
        self.queue.write().await.put_one(l, topic, fn_id, false);
      }
    }
  }
}


#[cfg(test)]
mod tests {
  #![allow(warnings, unused)]


  use super::*;


  #[tokio::test]
  async fn test_p() {
    let mut cnt = 0;
    let mut v = vec![1,2,3];
    v.retain(|x| if *x>2 {true} else {cnt +=1; false});
    assert_eq!(v, vec![3]);
    assert_eq!(cnt, 2);
 }

}