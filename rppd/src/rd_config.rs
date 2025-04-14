/*%LPH%*/


use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU16, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use slog::{crit, debug, error, info, warn, Logger};
use sqlx::{Pool, Postgres};
use sqlx::postgres::PgPoolOptions;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio::sync::mpsc::{Receiver, Sender};
use tonic::Request;
use tonic::transport::{Channel, Endpoint};
use uuid::Uuid;
use rppd_common::protogen::rppc::pk_column::PkValue::*;
use rppd_common::protogen::rppc::{status_request, DbAction, DbEventRequest, PkColumn, StatusRequest};
use rppd_common::protogen::rppd::MessageRequest;
use rppd_common::protogen::rppd::rppd_node_client::RppdNodeClient;
use rppd_common::protogen::rppd::rppd_node_server::RppdNode;
use crate::arg_config::*;
use crate::cron::CronContext;
use crate::py::PyContext;
use crate::rd_etcd::EtcdConnector;
use crate::rd_fn::*;
use crate::rd_monitor::ClusterStat;
use crate::rd_queue::QueueType;
use crate::rd_rpc::ScheduleResult;

pub(crate) const SELECT: &str = "select id, host, host_name, active_since, master, max_db_connections from %SCHEMA%.rppd_config where ";
pub(crate) const SELECT_MASTER: &str = "select id from %SCHEMA%.rppd_config where master";

const INSERT_HOST: &str = "insert into %SCHEMA%.rppd_config (host, host_name, master) values ($1, $2, $3) returning id";

pub(crate) const UP_MASTER: &str = "update %SCHEMA%.rppd_config set master = true where id = $1";
pub(crate) const DWN_MASTER: &str = "update %SCHEMA%.rppd_config set master = NULL where master and id = $1";

/// connection timeout. also sleep timeout on monitoring
pub(crate) const TIMEOUT_MS: u64 = 1000;
/// max errors including timeout, each attemnt after sleep of timeout, before
pub(crate) const MAX_ERRORS: u8 = 2;


/// Rust Python Host
///
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct RpHost {
    pub(crate) id: i32,
    pub(crate) host: String,
    pub(crate) host_name: String,
    pub(crate) active_since: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) master: Option<bool>,
    pub(crate) max_db_connections: i32,
}

impl RpHost {
    pub(crate) fn select(schema: &String, condition: &str) -> String {
        format!("{} {}", SELECT.replace("%SCHEMA%", schema.as_str()), condition)
    }

    pub(crate) async fn connect(&self) -> Result<Mutex<RppdNodeClient<Channel>>, String> {
        let path = format!("http://{}", self.host);
        RppdNodeClient::connect(Duration::from_millis(TIMEOUT_MS),
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
pub struct RppdNodeCluster {
    /// parsed configs
    pub(crate) cfg: RppdConfig,
    /// is the instance play master, receive trigger calls, schedule and call others
    pub(crate) master: Arc<AtomicBool>,
    /// is the node loaded all logs
    pub(crate) started: Arc<AtomicBool>,
    pub(crate) node_id: Arc<AtomicI32>,
    pub(crate) master_id: Arc<AtomicI32>,
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
    /// queued, in_proc
    pub(crate) stat: Arc<RwLock<ClusterStat>>,

    /// connections to nodes
    pub(crate) node_connections: Arc<RwLock<BTreeMap<i32, Result<Mutex<RppdNodeClient<Channel>>, String>>>>,

    /// connections to nodes, mostly use by master
    /// host to node_id
    pub(crate) node_ids: Arc<RwLock<HashMap<HostType, i32>>>,

    /// common queue to trigger bg dispatcher
    pub(crate) sender: Sender<Option<RpFnLog>>,

    /// common queue to trigger bg dispatcher
    pub(crate) kv_sender: Sender<MessageRequest>,

    /// queue(tablename) -> watcher_id
    pub(crate) watchers: Arc<RwLock<HashMap<SchemaTableType, WatcherW>>>,

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

    pub(crate) cron: Arc<RwLock<CronContext>>,
    pub(crate) etcd: Arc<RwLock<EtcdConnector>>,
    pub(crate) log: Logger,
}
#[cfg(feature = "etcd-external")]
pub(crate) type WatcherW = etcd_client::Watcher;

#[cfg(not(feature = "etcd-external"))]
pub(crate) struct  WatcherW {
    pub(crate) sender: Sender<etcd::etcdpb::etcdserverpb::WatchRequest>,
    pub(crate) watch_id: etcd::cluster::WatcherId,
}

#[inline]
fn make_sql_cnd(input: &Vec<PkColumn>) -> String {
    let mut sql = String::new();
    for idx in 0..input.len() {
        if idx > 0 { sql.push_str(" and "); }
        sql.push_str(format!("{} = ${}", input[idx].column_name, idx + 1).as_str());
    }
    sql
}

impl RppdNodeCluster {
    pub(crate) fn db(&self) -> Pool<Postgres> {
        self.db.clone()
    }


    #[inline]
    pub(crate) async fn reload_hosts(&self, x: &DbEventRequest) -> Result<(), String> {
        let sql = RpHost::select(&self.cfg.schema, "active_since is not null");
        let sql = if x.pks.len() == 0 { sql } else { format!("{} and {}", sql, make_sql_cnd(&x.pks)) };
        let mut r = sqlx::query_as::<_, RpHost>(sql.as_str());
        // PK ID set for Hosts is redundant, because config table has only one int PK, but use of common struct dictate to perform this way
        for x in &x.pks {
            if let Some(x) = &x.pk_value {
                match x {
                    IntValue(x) => { r = r.bind(*x); }
                    BigintValue(x) => { r = r.bind(*x); }
                    StringValue(x) => {r = r.bind(x.clone());}
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
                let nodes = self.nodes.read().await;
                match nodes.get(&n.id) {
                    None => {
                        self.node_connections.write().await.insert(n.id.clone(), n.connect().await);
                        self.nodes.write().await.insert(n.id.clone(), n);
                    }
                    Some(nx) => { // check if node changed
                        if nx.host != n.host {
                            if let Some(nx) = self.node_connections.write().await.insert(n.id.clone(), n.connect().await) {
                                if let Ok(_nx) = nx {
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
    pub(crate) async fn reload_fs(&self, x: &Option<DbEventRequest>) -> Result<(), String> {
        let sql = SELECT_FN.replace("%SCHEMA%", self.cfg.schema.as_str());
        let sql = if x.is_none() || x.as_ref().unwrap().pks.len() == 0 { sql } else {
            format!("{} where {}", sql, make_sql_cnd(&x.as_ref().unwrap().pks))
        };
        let mut r = sqlx::query_as::<_, RpFn>(sql.as_str());

        if let Some(x) = &x {
            for column in &x.pks {
                if let Some(pk) = &column.pk_value {
                    match pk {
                        IntValue(x) => { r = r.bind(*x); }
                        BigintValue(x) => { r = r.bind(*x); }
                        StringValue(x) => {r = r.bind(x.clone());}
                    }
                }
            }
        }

        let r = r.fetch_all(&self.db()).await
            .map_err(|e| format!("load functions [{}] {}", sql, e))?;
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
                    map.insert(f.id.clone(), f.watch_init(&self).await);
                }
                Some(fx) => {
                    fx.merge(f, &self).await;
                }
            }
        }
        self.fns.write().await.retain(|k, _v| loaded.contains(&RpFnId { id: *k, ..RpFnId::default() }));
        info!(self.log, "loaded {} function(s) for {} table(s) and {} topic(s)", self.fns.read().await.len(), self.fn_tt.read().await.len(), self.fn_id.read().await.len(), );
        Ok(())
    }


    /// init
    pub async fn init(c: RppdConfig,
                      #[cfg(feature = "etcd-provided")] etcd: etcd::cluster::EtcdNode,
                      log: Logger
    ) -> Result<Self, String> {
        let pool = PgPoolOptions::new()
            .max_connections(1)  // use only on init
            .connect(c.db_url().as_str()).await
            .map_err(|e| format!("Connection error to: {} {}", c.db_url(), e))?;
        // getting cluster
        let mut nodes = BTreeMap::new();
        let mut node_connections = BTreeMap::new();
        let mut node_id = HashMap::new();
        let sql = RpHost::select(&c.schema, "active_since is not null");
        let r = sqlx::query_as::<_, RpHost>(sql.as_str())
            .fetch_all(&pool).await
            .map_err(|e| format!("Loading cluster of active nodes [{}]: {}", sql, e))?;
        debug!(log, "loaded {:?} for {}", r, c.name);
        let mut master = false; // master is present and up
        let mut found_self = false; // is self registered
        let mut found_self_master = false; // is self registered
        let mut id = 0; 
        let mut master_id = 0;
        for n in r {
            if n.master.unwrap_or(false) {
                master_id = n.id;
            }
            if n.host_name == c.name {
                found_self = true;
                id = n.id;
                found_self_master = n.master.unwrap_or(false);
                master = master || found_self_master
            } else {
                let node = n.connect().await;
                master = master || n.master.unwrap_or(false) && node.is_ok();
                node_connections.insert(n.id, node);
                node_id.insert(n.host.clone(), n.id);
                nodes.insert(n.id, n);
            }
        }

        let (sender, rsvr) = mpsc::channel(c.max_queue_size);
        let (kv_sender, kv_rsvr) = mpsc::channel(c.max_queue_size);

        #[cfg(feature = "etcd-embeded")]
        let etcd = EtcdConnector::init(&c, &log).await;
        #[cfg(feature = "etcd-provided")]
        let etcd = EtcdConnector::new(etcd, &c, &log).await;
        #[cfg(feature = "etcd-external")]
        let etcd = EtcdConnector::connect(&c, &log).await;


        let cluster = RppdNodeCluster {
            cfg: c,
            master: Arc::new(AtomicBool::new(false)),
            started: Arc::new(AtomicBool::new(false)),
            node_id: Arc::new(AtomicI32::new(id)),
            master_id: Arc::new(AtomicI32::new(master_id)),
            max_db_connections: Arc::new(AtomicU16::new(10)),
            max_context: Arc::new(AtomicU16::new(0)),
            running: Arc::new(AtomicI32::new(0)),
            db: pool,
            nodes: Arc::new(RwLock::new(nodes)),
            stat: Arc::new(RwLock::new(ClusterStat::default())),
            node_connections: Arc::new(RwLock::new(node_connections)),
            node_ids: Arc::new(RwLock::new(node_id)),
            sender,
            queue: Arc::new(Default::default()),
            fns: Arc::new(Default::default()),
            fn_tt: Arc::new(Default::default()),
            fn_id: Arc::new(Default::default()),
            exec: Arc::new(Default::default()),
            cron: Arc::new(RwLock::new(CronContext {
                crons: BTreeMap::new(),
                jobs: BTreeMap::new(),
                log: log.clone(),
            })),
            etcd: Arc::new(RwLock::new(etcd)),
            kv_sender,
            watchers: Arc::new(Default::default()),
            log,
        };

        cluster.run(rsvr, kv_rsvr, !found_self, !master && !found_self_master).await;
        info!(cluster.log, "Cluster instance created");
        Ok(cluster)
    }

    /// prepare to run code on startup and background
    async fn run(&self, mut rsvr: Receiver<Option<RpFnLog>>, mut kv_rsvr: Receiver<MessageRequest>, self_register: bool, self_master: bool) {
        let ctx = self.clone();
        let ctxm = self.clone();

        tokio::spawn(async move { // will execute as background, but will try to connect to this host
            if let Err(e) = ctx.start_bg(self_register, self_master).await {
                // TODO log
                crit!(ctx.log, "Failed to start: {}", e);
                eprint!("Failed to start: {}", e);
                let _ = tokio::time::sleep(Duration::from_millis(100));
                // drop(ctx); // will flush logger on drop
                std::process::exit(12);
            }
            tokio::spawn(async move {
                ctxm.start_monitoring().await;
            });
            while let Some(ci) = rsvr.recv().await {
                ctx.execute(ci).await
            }
        });
        let ctx = self.clone();
        tokio::spawn(async move {
            while let Some(m) = kv_rsvr.recv().await {
                if let Err(e) = ctx.message(Request::new(m)).await {
                    error!(ctx.log, "{}", e);
                }
            }
        });
    }

    pub(crate) async fn check_fn(&self, table_name: &String) -> BTreeMap<i32, RpFnId> {
        let mut fn_ids = BTreeMap::new();
        if let Some(topics) = self.fn_tt.read().await.get(table_name) {
            for topic in topics {
                if let Some(t) = self.fn_id.read().await.get(topic) {
                    for fid in t {
                        fn_ids.insert(fid.id, fid.clone());
                    }
                }
            }
        };
        fn_ids
    }

    /// find topic, fnb_id and node_id by table
    /// synchroniously on client DB transaction store log, if configured
    // TODO if requested from master, than perform queueing
    #[inline]
    pub(crate) async fn prepare(&self, fn_ids: BTreeMap<i32, RpFnId>, pks: &Vec<PkColumn>, event: DbAction, value: &Option<Vec<u8>>)
        -> Result<ScheduleResult, String> {
        debug_assert!(fn_ids.len() > 0);
        let mut non_pk_column = vec![];
        let mut fn_logs = vec![];
        let mut trig_value = HashMap::new();
        for p in pks {
            if let Some(pk) = &p.pk_value {
                trig_value.insert(p.column_name.clone(), match pk {
                    IntValue(v) => v.to_string(),
                    BigintValue(v) => v.to_string(),
                    StringValue(v) => v.clone(),
                });
            }
        }
        let node_id = self.node_id.load(Ordering::Relaxed);

        let map = self.fns.read().await;
        for (fn_id, rn_fn_id) in &fn_ids {
            if let Some(f) = map.get(fn_id) {
                let is_dot = f.is_dot();

                if is_dot && !trig_value.contains_key(&f.topic.clone().split_off(1)) {
                    non_pk_column.push(f.to_repeat_result());
                }

                if non_pk_column.len() == 0 {
                    fn_logs.push(RpFnLog {
                        id: 0,
                        node_id: node_id.clone(),
                        fn_id: f.id,
                        took_sec: None,
                        uuid: None,
                        value: value.clone(), // None for db trigger, one
                        rn_fn_id: Some(rn_fn_id.clone()),
                        trig_type: event as i32,
                        trig_value: Some(sqlx::types::Json::from(trig_value.clone())),
                        started_at: Utc::now(),
                        error_msg: None,
                        fn_idp: Some(RpFnId::fromf(f)),
                        started: Some(Instant::now()),
                    }
                    );
                }
            }
        }

        if non_pk_column.len() > 0 {
            return Ok(ScheduleResult::Repeat(non_pk_column));
        }

        let mut res = Vec::with_capacity(fn_logs.len());
        for fl in fn_logs {
            if let Some(ref rn) = fl.rn_fn_id {
                if rn.save {
                    let r = if rn.trig_value {
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
                    res.push(RpFnLog { id: r, ..fl });
                } else {
                    res.push(RpFnLog { uuid: Some(Uuid::new_v4()), ..fl });
                };
            }
        }
        Ok(ScheduleResult::Some(res))
    }


    /// try to self register as master if needed, return node_id
    async fn start_bg(&self, insert: bool, master: bool) -> Result<(), String> {
        let pool = self.db();
        let host = format!("{}:{}", self.cfg.bind, self.cfg.port);
        if insert {
            info!(self.log, "Self registering: {} by name: {} {}", host, self.cfg.name, if master { "as master" } else { "" });
            let sql = INSERT_HOST.replace("%SCHEMA%", &self.cfg.schema.as_str());
            let id = sqlx::query_scalar::<_, i32>(sql.as_str())
                .bind(&host)
                .bind(&self.cfg.name)
                .bind(if master { Some(true) } else { None })
                .fetch_one(&self.db()).await
                .map_err(|e| format!("self reg failed {}", e))?;

            self.node_id.store(id, Ordering::Relaxed);
            info!(self.log, "Registered: {}", host);
        }
        let id = self.node_id.load(Ordering::Relaxed);

        if !insert && master {
            info!(self.log, "Self mark as master: {} by name: {} ", host, self.cfg.name);
            let sql = UP_MASTER.replace("%SCHEMA%", &self.cfg.schema.as_str());
            if let Err(e) = sqlx::query(sql.as_str()).bind(id).execute(&pool).await {
                error!(self.log, "{}", e);
            }
        }

        let sql = "select master from %SCHEMA%.rppd_config where id = $1".replace("%SCHEMA%", &self.cfg.schema.as_str());

        let master = sqlx::query_scalar::<_, bool>(sql.as_str())
            .bind(id)
            .fetch_one(&pool).await
            .map_err(|e| e.to_string())?;
        if master {
            self.master.store(true, Ordering::Relaxed);
            self.master_id.store(id, Ordering::Relaxed);
        }

        // let _sql = RpHost::select(&self.cfg.schema, "id = $1");

        debug!(self.log, "loading functions");
        let _ = self.reload_fs(&None).await?;

        // load messages to internal queue from fn_log, check timeout and execution status,
        for l in sqlx::query_as::<_, RpFnLog>(
            format!("{} where took_sec is null order by id", RpFnLog::select(&self.cfg.schema)).as_str())
            .fetch_all(&self.db()).await.map_err(|e| e.to_string())? {
            self.queueing(l, false).await; // append loaded on startup
        }

        debug!(self.log, "loading cron jobs");
        let _ = self.reload_cron(&None).await;

        // set STARTED
        self.started.store(true, Ordering::Relaxed);
        Ok(())
    }


    /// primary loop of event execution. If None, then take from queue
    /// call by receive
    #[inline]
    async fn execute(&self, f: Option<RpFnLog>) {
        debug!(self.log, "invoke {:?}", f);
        let f = match f {
            None => match self.queue.write().await.pick_one() {
                None => { // nothing to execute from topics or not ready as a queue
                    let mut rm = 0;
                    self.exec.write().await
                        .retain(|p| if let Ok(pp) = p.try_lock() {
                            if pp.alive() { true } else {
                                rm += 1;
                                false
                            }
                        } else { true });
                    let max = self.max_context.load(Ordering::Relaxed);
                    let max = if max > rm { max - rm } else { 0 };
                    self.max_context.store(max, Ordering::Relaxed);
                    return;
                }
                Some(f) => f
            }
            Some(f) => f
        };

        if f.took_sec.is_some() { // the function execution already completed
            if !self.master.load(Ordering::Relaxed) {
                if let Some(node) = self.node_connections.read().await.get(&self.master_id.load(Ordering::Relaxed)) {
                    if let Ok(master) = node {
                        // notify master about completion of function execution and trigger to pick a next event in a queue
                        let _ = master.lock().await.complete(StatusRequest {
                            config_schema_table: self.cfg.schema.clone(),
                            node_id: self.node_id.load(Ordering::Relaxed),
                            fn_log: Some(
                                if f.id > 0 {
                                    status_request::FnLog::FnLogId(f.id)
                                } else {
                                    status_request::FnLog::Uuid(f.uuid.unwrap_or(Uuid::new_v4()).to_string())
                                }
                            ),
                        }).await;
                    }
                }
            }
            // info!("re-send1");
            if let Err(e) = self.sender.send(None).await {
                warn!(self.log, "{}", e);
            }
            return;
        }

        let max_con_cfg = self.max_db_connections.load(Ordering::Relaxed);
        match self.fns.read().await.get(&f.fn_id) {
            None => {
                error!(self.log, "no function by id: {}", f.fn_id);
            }
            Some(fc) => {
                let started = Instant::now();

                let c = self.exec.write().await.pop_back();
                if c.is_none() && max_con_cfg <= self.max_context.load(Ordering::Relaxed) {
                    let _ = self.queueing(f, true).await; // re-queing, put back to queue
                    return;
                }
                let p = match c {
                    None => {
                        let p = match self.new_py_context(&fc, &self.cfg.db_url()).await
                            .map_err(|e| format!("connecting python to {}: {}", self.cfg.db_url(), e)) {
                            Ok(p) => p,
                            Err(e) => {
                                f.update_err(e, self.db(), &self.cfg.schema, &self.log).await;
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
                let run = self.running.clone();
                #[cfg(not(feature = "etcd-external"))]
                let etcd = self.etcd.clone();
                let log = self.log.clone();
                tokio::spawn(async move {
                    let r = p.lock().await.invoke(&f, &fc);

                    #[cfg(not(feature = "etcd-external"))]
                    if fc.is_etcd_queue() && fc.queue {
                        etcd.read().await.aks(&fc.schema_table).await;
                    }

                    match &r {
                        Ok(_) => debug!(log, "executed OK! {} {}", fc.schema_table, fc.topic),
                        Err(e) => error!(log, "executed notOK#{} {} {}", fc.schema_table, fc.topic, e),
                    }
                    
                    let sec = started.elapsed().as_secs();
                    f.update(sec, r.err(), db, &schema, &log).await;
                    exec.write().await.push_front(p);
                    run.fetch_sub(1, Ordering::Relaxed);
                    if let Err(e) =
                        queue.send(Some(RpFnLog { took_sec: Some(sec as i32), ..f })).await {
                        warn!(log, "{}", e);
                    }
                });
            }
        }
    }


    /// prepare to execute: put into queue or send to
    // TODO link the queue (if use) to the node, if that node no longer available, than link to another one
    #[inline]
    pub(crate) async fn queueing(&self, l: RpFnLog, push_back: bool) {
        let (topic, fn_id, tbl) = if let Some(f) = self.fns.read().await.get(&l.fn_id) {
            (f.to_topic(&l), l.fn_idp.to_owned().unwrap_or(RpFnId::fromf(f)), f.schema_table.clone())
        } else { ("".to_string(), RpFnId::default(), "".to_string()) }; // should not happens

        if push_back { // queue not ready to pick next
            let mut queues = self.queue.write().await;
            match &l.started {
                None => queues.put_one(l, topic, fn_id, true),
                Some(_uid) => queues.return_back(l, topic)
            }
        } else {
            match self.pick_node().await {
                None => {
                    if self.max_db_connections.load(Ordering::Relaxed) > self.max_context.load(Ordering::Relaxed) {
                        let _ = self.sender.send(Some(l)).await; // queueing()
                    } else {
                        self.queue.write().await.put_one(l, topic, fn_id, false);
                    }
                }
                Some(node_id) => if let Some(node) = self.node_connections.read().await.get(&node_id) {
                    if let Ok(node) = node {
                        let node_id = self.node_id.load(Ordering::Relaxed);
                        match node.lock().await.event(l.to_event(tbl, node_id)).await {
                            Ok(_) => {}
                            Err(_) => {
                                // TODO mark node fail
                                self.queue.write().await.put_one(l, topic, fn_id, false);
                            }
                        }
                    }
                }
            }
        }
    }
}


#[cfg(test)]
mod tests {
    #![allow(warnings, unused)]


  #[tokio::test]
    async fn test_p() {
        let mut cnt = 0;
        let mut v = vec![1, 2, 3];
        v.retain(|x| if *x > 2 { true } else {
            cnt += 1;
            false
        });
        assert_eq!(v, vec![3]);
        assert_eq!(cnt, 2);
    }
}