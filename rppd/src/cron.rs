#![allow(unused_variables, dead_code)]

use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use slog::{error, warn, Logger};
use sqlx::{Pool, Postgres};
use rppd_common::protogen::rppc::DbEventRequest;
use crate::rd_config::RppdNodeCluster;
use crate::cadence::Cadence;
use crate::LP;

pub(crate) const SELECT_CRON: &str = "select id, fn_id, cron, column_name, column_value, cadence, timeout_sec, started_at, finished_at, error_msg, \
    watch_min, retry_sec, watch_q, deadline_q, state, last_seen, since, attempt from %SCHEMA%.rppd_cron";
/// [cadence] a table from before the cadence columns: cron runs, cadence does not
const SELECT_CRON_LEGACY: &str = "select id, fn_id, cron, column_name, column_value, cadence, timeout_sec, started_at, finished_at, error_msg, \
    0 as watch_min, 0 as retry_sec, null::varchar as watch_q, null::varchar as deadline_q, 'ok'::varchar as state, \
    null::timestamptz as last_seen, null::timestamptz as since, 0 as attempt from %SCHEMA%.rppd_cron";
pub type CronDTType = DateTime<Utc>;

/// Rust Python Function Cron
#[derive(sqlx::FromRow, PartialEq, Debug, Clone)]
pub struct RpFnCron {
    pub(crate) id: i32,
    pub(crate) fn_id: i32,
    pub(crate) cron: String,
    // cron-parser

    pub(crate) column_name: String , // varchar(64) not null,
    pub(crate) column_value: Option<String>,
    pub(crate) cadence: bool, // not null default false,

    pub(crate) timeout_sec: Option<i32>,
    pub(crate) started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) error_msg: Option<String>,

    // [cadence] see cadence.rs
    pub(crate) watch_min: i32,
    pub(crate) retry_sec: i32,
    pub(crate) watch_q: Option<String>,
    pub(crate) deadline_q: Option<String>,
    pub(crate) state: String,
    pub(crate) last_seen: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) since: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) attempt: i32,
    /// the master's runtime of a cadence row, kept across reloads
    #[sqlx(skip)]
    pub(crate) cadence_rt: Option<Cadence>,
}

impl RpFnCron {
    /// the settings of a cadence row; a change restarts its runtime from the row
    fn cadence_cfg(&self) -> (i32, &String, i32, i32, &Option<String>, &Option<String>) {
        (self.fn_id, &self.cron, self.watch_min, self.retry_sec, &self.watch_q, &self.deadline_q)
    }

    /// load all or reload one or remove one schedules

    async fn update_err(&self, db: &Pool<Postgres>, msg: String, schema: &String, log: &Logger) -> Result<(), String> {
        let sql = "update %SCHEMA%.rppd_cron set error_msg = $2 where id = $1";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.id)
            .bind(Some(msg))
            .execute(db).await {
            error!(log, "{}{}", LP, e);
        }

        Ok(())
    }

    async fn update_no_err(&self, db: &Pool<Postgres>, schema: &String, log: &Logger) -> Result<(), String> {
        let sql = "update %SCHEMA%.rppd_cron set error_msg = NULL where id = $1";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(sql))
            .bind(self.id)
            .execute(db).await {
            error!(log, "{}{}", LP, e);
        }

        Ok(())
    }

    async fn start(&mut self, db: &Pool<Postgres>, schema: &String, target: String, log: &Logger) -> Option<String> {
        let now = Utc::now();

        if let Some(started_at) = self.started_at {
            if let Some(finished_at) = self.finished_at {
                if let Some(timeout) = self.timeout_sec {
                    let duration = now - self.started_at.clone().unwrap();
                    if timeout > 0 && duration.num_seconds() < timeout as i64 {
                        return None;
                    }
                }
            }
        }

        self.started_at = Some(now);
        self.finished_at = None;
        let sql = "update %SCHEMA%.rppd_cron set started_at = current_timestamp, finished_at = NULL where id = $1";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(sql.as_str()))
            .bind(self.id)
            .execute(db).await {
            error!(log, "{}starting cron job with SQL: {} raise error: {}", LP, sql, e);
            return None;
        }

        let sql = "update %SCHEMA%.rppd_cron set finished_at = current_timestamp, error_msg = ";
        let mut esql = sql.replace("%SCHEMA%", schema.as_str());

        let sql = "update %TARGET% set ";
        let mut sql = sql.replace("%TARGET%", target.as_str());
        sql.push_str(self.column_name.as_str());
        if let Err(e) = match {
            match &self.column_value {
                None => {
                    sql.push_str(" = NULL where id = $1");
                    sqlx::query(sqlx::AssertSqlSafe(sql)).bind(self.id).execute(db).await
                }
                Some(v) => {
                    sql.push_str(" = $1 where id = $2");
                    sqlx::query(sqlx::AssertSqlSafe(sql))
                        .bind(v)
                        .bind(self.id).execute(db).await
                }
            }
        } {
            Ok(_) => {
                esql.push_str("NULL where id = $1");
                sqlx::query(sqlx::AssertSqlSafe(esql.as_str())).bind(self.id).execute(db).await
            }
            Err(e) => {
                esql.push_str("$1 where id = $2");
                sqlx::query(sqlx::AssertSqlSafe(esql.as_str())).bind(Some(e.to_string())).bind(self.id).execute(db).await
            }
        }
        {
            error!(log, "{}finishing cron job SQL: {}, raise error{}", LP, esql, e);
        }
        self.finished_at = Some(Utc::now());
        Some(self.cron.clone())
    }

}

impl RppdNodeCluster {
    /// periodic (every second) spawn a job check, call by monitoring loop
    /// the job itself a lightweight trigger to update a target table which will a spawn a new transaction as Python function
    pub(crate) async fn cronjob(&self) {
        if let Ok(mut cron) = self.cron.try_write() {
            let db = self.db();
            let schema = self.cfg.read().await.schema.clone();
            let mut jobs = Vec::with_capacity(cron.jobs.len());
            let now = Utc::now();
            for (dt, id) in &cron.jobs {
                if dt < &now {
                     jobs.push((*id, dt.clone()));
                } else {
                    break;
                }
            }
            // the master runs the jobs (the cadence tick checks the same): a job is an
            // UPDATE of its target row, and on every node of a yt cluster it was one
            // replicated write per node per due time, the function run as many times.
            // Another node only moves its schedule on, so a node that becomes master
            // does not fire every job it skipped meanwhile.
            if !self.master.load(std::sync::atomic::Ordering::Relaxed) {
                for (id, dt) in jobs {
                    let next = cron.crons.get(&id).and_then(|c| cron_parser::parse(c.cron.as_str(), &now).ok());
                    cron.jobs.remove(&dt);
                    if let Some(next) = next {
                        cron.jobs.insert(next, id);
                    }
                }
                return;
            }
            for (id, dt) in jobs {

                #[allow(unused_mut)]
                if let Some(crontab) = if let Some(mut c) = cron.crons.get_mut(&id) {
                    let target = match self.fns.read().await.get(&c.fn_id) {
                        None => { continue; }
                        Some(pfn) => pfn.schema_table.clone()
                    };
                    c.start(&db, &schema, target, &self.log).await
                } else { None } {
                    cron.jobs.remove(&dt);
                    if let Ok(dt) = cron_parser::parse(crontab.as_str(), &Utc::now()) {
                        cron.jobs.insert(dt, id);
                    }
                }
            }
        }

    }

    /// cron load on start OR reload on event
    pub(crate) async fn reload_cron(&self, request: &Option<DbEventRequest>) -> Result<(), String>  {
        let mut cron = self.cron.write().await;
        cron.reload(self.cfg.read().await.schema.clone(), self.db()).await
    }
}

pub struct CronContext {
    /// loaded cron jobs - only on master node
    pub(crate) crons: BTreeMap<i32, RpFnCron>,
    /// sorted cron jobs
    pub(crate) jobs: BTreeMap<CronDTType, i32>,
    pub(crate) log: Logger,
    /// [cadence] what a pp never seen counts from
    pub(crate) started: CronDTType,
    /// [cadence] the table has the cadence columns
    pub(crate) cadence_on: bool,
    /// [cadence] the master term (`RppdNodeCluster::master_epoch`) the runtimes belong to
    pub(crate) cadence_epoch: u64,
}

impl CronContext {
    /// [cadence] Drop every runtime when this node became master again since they
    /// were built: they are from its previous term, and another master may have
    /// moved the rows since. They are rebuilt from the stored rows - the state the
    /// last master wrote - not from what this node remembers. Returns true when
    /// a new term began.
    pub(crate) fn new_term(&mut self, epoch: u64) -> bool {
        if self.cadence_epoch == epoch {
            return false;
        }
        self.cadence_epoch = epoch;
        for c in self.crons.values_mut() {
            c.cadence_rt = None;
        }
        true
    }

    /// background reloading cron jobs.
    /// cleanup
     #[inline]
   pub(crate) async fn reload(&mut self, schema: String, db: Pool<Postgres>) -> Result<(), String> {
        let cl = match sqlx::query_as::<_, RpFnCron>(sqlx::AssertSqlSafe(SELECT_CRON.replace("%SCHEMA%", schema.as_str())))
            .fetch_all(&db).await {
            Ok(cl) => { self.cadence_on = true; cl }
            Err(first) => {
                // [cadence] a table from before the cadence columns, which SchemaGuard
                // did not bring up to date at startup (schema.rs): cron runs alone
                warn!(self.log, "{}cadence off: rppd_cron lacks its columns ({})", LP, first);
                self.cadence_on = false;
                sqlx::query_as::<_, RpFnCron>(sqlx::AssertSqlSafe(SELECT_CRON_LEGACY.replace("%SCHEMA%", schema.as_str())))
                    .fetch_all(&db).await.map_err(|e| e.to_string())?
            }
        };

        // [cadence] the runtime of an unchanged cadence row carries over a reload:
        // the master's own state writes reload the table, and a runtime rebuilt
        // from the row would repeat the watch message it just sent
        let mut old = std::mem::take(&mut self.crons);
        self.jobs.clear();
        for mut c in cl {
            if c.cadence {
                if let Some(o) = old.remove(&c.id) {
                    if o.cadence && o.cadence_cfg() == c.cadence_cfg() {
                        c.cadence_rt = o.cadence_rt;
                    }
                }
                // evaluated by the master's cadence tick, never scheduled as a job
                self.crons.insert(c.id, c);
                continue;
            }
            match cron_parser::parse(c.cron.as_str(), &Utc::now()) {
                Ok(d) => {
                    if c.error_msg.is_some() {
                        let _ = c.update_no_err(&db, &schema, &self.log).await;
                    } else {
                        self.jobs.insert(d, c.id);
                        self.crons.insert(c.id, c);
                    }
                }
                Err(e) => {
                    let _ = c.update_err(&db, format!("parsing cron#[{}]: {} error: {}", c.id, c.cron, e), &schema, &self.log).await;
                }
            }
        }

        Ok(())
    }
}
