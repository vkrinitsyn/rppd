#![allow(unused_variables, dead_code)]

use std::collections::{BTreeMap, HashMap};
use chrono::{DateTime, Utc};
use slog::{error, warn, Logger};
use sqlx::{Pool, Postgres};
use rppd_common::protogen::rppc::DbEventRequest;
use crate::rd_config::RppdNodeCluster;
use crate::cadence::Cadence;
use crate::LP;

/// `%TEST%`: [test] test_level, or NULL where the table lacks it
pub(crate) const SELECT_CRON: &str = "select id, fn_id, cron, column_name, column_value, cadence, timeout_sec, started_at, finished_at, error_msg, \
    watch_min, retry_sec, watch_q, deadline_q, state, last_seen, since, attempt, %TEST% from %SCHEMA%.rppd_cron";
/// [cadence] a table from before the cadence columns: cron runs, cadence does not
const SELECT_CRON_LEGACY: &str = "select id, fn_id, cron, column_name, column_value, cadence, timeout_sec, started_at, finished_at, error_msg, \
    0 as watch_min, 0 as retry_sec, null::varchar as watch_q, null::varchar as deadline_q, 'ok'::varchar as state, \
    null::timestamptz as last_seen, null::timestamptz as since, 0 as attempt, %TEST% from %SCHEMA%.rppd_cron";
pub type CronDTType = DateTime<Utc>;
/// the code of `cron.parse`, in every language: only such an `error_msg` is the schedule's own
const PARSE_ERROR: &str = "RPPD-E082 ";

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
    /// [test] a test job: the test(level) of its function at this level, not the UPDATE
    /// that fires it (rd_test.rs); None: a job
    pub(crate) test_level: Option<i32>,
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
            error!(log, "{}{}", LP, tr!("cron.start", sql = sql, err = e));
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
            error!(log, "{}{}", LP, tr!("cron.finish", sql = esql, err = e));
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
                    cron.jobs.remove(&(dt, id));
                    if let Some(next) = next {
                        cron.jobs.insert((next, id));
                    }
                }
                return;
            }
            for (id, dt) in jobs {

                #[allow(unused_mut)]
                if let Some(crontab) = if let Some(mut c) = cron.crons.get_mut(&id) {
                    // [test] a test job: the test of its function, not the UPDATE that fires it;
                    // its row says how it went, as a job's does, and the test's log row (rd_test.rs)
                    if let Some(level) = c.test_level {
                        let fc = match self.fns.read().await.get(&c.fn_id) {
                            None => { continue; }
                            Some(pfn) => pfn.clone()
                        };
                        c.started_at = Some(Utc::now());
                        c.finished_at = None;
                        let (me, cid, timeout) = (self.clone(), c.id, c.timeout_sec);
                        tokio::spawn(async move {
                            me.test_cron(fc, level, cid, timeout).await;
                        });
                        Some(c.cron.clone())
                    } else {
                        let target = match self.fns.read().await.get(&c.fn_id) {
                            None => { continue; }
                            Some(pfn) => pfn.schema_table.clone()
                        };
                        c.start(&db, &schema, target, &self.log).await
                    }
                } else { None } {
                    cron.jobs.remove(&(dt, id));
                    if let Ok(next) = cron_parser::parse(crontab.as_str(), &Utc::now()) {
                        cron.jobs.insert((next, id));
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
    /// sorted cron jobs: by due time, then id - two rows due in one minute both run
    pub(crate) jobs: std::collections::BTreeSet<(CronDTType, i32)>,
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

    /// [own-tables] The rows read, as the schedule: what else a reload changes nothing of. An
    /// unchanged row keeps its due time, so a job due meanwhile still fires, and stays scheduled
    /// whatever its last run left in `error_msg`. Only a parse error ends here: a cron now good
    /// clears it, one still bad says so once. Returns the rows' `error_msg` to store.
    fn schedule(&mut self, cl: Vec<RpFnCron>, now: CronDTType) -> Vec<(i32, Option<String>)> {
        let mut old = std::mem::take(&mut self.crons);
        let due: HashMap<i32, CronDTType> = std::mem::take(&mut self.jobs).into_iter().map(|(d, id)| (id, d)).collect();
        let mut store = vec![];
        for mut c in cl {
            let was = old.remove(&c.id);
            if c.cadence {
                // [cadence] the runtime of an unchanged cadence row carries over a reload:
                // the master's own state writes reload the table, and a runtime rebuilt
                // from the row would repeat the watch message it just sent
                if let Some(o) = was {
                    if o.cadence && o.cadence_cfg() == c.cadence_cfg() {
                        c.cadence_rt = o.cadence_rt;
                    }
                }
                // evaluated by the master's cadence tick, never scheduled as a job
                self.crons.insert(c.id, c);
                continue;
            }
            let kept = was.filter(|o| !o.cadence && o.cron == c.cron).and_then(|_| due.get(&c.id).copied());
            match kept.map_or_else(|| cron_parser::parse(c.cron.as_str(), &now), Ok) {
                Ok(d) => {
                    if c.error_msg.as_deref().map_or(false, |e| e.starts_with(PARSE_ERROR)) {
                        store.push((c.id, None));
                        c.error_msg = None;
                    }
                    self.jobs.insert((d, c.id));
                    self.crons.insert(c.id, c);
                }
                Err(e) => {
                    let msg = tr!("cron.parse", id = c.id, cron = c.cron, err = e);
                    if c.error_msg.as_deref() != Some(msg.as_str()) {
                        store.push((c.id, Some(msg)));
                    }
                }
            }
        }
        store
    }

    /// background reloading cron jobs.
    /// cleanup
     #[inline]
   pub(crate) async fn reload(&mut self, schema: String, db: Pool<Postgres>) -> Result<(), String> {
        // [test] test_level where the table has it
        let test = if crate::rd_test::table_columns(&db, &schema, "rppd_cron").await?.contains("test_level") {
            "test_level" } else { "null::int as test_level" };
        let cl = match sqlx::query_as::<_, RpFnCron>(sqlx::AssertSqlSafe(SELECT_CRON.replace("%SCHEMA%", schema.as_str()).replace("%TEST%", test)))
            .fetch_all(&db).await {
            Ok(cl) => { self.cadence_on = true; cl }
            Err(first) => {
                // [cadence] a table from before the cadence columns, which SchemaGuard
                // did not bring up to date at startup (schema.rs): cron runs alone
                warn!(self.log, "{}{}", LP, tr!("cron.cadence_off", first = first));
                self.cadence_on = false;
                sqlx::query_as::<_, RpFnCron>(sqlx::AssertSqlSafe(SELECT_CRON_LEGACY.replace("%SCHEMA%", schema.as_str()).replace("%TEST%", test)))
                    .fetch_all(&db).await.map_err(|e| e.to_string())?
            }
        };

        for (id, msg) in self.schedule(cl, Utc::now()) {
            let r = sqlx::query(sqlx::AssertSqlSafe("update %SCHEMA%.rppd_cron set error_msg = $2 where id = $1".replace("%SCHEMA%", schema.as_str())))
                .bind(id).bind(msg.as_ref()).execute(&db).await;
            if let Err(e) = r {
                match msg {
                    Some(_) => error!(self.log, "{}{}", LP, tr!("cron.store_err", err = e)),
                    None => error!(self.log, "{}{}", LP, tr!("cron.clear_err", err = e)),
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn job(id: i32, cron: &str, error_msg: Option<&str>) -> RpFnCron {
        RpFnCron { id, fn_id: 1, cron: cron.into(), column_name: "c".into(), column_value: None, cadence: false,
            timeout_sec: None, started_at: None, finished_at: None, error_msg: error_msg.map(str::to_string),
            watch_min: 0, retry_sec: 0, watch_q: None, deadline_q: None, state: "ok".into(), last_seen: None,
            since: None, attempt: 0, test_level: None, cadence_rt: None }
    }

    fn ctx() -> CronContext {
        CronContext { crons: BTreeMap::new(), jobs: Default::default(), log: Logger::root(slog::Discard, slog::o!()),
            started: Utc::now(), cadence_on: true, cadence_epoch: 0 }
    }

    fn at(h: u32, m: u32, s: u32) -> CronDTType {
        Utc.with_ymd_and_hms(2026, 11, 3, h, m, s).unwrap()
    }

    /// [own-tables] a job due while the reload ran still fires: an unchanged row keeps its due
    /// time, recomputed from now it would be the next one. A changed cron is due by itself
    #[test]
    fn a_reload_keeps_the_due_time_of_an_unchanged_job() {
        let mut c = ctx();
        c.schedule(vec![job(1, "0 2 * * *", None), job(2, "0 2 * * *", None)], at(1, 0, 0));
        assert_eq!(c.jobs.iter().next().unwrap().0, at(2, 0, 0));
        // reloaded just past 02:00, before the tick that fires it
        c.schedule(vec![job(1, "0 2 * * *", None), job(2, "0 3 * * *", None)], at(2, 0, 1));
        assert!(c.jobs.contains(&(at(2, 0, 0), 1)), "{:?}", c.jobs);
        assert!(c.jobs.contains(&(at(3, 0, 0), 2)), "{:?}", c.jobs);
        assert_eq!(c.jobs.len(), 2);
    }

    /// [own-tables] a job's last error stays, and the job stays scheduled: at a reload it was
    /// cleared and the job not scheduled again. Only a parse error ends at a reload: fixed, it is
    /// cleared; still bad, said once
    #[test]
    fn a_failed_job_stays_scheduled_with_its_error() {
        let mut c = ctx();
        let store = c.schedule(vec![job(1, "0 2 * * *", Some("deadlock detected"))], at(1, 0, 0));
        assert!(store.is_empty(), "{:?}", store);
        assert_eq!(c.jobs.len(), 1);
        assert_eq!(c.crons[&1].error_msg.as_deref(), Some("deadlock detected"));

        let parse = format!("{}parsing cron 2", PARSE_ERROR);
        let store = c.schedule(vec![job(2, "0 2 * * *", Some(parse.as_str()))], at(1, 0, 0));
        assert_eq!(store, vec![(2, None)]);
        assert_eq!(c.crons[&2].error_msg, None);

        let store = c.schedule(vec![job(3, "not a cron", None)], at(1, 0, 0));
        assert_eq!(store.len(), 1);
        assert!(store[0].1.as_deref().unwrap().starts_with(PARSE_ERROR), "{:?}", store);
        assert!(c.jobs.is_empty(), "not scheduled");
        let said = store[0].1.clone();
        let store = c.schedule(vec![job(3, "not a cron", said.as_deref())], at(1, 0, 0));
        assert!(store.is_empty(), "said once: {:?}", store);
    }
}
