use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use chrono::{DateTime, Utc};
use cron_parser::ParseError;
use shims::Config;
use sqlx::{Pool, Postgres};
use rppd_common::gen::rppc::DbEventRequest;
use crate::arg_config::ArgConfig;
use crate::rd_config::Cluster;

pub(crate) const SELECT_CRON: &str = "select id, fn_id, cron, column_name, column_value, cadence, timeout_sec, started_at, finished_at, error_msg from %SCHEMA%.rppd_cron";
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
}

impl RpFnCron {
    /// load all or reload one or remove one schedules

    async fn update_err(&self, db: &Pool<Postgres>, msg: String, schema: &String) -> Result<(), String> {
        let sql = "update %SCHEMA%.rppd_cron set error_msg = $2 where id = $1";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sql.as_str())
            .bind(self.id)
            .bind(Some(msg))
            .execute(db).await {
            eprintln!("{}", e);
        }

        Ok(())
    }

    async fn update_no_err(&self, db: &Pool<Postgres>, schema: &String) -> Result<(), String> {
        let sql = "update %SCHEMA%.rppd_cron set error_msg = NULL where id = $1";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sql.as_str())
            .bind(self.id)
            .execute(db).await {
            eprintln!("{}", e);
        }

        Ok(())
    }

    async fn start(&mut self, db: &Pool<Postgres>, schema: &String, target: String) -> Option<String> {
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
        if let Err(e) = sqlx::query(sql.as_str())
            .bind(self.id)
            .execute(db).await {
            eprintln!("starting cron job with SQL: {} raise error: {}", sql, e);
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
                    sqlx::query(sql.as_str()).bind(self.id).execute(db).await
                }
                Some(v) => {
                    sql.push_str(" = $2 where id = $2");
                    sqlx::query(sql.as_str())
                        .bind(v)
                        .bind(self.id).execute(db).await
                }
            }
        } {
            Ok(_) => {
                esql.push_str("NULL where id = $1");
                sqlx::query(esql.as_str()).bind(self.id).execute(db).await
            }
            Err(e) => {
                esql.push_str(" = $2 where id = $2");
                sqlx::query(esql.as_str()).bind(Some(e.to_string())).bind(self.id).execute(db).await
            }
        }
        {
            eprintln!("finishing cron job SQL: {}, raise error{}", esql, e);
        }
        self.finished_at = Some(Utc::now());
        Some(self.cron.clone())
    }

}

impl Cluster {
    /// periodic (every second) spawn a job check, call by monitoring loop
    /// the job itself a lightweight trigger to update a target table which will a spawn a new transaction as Python function
    pub(crate) async fn cronjob(&self) {
        if let Ok(mut cron) = self.cron.try_write() {
            let db = self.db();
            let schema = self.cfg.schema.clone();
            let mut jobs = Vec::with_capacity(cron.jobs.len());
            let now = Utc::now();
            for (dt, id) in &cron.jobs {
                if dt < &now {
                     jobs.push((*id, dt.clone()));
                } else {
                    break;
                }
            }
            for (id, dt) in jobs {
                if let Some(crontab) = if let Some(mut c) = cron.crons.get_mut(&id) {
                    let target = match self.fns.read().await.get(&c.fn_id) {
                        None => { continue; }
                        Some(pfn) => pfn.schema_table.clone()
                    };
                    c.start(&db, &schema, target).await
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
        cron.reload(&self.cfg.schema, self.db()).await
    }
}

pub struct CronContext {
    /// loaded cron jobs - only on master node
    pub(crate) crons: BTreeMap<i32, RpFnCron>,
    /// sorted cron jobs
    pub(crate) jobs: BTreeMap<CronDTType, i32>,
}

impl Default for CronContext {
    fn default() -> Self {
        CronContext {
            crons: Default::default(),
            jobs: Default::default(),
        }
    }
}

impl CronContext {

    /// background reloading cron jobs.
    /// cleanup
     #[inline]
   pub(crate) async fn reload(&mut self, schema: &String, db: Pool<Postgres>) -> Result<(), String> {
        let mut cl = sqlx::query_as::<_, RpFnCron>(SELECT_CRON.replace("%SCHEMA%", schema.as_str()).as_str())
            .fetch_all(&db).await.map_err(|e| e.to_string())?;

        self.crons.clear();
        self.jobs.clear();
        for c in cl {
            match cron_parser::parse(c.cron.as_str(), &Utc::now()) {
                Ok(d) => {
                    if c.error_msg.is_some() {
                        let _ = c.update_no_err(&db, &schema);
                    } else {
                        self.jobs.insert(d, c.id);
                        self.crons.insert(c.id, c);
                    }
                }
                Err(e) => {
                    let _ = c.update_err(&db, format!("parsing cron#[{}]: {} error: {}", c.id, c.cron, e), &schema);
                }
            }
        }

        Ok(())
    }
}
