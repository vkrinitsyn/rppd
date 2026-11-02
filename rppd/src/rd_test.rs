//! [test] A function's own `test(level)`, run on a check only, never on an event
//! (README "Test a function"): the code's definitions run - its imports, defs, classes
//! and literal constants - never its body, then `test(level)`. 0 returned is passed;
//! another value, an exception, or no `test(level)` in the code is failed (py.rs
//! `PyContext::test`). A test is no call: no event, nothing acknowledged, no breaker,
//! shaper or cadence state touched.
//!
//! When - always on the master:
//! - on deploy: a function with `test_level` set and `test_timestamp` NULL - a new one,
//!   or one deployed or asked to test again with `test_timestamp = null`. rppd writes
//!   `test_timestamp` once it ran;
//! - by cron: a `rppd_cron` row with `test_level` set runs the test of its function on its
//!   schedule instead of the UPDATE of the target row that fires the function; the row's
//!   `started_at`, `finished_at` and `error_msg` say how it went, as a job's do.
//!
//! Each run is a `rppd_function_log` row with `test_level` set - `took_ms` -1 while it
//! runs (no rppd restores it as an event), then its time, output and error; a cron's
//! names it in `trig_value` (`{"cron": "<id>"}`). One test at a time per rppd instance, on
//! a Python context of its own: made for it on the blocking pool, closed after - one
//! connection more while it runs, none taken from the events.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::Utc;
use slog::{debug, error, info, warn};
use sqlx::{Pool, Postgres};

use crate::rd_config::RppdNodeCluster;
use crate::rd_fn::{test_due, RpFn};
use crate::LP;

/// a test's row: trig_type 4 (`dual`), a call with no table event; took_ms -1 until it ends
const INSERT_TEST: &str = "insert into %SCHEMA%.rppd_function_log (node_id, fn_id, trig_type, test_level, took_ms, trig_value) \
    values ($1, $2, 4, $3, -1, $4) returning id";
const UPDATE_TEST: &str = "update %SCHEMA%.rppd_function_log set took_ms = $1, output = $2, error_msg = $3 where id = $4";
const STAMP_TEST: &str = "update %SCHEMA%.rppd_function set test_timestamp = current_timestamp where id = $1";
const CRON_STARTED: &str = "update %SCHEMA%.rppd_cron set started_at = current_timestamp, finished_at = NULL where id = $1";
const CRON_FINISHED: &str = "update %SCHEMA%.rppd_cron set finished_at = current_timestamp, error_msg = $2 where id = $1";
/// a test running this long is said, once (a cron row's timeout_sec, when it has one)
const SLOW: Duration = Duration::from_secs(60);

/// Who asked for a test
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum TestBy {
    /// a function with its `test_timestamp` NULL
    Deploy,
    /// a `rppd_cron` row, by its id
    Cron(i32),
}

/// The columns `table` of `schema` has: a column rppd.yaml declares may be missing from an
/// install made by the extension's CREATE-only script, or by hand
pub(crate) async fn table_columns(db: &Pool<Postgres>, schema: &str, table: &str) -> HashSet<String> {
    sqlx::query_scalar::<_, String>("select column_name::text from information_schema.columns \
        where table_schema = $1 and table_name = $2")
        .bind(schema).bind(table).fetch_all(db).await.unwrap_or_default().into_iter().collect()
}

impl RppdNodeCluster {
    /// [test] The master's tick, with the cron jobs (rd_monitor.rs): each function whose
    /// test is due - and is not being tested now - runs it, marked done here so it is not
    /// due again before its row says so
    pub(crate) async fn test_tick(&self) {
        if !self.master.load(Ordering::Relaxed) {
            return;
        }
        let busy = self.testing.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if !self.fns.read().await.values().any(|f| test_due(f).is_some() && !busy.contains(&f.id)) {
            return;
        }
        let due: Vec<(RpFn, i32)> = {
            let mut fns = self.fns.write().await;
            fns.values_mut().filter(|f| !busy.contains(&f.id)).filter_map(|f| {
                let level = test_due(f)?;
                f.test_timestamp = Some(Utc::now());
                Some((f.clone(), level))
            }).collect()
        };
        for (fc, level) in due {
            let me = self.clone();
            tokio::spawn(async move {
                let _ = me.test_fn(fc, level, TestBy::Deploy, SLOW).await;
            });
        }
    }

    /// [test] A cron row's test: the row marked started, the test of its function, the row
    /// marked finished with the test's error, if any - as a job's row is
    pub(crate) async fn test_cron(&self, fc: RpFn, level: i32, cron: i32, timeout_sec: Option<i32>) {
        let schema = self.cfg.read().await.schema.clone();
        let db = self.db();
        let slow = timeout_sec.filter(|t| *t > 0).map_or(SLOW, |t| Duration::from_secs(t as u64));
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(CRON_STARTED.replace("%SCHEMA%", &schema))).bind(cron).execute(&db).await {
            error!(self.log, "{}{}", LP, tr!("test.cron_row", id = cron, err = e));
        }
        let err = match self.test_fn(fc, level, TestBy::Cron(cron), slow).await {
            None => return, // not run: its row stays as it was
            Some(r) => r.err(),
        };
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(CRON_FINISHED.replace("%SCHEMA%", &schema)))
            .bind(cron).bind(err).execute(&db).await {
            error!(self.log, "{}{}", LP, tr!("test.cron_row", id = cron, err = e));
        }
    }

    /// [test] The test of `fc` at `level`: None when it is not run - the function is being
    /// tested already - else why it failed, if it did. One test at a time on this instance.
    pub(crate) async fn test_fn(&self, fc: RpFn, level: i32, by: TestBy, slow: Duration) -> Option<Result<(), String>> {
        if !self.testing.lock().unwrap_or_else(|e| e.into_inner()).insert(fc.id) {
            info!(self.log, "{}{}", LP, tr!("test.busy", id = fc.id));
            return None;
        }
        let r = {
            let _slot = self.test_slots.acquire().await;
            self.test_run(&fc, level, by, slow).await
        };
        self.testing.lock().unwrap_or_else(|e| e.into_inner()).remove(&fc.id);
        Some(r)
    }

    async fn test_run(&self, fc: &RpFn, level: i32, by: TestBy, slow: Duration) -> Result<(), String> {
        let schema = self.cfg.read().await.schema.clone();
        let db = self.db();
        match by {
            TestBy::Deploy => info!(self.log, "{}{}", LP, tr!("test.deploy", id = fc.id, level = level)),
            TestBy::Cron(c) => debug!(self.log, "{}{}", LP, tr_lazy!("test.cron", id = fc.id, level = level, cron = c)),
        }
        // its log row: a test, no event
        let origin = match by {
            TestBy::Cron(c) => Some(sqlx::types::Json(HashMap::from([("cron".to_string(), c.to_string())]))),
            TestBy::Deploy => None,
        };
        let row = if table_columns(&db, &schema, "rppd_function_log").await.contains("test_level") {
            match sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(INSERT_TEST.replace("%SCHEMA%", &schema)))
                .bind(self.node_id.load(Ordering::Relaxed)).bind(fc.id).bind(level).bind(origin)
                .fetch_one(&db).await {
                Ok(id) => Some(id),
                Err(e) => {
                    error!(self.log, "{}{}", LP, tr!("test.log", id = fc.id, err = e));
                    None
                }
            }
        } else {
            None
        };
        let started = Instant::now();
        // the context's connections are made synchronously (psycopg2, the clients): on a
        // thread of the runtime's own, not one of its workers
        let ctx = tokio::task::block_in_place(|| tokio::runtime::Handle::current().block_on(self.new_py_context(fc)));
        let (outcome, output) = match ctx {
            Ok(mut ctx) => {
                let f = fc.clone();
                let mut job = tokio::task::spawn_blocking(move || {
                    let r = ctx.test(&f, level);
                    ctx.close();
                    r
                });
                let r = tokio::select! {
                    r = &mut job => r,
                    _ = tokio::time::sleep(slow) => {
                        warn!(self.log, "{}{}", LP, tr!("test.slow", id = fc.id, level = level, sec = slow.as_secs()));
                        (&mut job).await
                    }
                };
                r.unwrap_or_else(|e| (Err(e.to_string()), String::new()))
            }
            Err(e) => (Err(tr!("test.context", id = fc.id, err = e)), String::new()),
        };
        let took = started.elapsed().as_millis() as i64;
        if let Some(id) = row {
            if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(UPDATE_TEST.replace("%SCHEMA%", &schema)))
                .bind(took).bind(Some(output.as_str()).filter(|o| !o.is_empty())).bind(outcome.as_ref().err())
                .bind(id).execute(&db).await {
                error!(self.log, "{}{}", LP, tr!("test.log", id = fc.id, err = e));
            }
        }
        if by == TestBy::Deploy && table_columns(&db, &schema, "rppd_function").await.contains("test_timestamp") {
            if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(STAMP_TEST.replace("%SCHEMA%", &schema)))
                .bind(fc.id).execute(&db).await {
                error!(self.log, "{}{}", LP, tr!("test.stamp", id = fc.id, err = e));
            }
        }
        match &outcome {
            Ok(()) => info!(self.log, "{}{}", LP, tr!("test.passed", id = fc.id, level = level, ms = took)),
            Err(e) => error!(self.log, "{}{}", LP, tr!("test.failed", id = fc.id, level = level, err = e)),
        }
        outcome
    }
}
