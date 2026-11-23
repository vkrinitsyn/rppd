//! [coverage] the cron jobs: the schedule of loaded rows (a cadence row is not a job, a
//! reload keeps a cadence runtime, a bad cron is stored once), a new master term, and on a
//! running node the reload, a due job's run and its start

use super::*;
use chrono::TimeZone;

fn at(h: u32, m: u32, s: u32) -> CronDTType {
    Utc.with_ymd_and_hms(2026, 11, 3, h, m, s).unwrap()
}

fn row(id: i32, cron: &str, cadence: bool) -> RpFnCron {
    RpFnCron { id, fn_id: 1, cron: cron.into(), column_name: "c".into(), column_value: None, cadence,
        timeout_sec: None, started_at: None, finished_at: None, error_msg: None,
        watch_min: 0, retry_sec: 0, watch_q: Some("/q/w".into()), deadline_q: None, state: "ok".into(),
        last_seen: None, since: None, attempt: 0, test_level: None, cadence_rt: None }
}

fn ctx() -> CronContext {
    CronContext { crons: BTreeMap::new(), jobs: Default::default(), log: slog::Logger::root(slog::Discard, slog::o!()),
        started: Utc::now(), cadence_on: false, cadence_epoch: 0 }
}

#[test]
fn a_new_master_term_drops_the_runtimes_once() {
    let mut c = ctx();
    let mut cad = row(3, "* * * * *", true);
    cad.cadence_rt = Some(Cadence { state: crate::cadence::CState::Watch, since: None, attempt: 1, last_seen: None, last_sent: None });
    c.crons.insert(3, cad);
    assert!(!c.new_term(0), "the term the context was made in is no new one");
    assert!(c.new_term(2), "a new term began");
    assert!(c.crons[&3].cadence_rt.is_none(), "the runtime of an earlier term is dropped");
    assert!(!c.new_term(2), "the same term again is no news");
}

#[test]
fn a_cadence_row_is_not_a_job_and_an_unchanged_one_keeps_its_runtime() {
    let mut c = ctx();
    let store = c.schedule(vec![row(5, "* * * * *", true), row(6, "0 2 * * *", false)], at(1, 0, 0));
    assert!(store.is_empty());
    assert_eq!(c.jobs.len(), 1, "only the plain cron is a job: {:?}", c.jobs);
    assert!(c.crons.contains_key(&5) && c.crons[&5].cadence);
    // reloaded unchanged: the runtime it had carries over
    let mut kept = row(5, "* * * * *", true);
    kept.cadence_rt = Some(Cadence { state: crate::cadence::CState::Ok, since: None, attempt: 0, last_seen: None, last_sent: None });
    c.crons.get_mut(&5).unwrap().cadence_rt = kept.cadence_rt.clone();
    c.schedule(vec![kept], at(1, 0, 1));
    assert!(c.crons[&5].cadence_rt.is_some(), "an unchanged cadence keeps its runtime");
    // changed: the runtime starts again
    c.schedule(vec![row(5, "*/5 * * * *", true)], at(1, 0, 2));
    assert!(c.crons[&5].cadence_rt.is_none(), "a changed cadence starts again");
}

#[test]
fn a_bad_cron_is_stored_once_and_not_scheduled() {
    let mut c = ctx();
    let store = c.schedule(vec![row(8, "not a cron", false)], at(1, 0, 0));
    assert_eq!(store.len(), 1);
    assert!(c.jobs.is_empty());
    let said = store[0].1.clone();
    let mut again = row(8, "not a cron", false);
    again.error_msg = said;
    assert!(c.schedule(vec![again], at(1, 0, 0)).is_empty(), "said once");
}

/// On a running node: the rows loaded and scheduled, a due job's run, a start
#[cfg(all(feature = "etcd-embeded", not(feature = "lib-embedded")))]
mod cluster {
    use super::*;

    include!("cov_cluster.rs");

    fn func(id: i32) -> crate::rd_fn::RpFn {
        crate::rd_fn::RpFn {
            id, code: String::new(), checksum: String::new(), schema_table: "public.covt".into(),
            topic: String::new(), queue: true, cleanup_logs_min: 0, priority: 1,
            fn_logging: false, verbose_debug: false, secret_names: None,
            breaker_fails: 0, breaker_slow_ms: 0, breaker_open_sec: 30, breaker_open_max_sec: 900,
            max_attempts: 0, notify_q: None, dlq: None,
            max_parallel: 0, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0,
            test_level: None, test_timestamp: None,
        }
    }

    #[tokio::test]
    async fn cron_rows_are_loaded_run_when_due_and_started_once() {
        let Some(n) = cov_node("cron").await else { return };
        let c = &n.cluster;
        sqlx::query("create table public.covt (id int primary key, c text)").execute(&c.db()).await.unwrap();
        cov_fn_add(c, &func(10)).await;
        sqlx::query("insert into public.rppd_cron (fn_id, cron, column_name, column_value) values (10, '0 2 * * *', 'c', '5')")
            .execute(&c.db()).await.unwrap();
        // a row whose cron does not parse: its error is stored
        sqlx::query("insert into public.rppd_cron (fn_id, cron, column_name) values (10, 'not a cron', 'c')")
            .execute(&c.db()).await.unwrap();
        c.reload_cron(&None).await.unwrap();
        let (id, bad): (i32, i32) = (
            sqlx::query_scalar("select id from public.rppd_cron where cron = '0 2 * * *'").fetch_one(&c.db()).await.unwrap(),
            sqlx::query_scalar("select id from public.rppd_cron where cron = 'not a cron'").fetch_one(&c.db()).await.unwrap(),
        );
        let stored: Option<String> = sqlx::query_scalar("select error_msg from public.rppd_cron where id = $1")
            .bind(bad).fetch_one(&c.db()).await.unwrap();
        assert!(stored.is_some(), "the bad cron's error is stored");
        assert_eq!(c.cron.read().await.jobs.iter().filter(|(_, j)| *j == id).count(), 1, "the good one is scheduled");
        // a job due in the past runs on the master: its column is set, and it is due again
        sqlx::query("insert into public.covt (id, c) values ($1, '0')").bind(id).execute(&c.db()).await.unwrap();
        c.master.store(true, std::sync::atomic::Ordering::Relaxed);
        {
            let mut cr = c.cron.write().await;
            cr.jobs.retain(|(_, j)| *j != id);
            cr.jobs.insert((at(1, 0, 0), id));
        }
        c.cronjob().await;
        let value: String = sqlx::query_scalar("select c from public.covt where id = $1").bind(id).fetch_one(&c.db()).await.unwrap();
        assert_eq!(value, "5", "the job set its column to its value");
        assert!(c.cron.read().await.jobs.iter().any(|(d, j)| *j == id && *d > Utc::now()), "due again");
        // a start with a timeout still running is not started again
        let mut job = c.cron.read().await.crons[&id].clone();
        // a job without a value sets its column to NULL; a target that is missing is an error
        // stored with the job's finish
        sqlx::query("insert into public.rppd_cron (fn_id, cron, column_name) values (10, '0 6 * * *', 'c')")
            .execute(&c.db()).await.unwrap();
        sqlx::query("insert into public.rppd_cron (fn_id, cron, column_name, column_value, test_level) \
            values (10, '0 7 * * *', 'c', '1', 0), (77, '0 8 * * *', 'c', '1', null)")
            .execute(&c.db()).await.unwrap();
        c.reload_cron(&None).await.unwrap();
        let null_id: i32 = sqlx::query_scalar("select id from public.rppd_cron where cron = '0 6 * * *'").fetch_one(&c.db()).await.unwrap();
        let log = slog::Logger::root(slog::Discard, slog::o!());
        let mut nul = c.cron.read().await.crons[&null_id].clone();
        assert!(nul.start(&c.db(), "public", "public.covt".into(), &log).await.is_some());
        let missing = nul.start(&c.db(), "public", "public.nosuch".into(), &log).await;
        assert!(missing.is_some(), "a failed run is still a run");
        let err: Option<String> = sqlx::query_scalar("select error_msg from public.rppd_cron where id = $1")
            .bind(null_id).fetch_one(&c.db()).await.unwrap();
        assert!(err.is_some(), "the failure is stored");
        // due jobs: one with a test level runs its test, one whose function is gone is skipped
        let test_id: i32 = sqlx::query_scalar("select id from public.rppd_cron where cron = '0 7 * * *'").fetch_one(&c.db()).await.unwrap();
        let gone_id: i32 = sqlx::query_scalar("select id from public.rppd_cron where cron = '0 8 * * *'").fetch_one(&c.db()).await.unwrap();
        {
            let mut cr = c.cron.write().await;
            cr.jobs.insert((at(1, 0, 0), test_id));
            cr.jobs.insert((at(1, 0, 0), gone_id));
        }
        c.cronjob().await;
        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
        let log = slog::Logger::root(slog::Discard, slog::o!());
        assert!(job.start(&c.db(), "public", "public.covt".into(), &log).await.is_some());
        job.timeout_sec = Some(3600);
        assert!(job.start(&c.db(), "public", "public.covt".into(), &log).await.is_none(), "still within its timeout");
        // a non-master leaves the due jobs to the master: they are moved to their next time
        c.master.store(false, std::sync::atomic::Ordering::Relaxed);
        c.cron.write().await.jobs.retain(|(_, j)| *j != id);
        c.cron.write().await.jobs.insert((at(1, 0, 0), id));
        c.cronjob().await;
        assert!(c.cron.read().await.jobs.iter().any(|(d, j)| *j == id && *d > Utc::now()), "moved on, not run");
    }
}
