//! [coverage] the state names and the messages a cadence sends

use super::*;
use chrono::TimeZone;

fn t(h: u32, m: u32, s: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 7, h, m, s).unwrap()
}

fn pp(cron: &str) -> RpFnCron {
    RpFnCron {
        id: 4, fn_id: 6, cron: cron.into(), column_name: "c".into(), column_value: None, cadence: true,
        timeout_sec: None, started_at: None, finished_at: None, error_msg: None,
        watch_min: 2, retry_sec: 30, watch_q: Some("/q/watch".into()), deadline_q: Some("/q/dead".into()),
        state: "ok".into(), last_seen: None, since: Some(t(9, 0, 0)), attempt: 0, test_level: None,
        cadence_rt: None,
    }
}

fn rt(state: CState, last_seen: Option<DateTime<Utc>>) -> Cadence {
    Cadence { state, since: Some(t(10, 0, 0)), attempt: 2, last_seen, last_sent: None }
}

#[test]
fn a_state_has_its_name() {
    assert_eq!(CState::Ok.as_str(), "ok");
    assert_eq!(CState::Watch.as_str(), "watch");
    assert_eq!(CState::Deadline.as_str(), "deadline");
}

#[test]
fn a_recovered_pp_sends_ok_with_the_state_it_left_and_its_down_time() {
    let c = pp("* * * * *");
    let st = rt(CState::Ok, Some(t(10, 5, 0)));
    let m: serde_json::Value = serde_json::from_str(&body(&c, "/q/x", &st, &Emit::Ok { was: CState::Watch }, t(10, 5, 1), t(9, 0, 0))).unwrap();
    assert_eq!(m["state"], "ok");
    assert_eq!(m["was"], "watch");
    assert_eq!(m["pp"], 4);
    assert_eq!(m["fn_id"], 6);
    assert_eq!(m["queue"], "/q/x");
    assert_eq!(m["down_sec"], 3900, "from the pp's since (9:00) to its last arrival (10:05)");
    assert!(m["last_seen"].is_string());
    // no arrival yet: no down time, no last_seen
    let none = rt(CState::Ok, None);
    let m: serde_json::Value = serde_json::from_str(&body(&c, "/q/x", &none, &Emit::Ok { was: CState::Deadline }, t(10, 5, 1), t(9, 0, 0))).unwrap();
    assert_eq!(m["down_sec"], 0);
    assert!(m["last_seen"].is_null());
    assert_eq!(m["was"], "deadline");
}

#[test]
fn a_late_pp_sends_its_due_time_and_how_late_it_is() {
    let c = pp("*/5 * * * *");
    // last seen 10:00:30: the first full period after it ends at 10:10
    let st = rt(CState::Watch, Some(t(10, 0, 30)));
    let m: serde_json::Value = serde_json::from_str(&body(&c, "/q/x", &st, &Emit::Watch, t(10, 12, 0), t(9, 0, 0))).unwrap();
    assert_eq!(m["state"], "watch");
    assert_eq!(m["attempt"], 2);
    assert_eq!(m["late_sec"], 120, "two minutes past the due time");
    assert_eq!(m["due_at"], "2026-10-07T10:10:00+00:00");
    // no arrival ever: the due time counts from the baseline (the node's start)
    let never = rt(CState::Deadline, None);
    let m: serde_json::Value = serde_json::from_str(&body(&c, "/q/x", &never, &Emit::Deadline, t(9, 0, 1), t(9, 0, 0))).unwrap();
    assert_eq!(m["state"], "deadline");
    assert_eq!(m["due_at"], "2026-10-07T09:10:00+00:00");
    assert_eq!(m["late_sec"], 0, "not yet due: never negative");
}

#[test]
fn an_unparsable_cron_has_no_due_time() {
    let c = pp("not a cron");
    let st = rt(CState::Watch, None);
    let m: serde_json::Value = serde_json::from_str(&body(&c, "/q/x", &st, &Emit::Watch, t(10, 0, 0), t(9, 0, 0))).unwrap();
    assert!(m["due_at"].is_null());
    assert_eq!(m["late_sec"], 0);
}

/// On a running node: arrivals are kept, a late pp is watched with a message, a pp whose
/// function is missing or not a queue is recorded as an error
#[cfg(all(feature = "etcd-embeded", not(feature = "lib-embedded")))]
mod cluster {
    include!("cov_cluster.rs");

    fn func(id: i32, schema_table: &str) -> crate::rd_fn::RpFn {
        crate::rd_fn::RpFn {
            id, code: String::new(), checksum: String::new(), schema_table: schema_table.into(),
            topic: String::new(), queue: true, cleanup_logs_min: 0, priority: 1,
            fn_logging: false, verbose_debug: false, secret_names: None,
            breaker_fails: 0, breaker_slow_ms: 0, breaker_open_sec: 30, breaker_open_max_sec: 900,
            max_attempts: 0, notify_q: None, dlq: None,
            max_parallel: 0, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0,
            test_level: None, test_timestamp: None,
        }
    }

    #[tokio::test]
    async fn cadence_arrivals_watch_and_errors_are_recorded() {
        let Some(n) = cov_node("cadence").await else { return };
        let c = &n.cluster;
        sqlx::query("create table public.covt (id int primary key, c text)").execute(&c.db()).await.unwrap();
        cov_fn_add(c, &func(20, "/q/x")).await;
        cov_fn_add(c, &func(21, "/q/y")).await;
        cov_fn_add(c, &func(22, "public.covt")).await; // not a queue
        // the pps: one kept on time by an arrival, one long overdue, one whose function is gone,
        // one whose function is not a queue
        let rows = [(20, "fresh"), (21, "late"), (21, "watchy"), (99, "gone"), (22, "table")];
        for (fn_id, what) in rows {
            sqlx::query("insert into public.rppd_cron (fn_id, cron, column_name, cadence, watch_min, retry_sec, watch_q, deadline_q, \
                last_seen, since, state, attempt) values ($1, '* * * * *', $2, true, case when $2 = 'watchy' then 30 else 0 end, 0, '/q/watch', '/q/dead', \
                case when $2 in ('late', 'watchy') then '2020-01-01' else null end::timestamptz, now(), 'ok', 0)")
                .bind(fn_id).bind(what).execute(&c.db()).await.unwrap();
        }
        c.reload_cron(&None).await.unwrap();
        c.master.store(true, std::sync::atomic::Ordering::Relaxed);
        c.master_epoch.store(1, std::sync::atomic::Ordering::Relaxed);
        c.cadence_seen("/q/x").await; // the queue of the fresh pp got a message just now
        c.cadence_tick().await;
        let state = |what: &'static str| {
            let db = c.db();
            async move {
                sqlx::query_scalar::<_, String>("select state from public.rppd_cron where column_name = $1")
                    .bind(what).fetch_one(&db).await.unwrap()
            }
        };
        assert_eq!(state("fresh").await, "ok", "an arrival keeps it on time");
        assert_eq!(state("late").await, "deadline", "watch_min 0: deadline at once");
        assert_eq!(state("watchy").await, "watch", "overdue within watch_min: watch, and a message to watch_q");
        let err = |what: &'static str| {
            let db = c.db();
            async move {
                sqlx::query_scalar::<_, Option<String>>("select error_msg from public.rppd_cron where column_name = $1")
                    .bind(what).fetch_one(&db).await.unwrap()
            }
        };
        assert!(err("gone").await.is_some(), "a function that is gone is an error");
        assert!(err("table").await.is_some(), "a function that is no queue is an error");
        assert!(err("fresh").await.is_none());
    }
}
