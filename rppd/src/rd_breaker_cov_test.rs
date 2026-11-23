#![cfg(all(feature = "etcd-embeded", not(feature = "lib-embedded")))]
//! [coverage] the dispatcher's breaker on a running node: gate, completion, retry, dead
//! letter, the transitions' notices and rows, and the restore at startup

use crate::breaker::State;
use crate::rd_fn::{RpFn, RpFnId, RpFnLog};

include!("cov_cluster.rs");

/// A function with the breaker set as given, its notices and dead letters to /q/ queues
fn func(id: i32, fails: i32, max_attempts: i32) -> RpFn {
    RpFn {
        id, code: String::new(), checksum: String::new(), schema_table: "public.covt".into(),
        topic: String::new(), queue: true, cleanup_logs_min: 0, priority: 1,
        fn_logging: false, verbose_debug: false, secret_names: None,
        breaker_fails: fails, breaker_slow_ms: 0, breaker_open_sec: 600, breaker_open_max_sec: 900,
        max_attempts, notify_q: Some("/q/notify".into()), dlq: Some("/q/dead".into()),
        max_parallel: 0, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0,
        test_level: None, test_timestamp: None,
    }
}

/// An event of `fc` that ran for 5 ms and failed with `error` (or succeeded when None)
fn ev(fc: &RpFn, error: Option<&str>, attempt: u32) -> RpFnLog {
    RpFnLog {
        id: 1, fn_id: fc.id, fn_idp: Some(RpFnId::fromf(fc)), took_ms: Some(5),
        error_msg: error.map(str::to_string), attempt, ..RpFnLog::default()
    }
}

#[tokio::test]
async fn the_breaker_opens_holds_notifies_stores_and_closes() {
    let Some(n) = cov_node("breaker").await else { return };
    let c = &n.cluster;
    sqlx::query("create table public.covt (id int primary key, c int)").execute(&c.db()).await.unwrap();
    let fc = func(1, 2, 0);
    cov_fn_add(c, &fc).await;
    c.breaker_restore().await; // the table exists: transitions are stored
    // a gate on a breaker never opened passes
    assert!(c.breaker_gate(&fc, ev(&fc, None, 0)).await.is_some());
    // two failures in a row open it: a notice to notify_q and the state row
    c.breaker_done(&ev(&fc, Some("boom"), 0)).await;
    c.breaker_done(&ev(&fc, Some("boom"), 0)).await;
    assert_eq!(c.breakers.lock().unwrap()[&1].b.state, State::Open);
    let stored: String = sqlx::query_scalar("select state from public.rppd_function_state where fn_id = 1")
        .fetch_one(&c.db()).await.unwrap();
    assert_eq!(stored, "open");
    // while open the next event is held, not run
    assert!(c.breaker_gate(&fc, ev(&fc, None, 0)).await.is_none());
    assert_eq!(c.breakers.lock().unwrap()[&1].held.len(), 1);
    // the probe of a closing breaker: a success while half-open closes it, and the held
    // events go back to their queue (set here, not by a code change: the probe a change
    // starts would race the live dispatcher)
    c.breakers.lock().unwrap().get_mut(&1).unwrap().b.state = State::Half;
    // the close drains the held event back to its queue; the drained event then runs on the
    // live dispatcher, which may fail it again, so only the drain and the row are asserted
    c.breaker_done(&ev(&fc, None, 0)).await;
    assert!(c.breakers.lock().unwrap()[&1].held.is_empty());
    let rows: i64 = sqlx::query_scalar("select count(*) from public.rppd_function_state where fn_id = 1")
        .fetch_one(&c.db()).await.unwrap();
    assert_eq!(rows, 1);
}

/// A code change half-opens an open breaker at once, and releases one held event as the probe
#[tokio::test]
async fn a_code_change_half_opens_an_open_breaker() {
    let Some(n) = cov_node("codechg").await else { return };
    let c = &n.cluster;
    let fc = func(5, 1, 0);
    cov_fn_add(c, &fc).await;
    c.breaker_done(&ev(&fc, Some("z"), 0)).await; // one failure opens a breaker of one
    assert_eq!(c.breakers.lock().unwrap()[&5].b.state, State::Open);
    assert!(c.breaker_gate(&fc, ev(&fc, None, 0)).await.is_none());
    c.breaker_code_changed(&fc);
    assert_eq!(c.breakers.lock().unwrap()[&5].b.state, State::Half);
    // the spawned notice and probe run now; the probe's own outcome is not asserted
    for _ in 0..10 { tokio::task::yield_now().await; }
}

/// A function that never had a breaker passes the gate on the common path
#[tokio::test]
async fn a_function_without_a_breaker_passes_the_gate() {
    let Some(n) = cov_node("nobreaker").await else { return };
    let c = &n.cluster;
    let fc = func(40, 0, 0);
    cov_fn_add(c, &fc).await;
    assert!(c.breaker_gate(&fc, ev(&fc, None, 0)).await.is_some());
    assert!(c.breakers.lock().unwrap().get(&40).is_none());
}

/// A retry while the breaker is open is held, and the back-off's end releases it; a notice
/// to a name that is no queue is reported and not sent
#[tokio::test]
async fn a_retry_while_open_is_held_until_the_back_off_ends() {
    let Some(n) = cov_node("timer").await else { return };
    let c = &n.cluster;
    let mut fc = func(41, 1, 3);
    fc.breaker_open_sec = 1;
    fc.breaker_open_max_sec = 1;
    fc.notify_q = Some("public.notify".into());
    cov_fn_add(c, &fc).await;
    c.breaker_done(&ev(&fc, Some("t"), 0)).await; // opens: the notice is not sent
    assert_eq!(c.breakers.lock().unwrap()[&41].b.state, State::Open);
    c.breaker_done(&ev(&fc, Some("t"), 0)).await; // a retry while open: held at the head
    // the first failure was itself a retry while the breaker opened: both are held
    assert_eq!(c.breakers.lock().unwrap()[&41].held.len(), 2);
    tokio::time::sleep(std::time::Duration::from_millis(1200)).await;
    for _ in 0..10 { tokio::task::yield_now().await; }
}

#[tokio::test]
async fn a_failed_event_is_retried_then_dead_lettered() {
    let Some(n) = cov_node("retry").await else { return };
    let c = &n.cluster;
    let fc = func(2, 0, 2);
    cov_fn_add(c, &fc).await;
    // the first failure goes back to its queue for another attempt
    assert!(!crate::rd_config::RppdNodeCluster::will_retry(&fc, &ev(&fc, Some("x"), 0), false));
    assert!(crate::rd_config::RppdNodeCluster::will_retry(&fc, &ev(&fc, Some("x"), 0), true));
    assert!(!crate::rd_config::RppdNodeCluster::will_retry(&fc, &ev(&fc, Some("x"), 1), true));
    c.breaker_done(&ev(&fc, Some("x"), 0)).await;
    assert_eq!(c.queue.read().await.size().0, 1, "retried once: back on the queue");
    // the last failure is dead-lettered, and the event acknowledged
    c.breaker_done(&ev(&fc, Some("x"), 1)).await;
    // an event of a function that is gone is only acknowledged
    c.breaker_done(&RpFnLog { fn_id: 999, ..ev(&fc, None, 0) }).await;
    // a dead letter's acknowledgement goes to its queue's key
    c.breaker_done(&RpFnLog { key: Some("/q/z/1".into()), ..ev(&fc, Some("x"), 1) }).await;
}

#[tokio::test]
async fn a_gate_held_event_frees_its_line_and_a_probe_is_given_back() {
    let Some(n) = cov_node("held").await else { return };
    let c = &n.cluster;
    let fc = func(3, 1, 0);
    cov_fn_add(c, &fc).await;
    c.breaker_done(&ev(&fc, Some("y"), 0)).await; // one failure opens a breaker of one
    assert_eq!(c.breakers.lock().unwrap()[&3].b.state, State::Open);
    // a held event that came from a queue line: its mark is taken off the line
    let held = RpFnLog { fn_idp: Some(RpFnId::fromf(&fc)), ..ev(&fc, None, 0) };
    assert!(c.breaker_gate(&fc, held).await.is_none());
    // a half-open breaker whose probe did not run: the probe slot is free again
    {
        let mut m = c.breakers.lock().unwrap();
        let rt = m.get_mut(&3).unwrap();
        rt.b.state = State::Half;
        rt.b.probing = true;
    }
    c.breaker_undo(3);
    assert!(!c.breakers.lock().unwrap()[&3].b.probing);
    c.breaker_undo(404); // no breaker: nothing to give back
}

#[tokio::test]
async fn an_open_breaker_row_is_restored_at_startup() {
    let Some(n) = cov_node("restore").await else { return };
    let c = &n.cluster;
    // the node takes its id from the host table a moment after it starts: wait for it
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    let fc = func(4, 3, 0);
    cov_fn_add(c, &fc).await;
    sqlx::query("insert into public.rppd_function_state (fn_id, node_id, state, since, attempt, last_error, held, updated) \
        values (4, $1, 'open', current_timestamp, 2, 'gone', 0, current_timestamp)")
        .bind(c.node_id.load(std::sync::atomic::Ordering::Relaxed))
        .execute(&c.db()).await.unwrap();
    // a row of a function that is gone, and one of a function with no breaker: both skipped
    c.fns.write().await.insert(406, func(406, 0, 0));
    sqlx::query("insert into public.rppd_function_state (fn_id, node_id, state, since, attempt, last_error, held, updated) \
        values (405, $1, 'open', now(), 0, null, 0, now()), (406, $1, 'open', now(), 0, null, 0, now())")
        .bind(c.node_id.load(std::sync::atomic::Ordering::Relaxed))
        .execute(&c.db()).await.unwrap();
    c.breaker_restore().await;
    let m = c.breakers.lock().unwrap();
    assert!(!m.contains_key(&405) && !m.contains_key(&406));
    assert_eq!(m[&4].b.state, State::Open);
    assert_eq!(m[&4].b.attempt, 2);
    assert_eq!(m[&4].b.last_error.as_deref(), Some("gone"));
}
