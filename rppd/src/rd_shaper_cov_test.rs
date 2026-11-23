#![cfg(all(feature = "etcd-embeded", not(feature = "lib-embedded")))]
//! [coverage] the dispatcher's shaper on a running node: the gate parks what may not start,
//! a backlog is reported, a slot given back is undone, the rate cap wakes later

use crate::rd_fn::{RpFn, RpFnId, RpFnLog};

include!("cov_cluster.rs");

/// A function with the shaper set as given; its notices go to /q/notify
fn func(id: i32, max_parallel: i32, rate_per_sec: i32, max_backlog: i32) -> RpFn {
    RpFn {
        id, code: String::new(), checksum: String::new(), schema_table: "public.covt".into(),
        topic: String::new(), queue: true, cleanup_logs_min: 0, priority: 1,
        fn_logging: false, verbose_debug: false, secret_names: None,
        breaker_fails: 0, breaker_slow_ms: 0, breaker_open_sec: 30, breaker_open_max_sec: 900,
        max_attempts: 0, notify_q: Some("/q/notify".into()), dlq: None,
        max_parallel, min_parallel: 1, target_ms: 0, rate_per_sec, burst: 1, max_backlog,
        test_level: None, test_timestamp: None,
    }
}

fn call(fc: &RpFn, took_ms: i64) -> RpFnLog {
    RpFnLog { id: 1, fn_id: fc.id, fn_idp: Some(RpFnId::fromf(fc)), took_ms: Some(took_ms), ..RpFnLog::default() }
}

#[tokio::test]
async fn a_call_over_the_limit_waits_and_a_backlog_is_reported() {
    let Some(n) = cov_node("shaper").await else { return };
    let c = &n.cluster;
    c.breaker_restore().await; // the table exists: a limit is stored
    let fc = func(7, 1, 0, 1);
    cov_fn_add(c, &fc).await;
    // one slot: the first call starts, the next two wait, in order
    assert!(c.shaper_gate(&fc, call(&fc, 0)).await.is_some());
    assert!(c.shaper_gate(&fc, call(&fc, 0)).await.is_none());
    assert!(c.shaper_gate(&fc, call(&fc, 0)).await.is_none());
    assert_eq!(c.shapers.lock().unwrap()[&7].waiting.len(), 2);
    // a call that is already admitted passes the gate without a slot
    let admitted = RpFnLog { shaped: true, ..call(&fc, 0) };
    assert!(c.shaper_gate(&fc, admitted).await.is_some());
    // a function with no shaping and no shaper passes at once
    let plain = func(8, 0, 0, 0);
    assert!(c.shaper_gate(&plain, call(&plain, 0)).await.is_some());
    // the first call ends: its slot goes to the next waiting call, the limit is stored
    c.shaper_done(&call(&fc, 5)).await;
}

#[tokio::test]
async fn the_rate_cap_parks_a_call_until_its_token_and_a_slot_given_back_is_undone() {
    let Some(n) = cov_node("rate").await else { return };
    let c = &n.cluster;
    let fc = func(9, 0, 2, 0);
    cov_fn_add(c, &fc).await;
    // one token, taken at the gate; the next call waits for the rate
    let mut first = call(&fc, 0);
    first.granted = None;
    assert!(c.shaper_gate(&fc, first).await.is_some());
    assert!(c.shaper_gate(&fc, call(&fc, 0)).await.is_none());
    // a started call dates its token; an undone one gives its slot back
    c.shaper_started(&fc, &RpFnLog { granted: Some(std::time::Instant::now()), ..call(&fc, 0) });
    c.shaper_undo(&fc).await;
    assert_eq!(c.shapers.lock().unwrap()[&9].waiting.len(), 1);
    // a call that did not start has no token to date; a function with no rate cap has none to keep
    c.shaper_started(&fc, &RpFnLog { granted: None, ..call(&fc, 0) });
    let fast = func(12, 1, 0, 0);
    cov_fn_add(c, &fast).await;
    c.shaper_started(&fast, &RpFnLog { granted: Some(std::time::Instant::now()), ..call(&fast, 0) });
    // a call that ended releases waiting calls the limit allows
    c.shaper_done(&call(&fc, 5)).await;
    // the wait for the next token ends: the parked call is released by its timer
    tokio::time::sleep(std::time::Duration::from_millis(700)).await;
}
