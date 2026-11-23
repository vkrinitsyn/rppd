//! [coverage] a backlog is not reported when the function sets no max_backlog

use super::*;

#[test]
fn no_max_backlog_means_no_backlog_notice() {
    let c = Cfg { max_parallel: 4, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0 };
    let mut s = Shaper::new(&c);
    assert!(backlog(&mut s, 1_000_000, &c).is_none(), "no limit to pass");
    assert!(!s.backlog_over, "and the state stays as it was");
}
