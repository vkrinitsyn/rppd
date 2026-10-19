//! [shaper] The traffic shaper of a pipeline process (pp = an rppd_function), on
//! the node that runs it (yt-pgxt resilience.md, D5: adaptive).
//!
//! An **adaptive concurrency limit** `L` per pp per node - how many calls of the pp
//! may run at once - follows the function's own latency, which is its downstream's
//! latency as the function sees it:
//!
//! ```text
//! on every completion of the pp (on this node):
//!   failure (as the breaker counts it)       -> L = max(min_parallel, L * 0.5)   at most once per round
//!   took_ms >  target                        -> L = max(min_parallel, L * 0.9)   at most once per round
//!   took_ms <= target and L calls in flight  -> L = min(max_parallel, L + 1/L)   additive increase
//! target = target_ms, or (target_ms = 0) 2 x the 10th percentile of took_ms over the window
//! window = the last 100 completions; a round = ceil(L) completions since the last decrease
//! ```
//!
//! A round - the completions of one generation of calls in flight - is the AIMD
//! convention: the calls already running when the limit came down cannot have seen
//! it, so they must not bring it down again. `max_parallel` is the ceiling `L`
//! climbs back to (0 = no shaping); `rate_per_sec`/`burst` are a hard token-bucket cap
//! on top. Pure, like breaker.rs: the dispatcher asks `ready`, reports to `adapt`,
//! and sends what `notices` returns.

use std::collections::VecDeque;
use chrono::{DateTime, Duration, Utc};
use serde_json::json;

/// The completions the learned target and the throttled notice look at
pub(crate) const WINDOW: usize = 100;

/// The function's settings (rppd_function columns)
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Cfg {
    /// the ceiling L climbs back to; 0 = no shaping
    pub(crate) max_parallel: u32,
    /// the floor (at least 1)
    pub(crate) min_parallel: u32,
    /// the latency target; 0 = learned (2 x p10 of the window)
    pub(crate) target_ms: u32,
    /// a hard cap of calls per second; 0 = none
    pub(crate) rate_per_sec: u32,
    pub(crate) burst: u32,
    /// queued events over this send one 'over' notice (and one 'under'); 0 = none
    pub(crate) max_backlog: u32,
}

impl Cfg {
    pub(crate) fn on(&self) -> bool {
        self.max_parallel > 0 || self.rate_per_sec > 0
    }
    fn floor(&self) -> f64 {
        self.min_parallel.max(1) as f64
    }
    fn ceiling(&self) -> f64 {
        (self.max_parallel as f64).max(self.floor())
    }
}

/// One function's shaper on this node
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Shaper {
    /// the adaptive limit
    pub(crate) limit: f64,
    /// calls running now
    pub(crate) in_flight: u32,
    /// the last WINDOW completions' took_ms
    window: VecDeque<i64>,
    /// completions since the last decrease
    since_decrease: u32,
    /// completions in a row with L at the floor
    at_floor: u32,
    pub(crate) throttled: bool,
    pub(crate) backlog_over: bool,
    /// the token bucket
    tokens: f64,
    refilled: Option<DateTime<Utc>>,
}

impl Shaper {
    pub(crate) fn new(cfg: &Cfg) -> Shaper {
        Shaper { limit: cfg.ceiling(), in_flight: 0, window: VecDeque::with_capacity(WINDOW), since_decrease: u32::MAX,
            at_floor: 0, throttled: false, backlog_over: false, tokens: cfg.burst.max(1) as f64, refilled: None }
    }

    /// The limit as a count of calls: at least one call may always run
    pub(crate) fn slots(&self) -> u32 {
        (self.limit.floor() as u32).max(1)
    }

    /// The latency target: the configured one, or 2 x the 10th percentile of the
    /// window once it has some completions to learn from
    pub(crate) fn target(&self, cfg: &Cfg) -> Option<i64> {
        if cfg.target_ms > 0 {
            return Some(cfg.target_ms as i64);
        }
        if self.window.len() < 10 {
            return None;
        }
        let mut v: Vec<i64> = self.window.iter().cloned().collect();
        v.sort_unstable();
        Some((v[v.len() / 10] * 2).max(1))
    }
}

/// Why a call of the pp may not start now
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Wait {
    /// L calls are running: one has to finish
    Limit,
    /// the rate cap: the next token is due then
    Rate(DateTime<Utc>),
}

fn refill(s: &mut Shaper, now: DateTime<Utc>, cfg: &Cfg) {
    let burst = cfg.burst.max(1) as f64;
    if let Some(last) = s.refilled {
        let secs = (now - last).num_microseconds().unwrap_or(0).max(0) as f64 / 1e6;
        s.tokens = (s.tokens + secs * cfg.rate_per_sec as f64).min(burst);
    }
    s.refilled = Some(now);
}

/// May a call start now? `Ok` takes a slot (and a token); the caller reports its
/// end to `adapt`
pub(crate) fn ready(s: &mut Shaper, now: DateTime<Utc>, cfg: &Cfg) -> Result<(), Wait> {
    if !cfg.on() {
        s.in_flight += 1;
        return Ok(());
    }
    if cfg.max_parallel > 0 && s.in_flight >= s.slots() {
        return Err(Wait::Limit);
    }
    if cfg.rate_per_sec > 0 {
        refill(s, now, cfg);
        if s.tokens < 1.0 {
            let need = (1.0 - s.tokens) / cfg.rate_per_sec as f64;
            return Err(Wait::Rate(now + Duration::microseconds((need * 1e6).ceil() as i64)));
        }
        s.tokens -= 1.0;
    }
    s.in_flight += 1;
    Ok(())
}

/// A transition the shaper reports to notify_q
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum Notice {
    Throttled,
    Recovered,
    BacklogOver,
    BacklogUnder,
}

/// A call of the pp ended: the limit adapts. `failed` is the breaker's verdict (an
/// exception, or slower than breaker_slow_ms)
pub(crate) fn adapt(s: &mut Shaper, failed: bool, took_ms: i64, cfg: &Cfg) -> Option<Notice> {
    let running = s.in_flight;
    s.in_flight = s.in_flight.saturating_sub(1);
    if s.window.len() == WINDOW {
        s.window.pop_front();
    }
    s.window.push_back(took_ms.max(0));
    if cfg.max_parallel == 0 {
        return None;
    }
    s.since_decrease = s.since_decrease.saturating_add(1);
    let round = s.limit.ceil().max(1.0) as u32;
    let may_decrease = s.since_decrease >= round;
    let target = s.target(cfg);
    let (floor, ceiling) = (cfg.floor(), cfg.ceiling());
    if failed {
        if may_decrease {
            s.limit = (s.limit * 0.5).max(floor);
            s.since_decrease = 0;
        }
    } else if target.map(|t| took_ms > t).unwrap_or(false) {
        if may_decrease {
            s.limit = (s.limit * 0.9).max(floor);
            s.since_decrease = 0;
        }
    } else if running >= s.slots() {
        s.limit = (s.limit + 1.0 / s.limit).min(ceiling);
    }
    s.limit = s.limit.clamp(floor, ceiling);
    // throttled: at the floor for a whole window; recovered: above it again
    if s.limit <= floor && ceiling > floor {
        s.at_floor = s.at_floor.saturating_add(1);
        if !s.throttled && s.at_floor as usize >= WINDOW {
            s.throttled = true;
            return Some(Notice::Throttled);
        }
    } else {
        s.at_floor = 0;
        if s.throttled {
            s.throttled = false;
            return Some(Notice::Recovered);
        }
    }
    None
}

/// The pp's queued events on this node: one 'over' when they pass max_backlog, one
/// 'under' when they are back below it
pub(crate) fn backlog(s: &mut Shaper, queued: usize, cfg: &Cfg) -> Option<Notice> {
    if cfg.max_backlog == 0 {
        return None;
    }
    let over = queued > cfg.max_backlog as usize;
    if over != s.backlog_over {
        s.backlog_over = over;
        return Some(if over { Notice::BacklogOver } else { Notice::BacklogUnder });
    }
    None
}

/// The message a notice sends to notify_q (the cadence format)
pub(crate) fn message(pp: i32, node: i32, s: &Shaper, n: Notice, queued: usize, cfg: &Cfg) -> String {
    let p50 = {
        let mut v: Vec<i64> = s.window.iter().cloned().collect();
        v.sort_unstable();
        v.get(v.len() / 2).cloned().unwrap_or(0)
    };
    match n {
        Notice::Throttled | Notice::Recovered => json!({
            "pp": pp, "kind": if n == Notice::Throttled { "throttled" } else { "recovered" }, "node": node,
            "limit": s.slots(), "target_ms": s.target(cfg), "took_ms_p50": p50,
        }).to_string(),
        Notice::BacklogOver | Notice::BacklogUnder => json!({
            "pp": pp, "kind": "backlog", "state": if n == Notice::BacklogOver { "over" } else { "under" },
            "node": node, "backlog": queued,
        }).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(ms: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 13, 12, 0, 0).unwrap() + Duration::milliseconds(ms)
    }
    fn cfg() -> Cfg {
        Cfg { max_parallel: 16, min_parallel: 1, target_ms: 0, rate_per_sec: 0, burst: 1, max_backlog: 0 }
    }
    /// run `n` calls with the latency the downstream gives at L calls in flight
    fn drive(s: &mut Shaper, c: &Cfg, n: usize, latency: impl Fn(u32) -> i64) {
        for _ in 0..n {
            while ready(s, t(0), c).is_ok() {}
            let took = latency(s.in_flight);
            adapt(s, false, took, c);
        }
    }

    #[test]
    fn a_degrading_downstream_brings_the_limit_down_and_a_recovered_one_lets_it_climb_back() {
        let c = Cfg { target_ms: 300, ..cfg() };
        let mut s = Shaper::new(&c);
        assert_eq!(s.slots(), 16);
        // healthy: 100 ms whatever the load - stays at the ceiling
        drive(&mut s, &c, 200, |_| 100);
        assert_eq!(s.slots(), 16);
        // saturating: the more calls run at once, the slower each one (4 is its sweet spot)
        drive(&mut s, &c, 400, |inflight| if inflight > 4 { 100 * inflight as i64 } else { 100 });
        assert!(s.slots() <= 5, "converged down to about what the downstream takes: {}", s.slots());
        assert!(s.slots() >= 1);
        // recovered: back to the ceiling
        drive(&mut s, &c, 2000, |_| 100);
        assert_eq!(s.slots(), 16);
    }

    #[test]
    fn a_learned_target_is_twice_the_tenth_percentile() {
        let c = cfg();
        let mut s = Shaper::new(&c);
        assert_eq!(s.target(&c), None, "nothing to learn from yet");
        drive(&mut s, &c, 9, |_| 100);
        assert_eq!(s.target(&c), None);
        drive(&mut s, &c, 91, |_| 100);
        assert_eq!(s.target(&c), Some(200));
        assert_eq!(s.target(&Cfg { target_ms: 50, ..c }), Some(50), "a configured target wins");
    }

    #[test]
    fn one_slow_outlier_does_not_halve_the_limit() {
        let c = cfg();
        let mut s = Shaper::new(&c);
        drive(&mut s, &c, 200, |_| 100);
        let before = s.limit;
        while ready(&mut s, t(0), &c).is_ok() {}
        adapt(&mut s, false, 5_000, &c);
        assert!(s.limit >= before * 0.9 - 1e-9, "one outlier takes 10% at most: {} -> {}", before, s.limit);
        // and the rest of that round cannot take more
        for _ in 0..5 { adapt(&mut s, false, 5_000, &c); }
        assert!(s.limit >= before * 0.9 - 1e-9);
    }

    #[test]
    fn failures_halve_it_once_per_round_down_to_the_floor() {
        let mut c = cfg();
        c.min_parallel = 2;
        let mut s = Shaper::new(&c);
        drive(&mut s, &c, 50, |_| 100);
        while ready(&mut s, t(0), &c).is_ok() {}
        adapt(&mut s, true, 100, &c);
        assert_eq!(s.slots(), 8);
        adapt(&mut s, true, 100, &c); // same round
        assert_eq!(s.slots(), 8);
        for _ in 0..200 { adapt(&mut s, true, 100, &c); }
        assert_eq!(s.slots(), 2, "never below min_parallel");
    }

    #[test]
    fn the_limit_counts_calls_in_flight() {
        let mut c = cfg();
        c.max_parallel = 2;
        let mut s = Shaper::new(&c);
        assert_eq!(ready(&mut s, t(0), &c), Ok(()));
        assert_eq!(ready(&mut s, t(0), &c), Ok(()));
        assert_eq!(ready(&mut s, t(0), &c), Err(Wait::Limit));
        adapt(&mut s, false, 10, &c);
        assert_eq!(ready(&mut s, t(0), &c), Ok(()));
    }

    #[test]
    fn the_rate_cap_holds_whatever_the_limit() {
        let c = Cfg { max_parallel: 0, min_parallel: 1, target_ms: 0, rate_per_sec: 2, burst: 1, max_backlog: 0 };
        let mut s = Shaper::new(&c);
        assert_eq!(ready(&mut s, t(0), &c), Ok(()));
        match ready(&mut s, t(100), &c) {
            Err(Wait::Rate(at)) => assert_eq!(at, t(500)),
            x => panic!("{:?}", x),
        }
        assert_eq!(ready(&mut s, t(500), &c), Ok(()));
        // 10 s idle refills only up to the burst
        assert_eq!(ready(&mut s, t(10_500), &c), Ok(()));
        assert!(ready(&mut s, t(10_501), &c).is_err());
    }

    #[test]
    fn off_never_waits() {
        let c = Cfg::default();
        let mut s = Shaper::new(&c);
        for _ in 0..1000 { assert_eq!(ready(&mut s, t(0), &c), Ok(())); }
        assert_eq!(adapt(&mut s, true, 99_999, &c), None);
    }

    #[test]
    fn throttled_after_a_window_at_the_floor_then_recovered() {
        let c = Cfg { max_parallel: 4, min_parallel: 1, target_ms: 50, rate_per_sec: 0, burst: 1, max_backlog: 0 };
        let mut s = Shaper::new(&c);
        let mut notices = vec![];
        // ~40 completions to come down from 4 to the floor, then a whole window there
        for _ in 0..WINDOW + 80 {
            while ready(&mut s, t(0), &c).is_ok() {}
            if let Some(n) = adapt(&mut s, false, 500, &c) { notices.push(n); }
        }
        assert_eq!(notices, vec![Notice::Throttled], "once, not per call");
        let m: serde_json::Value = serde_json::from_str(&message(7, 2, &s, Notice::Throttled, 0, &c)).unwrap();
        assert_eq!((m["kind"].as_str(), m["limit"].as_u64(), m["target_ms"].as_i64()), (Some("throttled"), Some(1), Some(50)));
        for _ in 0..50 {
            while ready(&mut s, t(0), &c).is_ok() {}
            if let Some(n) = adapt(&mut s, false, 10, &c) { notices.push(n); }
        }
        assert_eq!(notices, vec![Notice::Throttled, Notice::Recovered]);
    }

    #[test]
    fn backlog_sends_one_over_and_one_under() {
        let c = Cfg { max_backlog: 100, ..cfg() };
        let mut s = Shaper::new(&c);
        assert_eq!(backlog(&mut s, 50, &c), None);
        assert_eq!(backlog(&mut s, 101, &c), Some(Notice::BacklogOver));
        assert_eq!(backlog(&mut s, 5000, &c), None);
        assert_eq!(backlog(&mut s, 99, &c), Some(Notice::BacklogUnder));
        let m: serde_json::Value = serde_json::from_str(&message(7, 2, &s, Notice::BacklogOver, 5000, &c)).unwrap();
        assert_eq!((m["state"].as_str(), m["backlog"].as_u64()), (Some("over"), Some(5000)));
    }
}
