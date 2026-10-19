//! [breaker] The circuit breaker of a pipeline process (pp = an rppd_function), on
//! the node that runs it (yt-pgxt resilience.md, D2).
//!
//! ```text
//!            breaker_fails failures in a row
//!            (a Python exception, or took_ms > breaker_slow_ms when set)
//!  CLOSED ─────────────────────────────────────────────▶ OPEN ── 'open' to notify_q
//!    ▲                                                    │ breaker_open_sec, doubling per
//!    │ probe succeeds: 'closed', the held events drain    ▼ re-open up to breaker_open_max_sec
//!    └──────────────────────────────────────────────  HALF-OPEN: ONE event goes through
//!                         probe fails ──▶ OPEN (attempt + 1)
//! ```
//!
//! Pure, in the style of cadence.rs: no I/O, so every transition is unit-tested.
//! The dispatcher (rd_config.rs) asks `admit` before it runs an event and holds the
//! event when the answer is no, reports each completion to `step`, and sends,
//! stores and drains on what they return. A code change (`CodeChanged`, the
//! operator's fix) moves an open breaker straight to half-open, so the fix is tried
//! at once rather than after the back-off.

use chrono::{DateTime, Duration, Utc};
use serde_json::json;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Closed,
    Open,
    Half,
}

impl State {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            State::Closed => "closed",
            State::Open => "open",
            State::Half => "half",
        }
    }

    pub(crate) fn parse(s: &str) -> State {
        match s.trim() {
            "open" => State::Open,
            "half" => State::Half,
            _ => State::Closed,
        }
    }
}

/// The function's settings (rppd_function columns)
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub(crate) struct Cfg {
    /// failures in a row that open it; 0 = no breaker
    pub(crate) fails: u32,
    /// a call slower than this counts as failed; 0 = off
    pub(crate) slow_ms: u32,
    pub(crate) open_sec: u32,
    pub(crate) open_max_sec: u32,
}

impl Cfg {
    pub(crate) fn on(&self) -> bool {
        self.fails > 0
    }
}

/// One function's breaker on this node
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Breaker {
    pub(crate) state: State,
    /// failures in a row while closed
    pub(crate) fails: u32,
    /// the current state's start
    pub(crate) since: DateTime<Utc>,
    /// open: when the back-off ends and one probe may go
    pub(crate) until: Option<DateTime<Utc>>,
    /// re-opens since the last close: the back-off doubles with each
    pub(crate) attempt: u32,
    /// half-open: the one event is in flight
    pub(crate) probing: bool,
    pub(crate) last_error: Option<String>,
    /// when it opened, for the down time a close reports
    pub(crate) opened_at: Option<DateTime<Utc>>,
}

impl Breaker {
    pub(crate) fn new(now: DateTime<Utc>) -> Breaker {
        Breaker { state: State::Closed, fails: 0, since: now, until: None, attempt: 0,
            probing: false, last_error: None, opened_at: None }
    }

    /// A breaker read back from its rppd_function_state row: an open one stays open
    /// until its back-off ends; a half-open one had a probe in flight that died with
    /// the process, so it probes again at once
    pub(crate) fn restored(state: State, since: DateTime<Utc>, attempt: u32, last_error: Option<String>,
                           cfg: &Cfg) -> Breaker {
        let mut b = Breaker::new(since);
        b.last_error = last_error;
        match state {
            State::Closed => {}
            State::Open | State::Half => {
                b.state = State::Open;
                b.attempt = attempt.max(1);
                b.opened_at = Some(since);
                b.until = Some(if state == State::Half { since } else { since + backoff(b.attempt, cfg) });
            }
        }
        b
    }
}

/// What a completed invocation reports, or a change of the code
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Event {
    Done { failed: bool, error: Option<String> },
    CodeChanged,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Transition {
    Opened,
    HalfOpened,
    Closed,
}

/// Did this call fail, as the breaker counts it: an exception, or slower than
/// `breaker_slow_ms` when that is set
pub(crate) fn failed(ok: bool, took_ms: i64, cfg: &Cfg) -> bool {
    !ok || (cfg.slow_ms > 0 && took_ms > cfg.slow_ms as i64)
}

/// The back-off of the attempt-th opening: open_sec, doubling, up to open_max_sec
pub(crate) fn backoff(attempt: u32, cfg: &Cfg) -> Duration {
    let base = cfg.open_sec.max(1) as i64;
    let max = (cfg.open_max_sec as i64).max(base);
    let shift = attempt.saturating_sub(1).min(30);
    Duration::seconds(base.saturating_mul(1i64 << shift).min(max))
}

fn open(b: &mut Breaker, now: DateTime<Utc>, attempt: u32, cfg: &Cfg) {
    if b.state == State::Closed {
        b.opened_at = Some(now);
    }
    b.state = State::Open;
    b.attempt = attempt;
    b.since = now;
    b.until = Some(now + backoff(attempt, cfg));
    b.probing = false;
}

fn close(b: &mut Breaker, now: DateTime<Utc>) {
    b.state = State::Closed;
    b.fails = 0;
    b.attempt = 0;
    b.since = now;
    b.until = None;
    b.probing = false;
}

/// May an event run now? Closed: yes. Open: no until its back-off ends - then it
/// is half-open and this event is the probe. Half-open: only while no probe is in
/// flight. A breaker turned off (breaker_fails = 0) while not closed closes.
pub(crate) fn admit(b: &mut Breaker, now: DateTime<Utc>, cfg: &Cfg) -> (bool, Option<Transition>) {
    if !cfg.on() {
        return if b.state == State::Closed { (true, None) } else {
            close(b, now);
            (true, Some(Transition::Closed))
        };
    }
    match b.state {
        State::Closed => (true, None),
        State::Open => {
            if b.until.map(|u| now >= u).unwrap_or(true) {
                b.state = State::Half;
                b.since = now;
                b.probing = true;
                (true, Some(Transition::HalfOpened))
            } else {
                (false, None)
            }
        }
        State::Half => {
            if b.probing { (false, None) } else {
                b.probing = true;
                (true, None)
            }
        }
    }
}

/// A completion or a code change: the new state, and the transition when there
/// was one
pub(crate) fn step(b: &mut Breaker, ev: Event, now: DateTime<Utc>, cfg: &Cfg) -> Option<Transition> {
    match ev {
        Event::CodeChanged => match b.state {
            State::Closed => { b.fails = 0; None }
            // the operator's fix is tried now, not after the back-off
            State::Open => {
                b.state = State::Half;
                b.since = now;
                b.probing = false;
                Some(Transition::HalfOpened)
            }
            State::Half => None,
        },
        Event::Done { failed, error } => {
            if failed {
                b.last_error = error.or_else(|| b.last_error.take());
            }
            if !cfg.on() {
                return None;
            }
            match b.state {
                State::Closed => {
                    if failed {
                        b.fails += 1;
                        if b.fails >= cfg.fails {
                            open(b, now, 1, cfg);
                            return Some(Transition::Opened);
                        }
                    } else {
                        b.fails = 0;
                    }
                    None
                }
                State::Half => {
                    if failed {
                        let attempt = b.attempt + 1;
                        open(b, now, attempt, cfg);
                        Some(Transition::Opened)
                    } else {
                        close(b, now);
                        Some(Transition::Closed)
                    }
                }
                // a call that was already running when it opened: counted in
                // nothing, its error kept
                State::Open => None,
            }
        }
    }
}

/// The message a transition sends to `notify_q` (the cadence format: JSON on a
/// producer key)
pub(crate) fn message(pp: i32, node: i32, b: &Breaker, t: Transition, held: usize, now: DateTime<Utc>) -> String {
    match t {
        Transition::Opened => json!({
            "pp": pp, "kind": "breaker", "state": "open", "node": node, "fails": b.fails,
            "error": b.last_error, "attempt": b.attempt, "retry_at": b.until.map(|u| u.to_rfc3339()),
        }).to_string(),
        Transition::HalfOpened => json!({
            "pp": pp, "kind": "breaker", "state": "half", "node": node, "attempt": b.attempt, "held": held,
        }).to_string(),
        Transition::Closed => json!({
            "pp": pp, "kind": "breaker", "state": "closed", "node": node,
            "down_sec": b.opened_at.map(|o| (now - o).num_seconds().max(0)).unwrap_or(0), "held": held,
        }).to_string(),
    }
}

/// The message a dead-lettered event sends to `dlq`
pub(crate) fn dead_message(pp: i32, node: i32, key: &Option<String>, value: &Option<Vec<u8>>,
                           trig: Option<String>, error: &Option<String>, attempts: u32) -> String {
    json!({
        "pp": pp, "kind": "dead", "node": node, "key": key,
        "value": value.as_ref().map(|v| String::from_utf8_lossy(v).to_string()),
        "trig_value": trig, "error": error, "attempts": attempts,
    }).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(s: i64) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 10, 13, 12, 0, 0).unwrap() + Duration::seconds(s)
    }
    fn cfg() -> Cfg {
        Cfg { fails: 3, slow_ms: 0, open_sec: 30, open_max_sec: 100 }
    }
    fn fail(e: &str) -> Event {
        Event::Done { failed: true, error: Some(e.to_string()) }
    }
    fn ok() -> Event {
        Event::Done { failed: false, error: None }
    }

    #[test]
    fn opens_after_breaker_fails_in_a_row_and_a_success_resets_the_count() {
        let c = cfg();
        let mut b = Breaker::new(t(0));
        assert_eq!(step(&mut b, fail("e1"), t(1), &c), None);
        assert_eq!(step(&mut b, fail("e2"), t(2), &c), None);
        assert_eq!(step(&mut b, ok(), t(3), &c), None);
        assert_eq!(b.fails, 0, "not in a row");
        step(&mut b, fail("a"), t(4), &c);
        step(&mut b, fail("b"), t(5), &c);
        assert_eq!(step(&mut b, fail("c"), t(6), &c), Some(Transition::Opened));
        assert_eq!(b.state, State::Open);
        assert_eq!(b.until, Some(t(36)));
        assert_eq!(b.last_error.as_deref(), Some("c"));
        let m: serde_json::Value = serde_json::from_str(&message(7, 2, &b, Transition::Opened, 0, t(6))).unwrap();
        assert_eq!(m["state"], "open");
        assert_eq!(m["fails"], 3);
        assert_eq!(m["error"], "c");
        assert_eq!(m["node"], 2);
    }

    #[test]
    fn open_holds_until_the_back_off_then_lets_exactly_one_probe_through() {
        let c = cfg();
        let mut b = Breaker::new(t(0));
        for i in 0..3 { step(&mut b, fail("x"), t(i), &c); }
        assert_eq!(admit(&mut b, t(10), &c), (false, None));
        assert_eq!(admit(&mut b, t(32), &c), (true, Some(Transition::HalfOpened)));
        assert_eq!(b.state, State::Half);
        // the probe is in flight: nothing else goes
        assert_eq!(admit(&mut b, t(33), &c), (false, None));
        // the probe succeeds: closed, and the next events go
        assert_eq!(step(&mut b, ok(), t(34), &c), Some(Transition::Closed));
        assert_eq!(admit(&mut b, t(35), &c), (true, None));
        let m: serde_json::Value = serde_json::from_str(&message(7, 2, &b, Transition::Closed, 52, t(34))).unwrap();
        assert_eq!(m["down_sec"], 32);
        assert_eq!(m["held"], 52);
    }

    #[test]
    fn a_failed_probe_reopens_with_a_doubled_back_off_up_to_the_max() {
        let c = cfg();
        let mut b = Breaker::new(t(0));
        for i in 0..3 { step(&mut b, fail("x"), t(i), &c); }
        let mut now = 2;
        let mut want = vec![60, 100, 100]; // 30 -> 60 -> 120 capped at 100
        want.reverse();
        for _ in 0..3 {
            now = (b.until.unwrap() - t(0)).num_seconds();
            assert_eq!(admit(&mut b, t(now), &c).0, true);
            assert_eq!(step(&mut b, fail("still"), t(now), &c), Some(Transition::Opened));
            assert_eq!((b.until.unwrap() - t(now)).num_seconds(), want.pop().unwrap());
        }
        assert_eq!(b.attempt, 4);
        let _ = now;
    }

    #[test]
    fn a_slow_call_counts_as_failed_only_when_breaker_slow_ms_is_set() {
        let mut c = cfg();
        assert!(!failed(true, 5000, &c));
        c.slow_ms = 800;
        assert!(failed(true, 801, &c));
        assert!(!failed(true, 800, &c));
        assert!(failed(false, 1, &c));
    }

    #[test]
    fn a_code_change_half_opens_an_open_breaker_at_once() {
        let c = cfg();
        let mut b = Breaker::new(t(0));
        for i in 0..3 { step(&mut b, fail("x"), t(i), &c); }
        assert_eq!(step(&mut b, Event::CodeChanged, t(5), &c), Some(Transition::HalfOpened));
        // long before the back-off would have ended, the fix gets its probe
        assert_eq!(admit(&mut b, t(6), &c), (true, None));
        assert_eq!(step(&mut b, ok(), t(7), &c), Some(Transition::Closed));
        // closed: a code change only clears the count
        step(&mut b, fail("y"), t(8), &c);
        assert_eq!(step(&mut b, Event::CodeChanged, t(9), &c), None);
        assert_eq!(b.fails, 0);
    }

    #[test]
    fn off_never_holds_and_turning_it_off_closes_an_open_breaker() {
        let off = Cfg::default();
        let mut b = Breaker::new(t(0));
        for i in 0..10 { assert_eq!(step(&mut b, fail("x"), t(i), &off), None); }
        assert_eq!(admit(&mut b, t(11), &off), (true, None));
        let c = cfg();
        for i in 0..3 { step(&mut b, fail("x"), t(20 + i), &c); }
        assert_eq!(b.state, State::Open);
        assert_eq!(admit(&mut b, t(30), &off), (true, Some(Transition::Closed)));
        assert_eq!(b.state, State::Closed);
    }

    #[test]
    fn a_call_finishing_while_open_changes_nothing() {
        let c = cfg();
        let mut b = Breaker::new(t(0));
        for i in 0..3 { step(&mut b, fail("x"), t(i), &c); }
        let before = b.until;
        assert_eq!(step(&mut b, ok(), t(4), &c), None);
        assert_eq!(step(&mut b, fail("late"), t(5), &c), None);
        assert_eq!((b.state, b.until), (State::Open, before));
        assert_eq!(b.last_error.as_deref(), Some("late"));
    }

    #[test]
    fn a_restored_row_keeps_an_open_breaker_open_until_its_back_off_ends() {
        let c = cfg();
        let mut b = Breaker::restored(State::Open, t(0), 2, Some("e".into()), &c);
        assert_eq!(b.until, Some(t(60)));
        assert_eq!(admit(&mut b, t(59), &c), (false, None));
        assert_eq!(admit(&mut b, t(60), &c), (true, Some(Transition::HalfOpened)));
        // a probe that died with the process: probe again at once
        let mut h = Breaker::restored(State::Half, t(0), 1, None, &c);
        assert_eq!(admit(&mut h, t(0), &c), (true, Some(Transition::HalfOpened)));
        assert_eq!(Breaker::restored(State::Closed, t(0), 0, None, &c).state, State::Closed);
        assert_eq!(State::parse(State::Half.as_str()), State::Half);
    }

    #[test]
    fn a_dead_letter_carries_the_message_and_its_last_error() {
        let m: serde_json::Value = serde_json::from_str(&dead_message(7, 2, &Some("/q/o/producer/991".into()),
            &Some(b"{\"id\":1}".to_vec()), None, &Some("boom".into()), 5)).unwrap();
        assert_eq!(m["kind"], "dead");
        assert_eq!(m["key"], "/q/o/producer/991");
        assert_eq!(m["value"], "{\"id\":1}");
        assert_eq!(m["error"], "boom");
        assert_eq!(m["attempts"], 5);
    }
}
