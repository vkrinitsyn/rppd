//! [coverage] the states as text, the back-off of a restored breaker, and every transition's message

use super::*;
use chrono::TimeZone;

fn t(s: i64) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 13, 12, 0, 0).unwrap() + Duration::seconds(s)
}

fn cfg() -> Cfg {
    Cfg { fails: 2, slow_ms: 0, open_sec: 10, open_max_sec: 40 }
}

#[test]
fn a_state_has_its_word_and_a_word_its_state() {
    for (s, w) in [(State::Closed, "closed"), (State::Open, "open"), (State::Half, "half")] {
        assert_eq!(s.as_str(), w);
        assert_eq!(State::parse(w), s);
    }
    assert_eq!(State::parse(" open\n"), State::Open, "surrounding space is trimmed");
    assert_eq!(State::parse("unknown"), State::Closed, "anything else is closed");
    assert!(cfg().on() && !Cfg { fails: 0, ..cfg() }.on());
}

#[test]
fn a_half_open_breaker_ignores_a_code_change_already_probing() {
    let c = cfg();
    let mut b = Breaker::new(t(0));
    b.state = State::Half;
    assert_eq!(step(&mut b, Event::CodeChanged, t(1), &c), None, "already half-open: nothing to do");
    assert_eq!(b.state, State::Half);
}

#[test]
fn a_restored_half_open_breaker_is_open_with_its_back_off_from_the_row() {
    let c = cfg();
    let open = Breaker::restored(State::Open, t(0), 0, Some("boom".into()), &c);
    assert_eq!(open.state, State::Open);
    assert_eq!(open.attempt, 1, "at least one re-open");
    assert_eq!(open.until, Some(t(0) + backoff(1, &c)));
    assert_eq!(open.last_error.as_deref(), Some("boom"));
    let half = Breaker::restored(State::Half, t(5), 2, None, &c);
    assert_eq!(half.state, State::Open);
    assert_eq!(half.until, Some(t(5)), "a probe that died with the process is due at once");
    assert_eq!(half.opened_at, Some(t(5)));
    let closed = Breaker::restored(State::parse("closed"), t(9), 3, None, &c);
    assert_eq!(closed.state, State::Closed);
    assert_eq!(closed.until, None);
}

#[test]
fn every_transition_tells_its_state_in_the_notice() {
    let c = cfg();
    let mut b = Breaker::new(t(0));
    let opened = step(&mut b, Event::Done { failed: true, error: Some("e1".into()) }, t(1), &c);
    assert_eq!(opened, None, "one failure of two does not open");
    let opened = step(&mut b, Event::Done { failed: true, error: Some("e2".into()) }, t(2), &c);
    assert_eq!(opened, Some(Transition::Opened));
    let m: serde_json::Value = serde_json::from_str(&message(7, 3, &b, Transition::Opened, 4, t(2))).unwrap();
    assert_eq!(m["state"], "open");
    assert_eq!(m["fails"], 2, "the two failures in a row");
    assert_eq!(m["error"], "e2");
    assert_eq!(m["pp"], 7);
    assert_eq!(m["node"], 3);
    assert!(m["retry_at"].is_string());

    let half = step(&mut b, Event::CodeChanged, t(3), &c);
    assert_eq!(half, Some(Transition::HalfOpened));
    let m: serde_json::Value = serde_json::from_str(&message(7, 3, &b, Transition::HalfOpened, 2, t(3))).unwrap();
    assert_eq!(m["state"], "half");
    assert_eq!(m["held"], 2);

    let closed = step(&mut b, Event::Done { failed: false, error: None }, t(12), &c);
    assert_eq!(closed, Some(Transition::Closed));
    let m: serde_json::Value = serde_json::from_str(&message(7, 3, &b, Transition::Closed, 0, t(12))).unwrap();
    assert_eq!(m["state"], "closed");
    assert_eq!(m["down_sec"], 10, "down from the opening at t(2)");
    assert_eq!(m["held"], 0);
    // a close with no opening time (a breaker never opened) reports no down time
    let never = Breaker::new(t(0));
    let m: serde_json::Value = serde_json::from_str(&message(7, 3, &never, Transition::Closed, 1, t(5))).unwrap();
    assert_eq!(m["down_sec"], 0);
}

#[test]
fn a_dead_letter_names_its_key_value_trigger_and_error() {
    let v = dead_message(9, 1, &Some("/q/x".into()), &Some(b"body".to_vec()), Some("{\"a\":1}".into()),
        &Some("bad".into()), 3);
    let m: serde_json::Value = serde_json::from_str(&v).unwrap();
    assert_eq!(m["kind"], "dead");
    assert_eq!(m["key"], "/q/x");
    assert_eq!(m["value"], "body");
    assert_eq!(m["attempts"], 3);
    let none = dead_message(9, 1, &None, &None, None, &None, 1);
    let m: serde_json::Value = serde_json::from_str(&none).unwrap();
    assert!(m["value"].is_null() && m["key"].is_null());
}
