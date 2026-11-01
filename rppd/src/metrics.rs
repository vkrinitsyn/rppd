//! [otel] rppd's instruments (yt-pgxt resilience.md phase 3), on the meter its host
//! hands it (`ConfigurationRequest::Meter`; embedded only, D9 - a standalone rppd
//! has none and records nothing).
//!
//! | name | kind | attributes |
//! |---|---|---|
//! | `rppd.pp.duration` | histogram, ms | pp, outcome |
//! | `rppd.pp.calls` | counter | pp, outcome: ok, error, dead |
//! | `rppd.pp.backlog` | observable gauge | pp: events waiting (shaper) or held (breaker) |
//! | `rppd.breaker.state` | observable gauge | pp: 0 closed, 1 half, 2 open |
//! | `rppd.breaker.transitions` | counter | pp, to |
//! | `rppd.shaper.limit` | observable gauge | pp: the adaptive limit |
//! | `rppd.shaper.wait` | histogram, ms | pp: how long a call waited in its shaper for a slot or a token |
//! | `rppd.contexts` | observable gauge | state: in_use, max |

use std::sync::atomic::Ordering;
use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Histogram, Meter, ObservableGauge};

use crate::breaker::State;
use crate::rd_config::RppdNodeCluster;

pub(crate) struct Instruments {
    duration: Histogram<f64>,
    shaper_wait: Histogram<f64>,
    calls: Counter<u64>,
    transitions: Counter<u64>,
    _gauges: Vec<ObservableGauge<u64>>,
}

impl Instruments {
    pub(crate) fn new(m: &Meter, cl: &RppdNodeCluster) -> Instruments {
        let (b, s) = (cl.breakers.clone(), cl.shapers.clone());
        let backlog = m.u64_observable_gauge("rppd.pp.backlog")
            .with_callback(move |o| {
                let mut n: std::collections::BTreeMap<i32, u64> = Default::default();
                if let Ok(b) = b.lock() { for (id, rt) in b.iter() { *n.entry(*id).or_default() += rt.held.len() as u64; } }
                if let Ok(s) = s.lock() { for (id, rt) in s.iter() { *n.entry(*id).or_default() += rt.waiting.len() as u64; } }
                for (id, v) in n { o.observe(v, &[KeyValue::new("pp", id as i64)]); }
            }).build();
        let b = cl.breakers.clone();
        let state = m.u64_observable_gauge("rppd.breaker.state")
            .with_callback(move |o| {
                if let Ok(b) = b.lock() {
                    for (id, rt) in b.iter() {
                        let v = match rt.b.state { State::Closed => 0, State::Half => 1, State::Open => 2 };
                        o.observe(v, &[KeyValue::new("pp", *id as i64)]);
                    }
                }
            }).build();
        let s = cl.shapers.clone();
        let limit = m.u64_observable_gauge("rppd.shaper.limit")
            .with_callback(move |o| {
                if let Ok(s) = s.lock() {
                    for (id, rt) in s.iter() { o.observe(rt.s.slots() as u64, &[KeyValue::new("pp", *id as i64)]); }
                }
            }).build();
        let (run, max) = (cl.running.clone(), cl.max_db_connections.clone());
        let contexts = m.u64_observable_gauge("rppd.contexts")
            .with_callback(move |o| {
                o.observe(run.load(Ordering::Relaxed).max(0) as u64, &[KeyValue::new("state", "in_use")]);
                o.observe(max.load(Ordering::Relaxed) as u64, &[KeyValue::new("state", "max")]);
            }).build();
        Instruments {
            duration: m.f64_histogram("rppd.pp.duration").with_unit("ms").build(),
            shaper_wait: m.f64_histogram("rppd.shaper.wait").with_unit("ms").build(),
            calls: m.u64_counter("rppd.pp.calls").build(),
            transitions: m.u64_counter("rppd.breaker.transitions").build(),
            _gauges: vec![backlog, state, limit, contexts],
        }
    }
}

impl RppdNodeCluster {
    /// A call of a pp ended (`outcome`: ok or error), or an event was dead-lettered
    pub(crate) fn metric_call(&self, pp: i32, outcome: &'static str, took_ms: Option<i64>) {
        if let Ok(g) = self.metrics.read() {
            if let Some(i) = g.as_ref() {
                let a = [KeyValue::new("pp", pp as i64), KeyValue::new("outcome", outcome)];
                i.calls.add(1, &a);
                if let Some(ms) = took_ms {
                    i.duration.record(ms as f64, &a);
                }
            }
        }
    }

    /// [shaper] A call left its shaper after waiting `took`
    pub(crate) fn metric_shaper_wait(&self, pp: i32, took: std::time::Duration) {
        if let Ok(g) = self.metrics.read() {
            if let Some(i) = g.as_ref() {
                i.shaper_wait.record(took.as_secs_f64() * 1000.0, &[KeyValue::new("pp", pp as i64)]);
            }
        }
    }

    /// A breaker moved to `to`
    pub(crate) fn metric_transition(&self, pp: i32, to: &'static str) {
        if let Ok(g) = self.metrics.read() {
            if let Some(i) = g.as_ref() {
                i.transitions.add(1, &[KeyValue::new("pp", pp as i64), KeyValue::new("to", to)]);
            }
        }
    }
}
