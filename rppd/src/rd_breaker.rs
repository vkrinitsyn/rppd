//! [breaker] The dispatcher's side of the circuit breaker (breaker.rs is the pure
//! state machine; yt-pgxt resilience.md, D2/D3/D6).
//!
//! - **Gate** (`breaker_gate`): before an event of a function runs, its breaker on
//!   this node is asked. Held events wait in the breaker, oldest first, outside the
//!   pick loop, so the other functions keep running. Nothing held is invoked, so an
//!   etcd message stays unacknowledged and a logged trigger event keeps its
//!   `took_ms` null - a restart has both again.
//! - **Done** (`breaker_done`): every completion is counted; then the event is
//!   acknowledged, retried (`max_attempts`: back to the head of its function's line,
//!   held if the breaker opened) or dead-lettered (`dlq`) - and acknowledged only
//!   then. With `max_attempts` = 0 a failure is recorded and the event is done, as
//!   before.
//! - **Transitions**: 'open' and 'closed' go to `notify_q` (the cadence format), every
//!   transition to this node's `rppd_function_state` row. A close drains the held
//!   events into the queue; the end of a back-off releases one held event as the
//!   probe; a code change half-opens and releases one at once.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::time::Instant;

use chrono::{DateTime, Utc};
use slog::{debug, error, info, warn};

use crate::breaker::{self, Breaker, Event, State, Transition};
use crate::cadence::{is_queue, producer_key};
use crate::rd_config::RppdNodeCluster;
use crate::rd_fn::{RpFn, RpFnId, RpFnLog};
use crate::LP;

/// One function's breaker on this node, and the events it holds
#[derive(Debug)]
pub(crate) struct BreakerRt {
    pub(crate) b: Breaker,
    /// held while not closed, oldest first
    pub(crate) held: VecDeque<RpFnLog>,
    /// a release is scheduled for the end of the back-off
    pub(crate) timer: bool,
}

pub(crate) type Breakers = HashMap<i32, BreakerRt>;

impl BreakerRt {
    fn new(now: DateTime<Utc>) -> BreakerRt {
        BreakerRt { b: Breaker::new(now), held: VecDeque::new(), timer: false }
    }
}

const STATE_UPSERT: &str = "insert into %SCHEMA%.rppd_function_state (fn_id, node_id, state, since, attempt, last_error, held, updated) \
    values ($1, $2, $3, $4, $5, $6, $7, current_timestamp) on conflict (fn_id, node_id) do update set state = excluded.state, \
    since = excluded.since, attempt = excluded.attempt, last_error = excluded.last_error, held = excluded.held, updated = excluded.updated";
const STATE_SELECT: &str = "select fn_id, state, since, attempt, last_error from %SCHEMA%.rppd_function_state \
    where node_id = $1 and state <> 'closed'";

/// What a gate or a completion leaves for the async side to do
#[derive(Default)]
struct After {
    /// (the breaker as it is now, the transition, events held at that moment)
    transition: Option<(Breaker, Transition, usize)>,
    /// a close: these go back to the queue
    drain: Vec<RpFnLog>,
    /// the end of a back-off to release one held event at
    release_at: Option<DateTime<Utc>>,
}

/// After a step: what to drain or when to release, from the breaker's state now
fn follow_up(rt: &mut BreakerRt, t: Option<Transition>, after: &mut After) {
    if let Some(t) = t {
        after.transition = Some((rt.b.clone(), t, rt.held.len()));
        if t == Transition::Closed {
            after.drain = rt.held.drain(..).collect();
        }
    }
    if rt.b.state == State::Open && !rt.held.is_empty() && !rt.timer {
        rt.timer = true;
        after.release_at = rt.b.until;
    }
}

impl RppdNodeCluster {
    /// May this event run now? `Some` when it may; `None` when the function's breaker
    /// holds it - it runs once the breaker lets events through again.
    pub(crate) async fn breaker_gate(&self, fc: &RpFn, f: RpFnLog) -> Option<RpFnLog> {
        let cfg = fc.breaker_cfg();
        let now = Utc::now();
        let mut after = After::default();
        // a held event's in-progress mark on its queue line, if its pick left one
        let mut mark = None;
        let run = {
            let mut map = self.breakers.lock().unwrap();
            if !cfg.on() && !map.contains_key(&fc.id) {
                return Some(f); // no breaker, never had one: the common path
            }
            let rt = map.entry(fc.id).or_insert_with(|| BreakerRt::new(now));
            let (ok, t) = breaker::admit(&mut rt.b, now, &cfg);
            let run = if ok { Some(f) } else {
                // the shaper's slot it took is given back (rd_config.rs): it asks again
                mark = f.fn_idp.as_ref().map(|p| (p.priority, f.id, fc.to_topic(&f)));
                rt.held.push_back(RpFnLog { shaped: false, ..f });
                None
            };
            follow_up(rt, t, &mut after);
            run
        };
        if run.is_none() {
            debug!(self.log, "{}breaker: fn {} holds an event", LP, fc.id);
        }
        if let Some((priority, id, topic)) = mark {
            // held, so not running: its line must not stay busy on its account, or
            // the events the close drains back behind the mark never run (topology
            // 92b B4, 2026-10-20: only the probe ran, one of ten)
            self.queue.write().await.unmark(priority, id, topic);
        }
        self.breaker_after(fc, after).await;
        run
    }

    /// An admitted event did not run after all (no Python context: it went back to
    /// the queue, or none could be made): a half-open breaker's probe slot is free
    /// again, or the probe that comes back would be held by its own breaker forever
    pub(crate) fn breaker_undo(&self, fn_id: i32) {
        if let Some(rt) = self.breakers.lock().unwrap().get_mut(&fn_id) {
            if rt.b.state == State::Half {
                rt.b.probing = false;
            }
        }
    }

    /// An event finished (`took_ms` set, `error_msg` set when it raised): its breaker
    /// counts it, then the event is acknowledged, retried or dead-lettered.
    pub(crate) async fn breaker_done(&self, f: &RpFnLog) {
        let fc = match self.fns.read().await.get(&f.fn_id) {
            Some(fc) => fc.clone(),
            None => { self.ack(f).await; return; }
        };
        let cfg = fc.breaker_cfg();
        let now = Utc::now();
        #[cfg(feature = "tracer")]
        self.metric_call(fc.id, if f.error_msg.is_none() { "ok" } else { "error" }, f.took_ms);
        let mut after = After::default();
        {
            let mut map = self.breakers.lock().unwrap();
            if cfg.on() || map.contains_key(&fc.id) {
                let rt = map.entry(fc.id).or_insert_with(|| BreakerRt::new(now));
                let failed = breaker::failed(f.error_msg.is_none(), f.took_ms.unwrap_or(0), &cfg);
                let t = breaker::step(&mut rt.b, Event::Done { failed, error: f.error_msg.clone() }, now, &cfg);
                follow_up(rt, t, &mut after);
            }
        }
        self.breaker_after(&fc, after).await;

        // the event itself: only a raised one is retried - a slow success counts
        // against the breaker, but it did its work
        let max = fc.max_attempts.max(0) as u32;
        if f.error_msg.is_some() && max > 0 {
            let attempt = f.attempt + 1;
            if attempt < max {
                let retry = RpFnLog { attempt, took_ms: None, error_msg: None, output: None,
                    started: Some(Instant::now()), shaped: false, ..f.clone() };
                self.breaker_retry(&fc, retry).await;
                return; // not acknowledged: it is not done
            }
            self.dead_letter(&fc, f, attempt).await;
        }
        self.ack(f).await;
    }

    /// Will this failed attempt be retried? Then its log row keeps `took_ms` null, so
    /// a restart still has it (rd_config.rs `execute`).
    pub(crate) fn will_retry(fc: &RpFn, f: &RpFnLog, failed: bool) -> bool {
        failed && fc.max_attempts > 0 && f.attempt + 1 < fc.max_attempts as u32
    }

    /// A failed event goes back to the head of its function's line: held when the
    /// breaker is not closed, otherwise queued to run again
    async fn breaker_retry(&self, fc: &RpFn, ev: RpFnLog) {
        let cfg = fc.breaker_cfg();
        let mut after = After::default();
        let queue_it = {
            let mut map = self.breakers.lock().unwrap();
            match map.get_mut(&fc.id) {
                Some(rt) if cfg.on() && rt.b.state != State::Closed => {
                    rt.held.push_front(ev);
                    follow_up(rt, None, &mut after);
                    None
                }
                _ => Some(ev),
            }
        };
        self.breaker_after(fc, after).await;
        if let Some(ev) = queue_it {
            debug!(self.log, "{}breaker: fn {} retries an event (attempt {})", LP, fc.id, ev.attempt + 1);
            let (topic, fid) = (fc.to_topic(&ev), ev.fn_idp.clone().unwrap_or(RpFnId::fromf(fc)));
            self.queue.write().await.put_one(ev, topic, fid, true);
            self.respawn_drive();
        }
    }

    /// [breaker] The operator changed the function's code: an open breaker probes
    /// the fix now (called from `RpFn::merge`, which holds `fns`)
    pub(crate) fn breaker_code_changed(&self, fc: &RpFn) {
        let now = Utc::now();
        let mut after = After::default();
        let probe = {
            let mut map = self.breakers.lock().unwrap();
            let Some(rt) = map.get_mut(&fc.id) else { return };
            let t = breaker::step(&mut rt.b, Event::CodeChanged, now, &fc.breaker_cfg());
            follow_up(rt, t, &mut after);
            if t == Some(Transition::HalfOpened) { rt.held.pop_front() } else { None }
        };
        let (ctx, fc) = (self.clone(), fc.clone());
        tokio::spawn(async move {
            ctx.breaker_after(&fc, after).await;
            if let Some(ev) = probe {
                info!(ctx.log, "{}breaker: fn {} has new code - probing it now", LP, fc.id);
                let _ = ctx.sender.send(Some(ev)).await;
            }
        });
    }

    /// The async side of a step: the notice, the state row, the drain, the release
    async fn breaker_after(&self, fc: &RpFn, after: After) {
        let now = Utc::now();
        let node = self.node_id.load(Ordering::Relaxed);
        if let Some((b, t, held)) = &after.transition {
            #[cfg(feature = "tracer")]
            self.metric_transition(fc.id, b.state.as_str());
            match t {
                Transition::Opened => warn!(self.log, "{}breaker: fn {} OPEN (attempt {}, retry at {:?}): {}", LP, fc.id,
                    b.attempt, b.until, b.last_error.clone().unwrap_or_default()),
                Transition::HalfOpened => info!(self.log, "{}breaker: fn {} half-open: one probe", LP, fc.id),
                Transition::Closed => info!(self.log, "{}breaker: fn {} closed, {} held event(s) drain", LP, fc.id, after.drain.len()),
            }
            if *t != Transition::HalfOpened {
                if let Some(q) = fc.notify_q.as_ref().filter(|q| !q.trim().is_empty()) {
                    let held = if *t == Transition::Closed { after.drain.len() } else { *held };
                    self.put_q(q, fc.id, breaker::message(fc.id, node, b, *t, held, now), "notify_q").await;
                }
            }
            if self.breaker_store.load(Ordering::Relaxed) {
                let schema = self.cfg.read().await.schema.clone();
                if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(STATE_UPSERT.replace("%SCHEMA%", schema.as_str())))
                    .bind(fc.id).bind(node).bind(b.state.as_str()).bind(b.since).bind(b.attempt as i32)
                    .bind(b.last_error.clone()).bind(*held as i32)
                    .execute(&self.db()).await {
                    error!(self.log, "{}breaker: storing fn {}: {}", LP, fc.id, e);
                }
            }
        }
        if !after.drain.is_empty() {
            // back to the queue, oldest at the head of the line (put_one with
            // push_back = false appends behind), and enough tokens to run them
            let n = after.drain.len();
            {
                let mut q = self.queue.write().await;
                for ev in after.drain {
                    let (topic, fid) = (fc.to_topic(&ev), ev.fn_idp.clone().unwrap_or(RpFnId::fromf(fc)));
                    q.put_one(ev, topic, fid, false);
                }
            }
            let tokens = n.min(self.max_db_connections.load(Ordering::Relaxed).max(1) as usize);
            for _ in 0..tokens {
                if self.sender.try_send(None).is_err() { break; }
            }
        }
        if let Some(at) = after.release_at {
            let (ctx, id) = (self.clone(), fc.id);
            tokio::spawn(async move {
                let wait = (at - Utc::now()).to_std().unwrap_or_default();
                tokio::time::sleep(wait).await;
                let ev = {
                    let mut map = ctx.breakers.lock().unwrap();
                    map.get_mut(&id).and_then(|rt| { rt.timer = false; rt.held.pop_front() })
                };
                // through the gate again: past the back-off it is the probe
                if let Some(ev) = ev {
                    let _ = ctx.sender.send(Some(ev)).await;
                }
            });
        }
    }

    /// A message on a /q/ queue, as cadence sends them
    pub(crate) async fn put_q(&self, q: &str, fn_id: i32, body: String, what: &str) {
        if !is_queue(q) {
            warn!(self.log, "{}breaker: fn {}: {} {} is not a /q/ queue, nothing sent", LP, fn_id, what, q);
            return;
        }
        // [db-tag] in this instance's namespace
        let key = self.cfg.read().await.qkey(&producer_key(q, fn_id, Utc::now()));
        if let Err(e) = self.etcd.write().await.put(&key, body.into_bytes()).await {
            error!(self.log, "{}breaker: put {}: {}", LP, key, e);
        }
    }

    /// The event failed `max_attempts` times: its message, key, value and last error
    /// go to `dlq` (or only to the log without one)
    async fn dead_letter(&self, fc: &RpFn, f: &RpFnLog, attempts: u32) {
        error!(self.log, "{}breaker: fn {} gives up on an event after {} attempt(s): {}", LP, fc.id, attempts,
            f.error_msg.clone().unwrap_or_default());
        #[cfg(feature = "tracer")]
        self.metric_call(fc.id, "dead", None);
        if let Some(q) = fc.dlq.as_ref().filter(|q| !q.trim().is_empty()) {
            let trig = f.trig_value.as_ref().map(|v| serde_json::to_string(&v.0).unwrap_or_default());
            let node = self.node_id.load(Ordering::Relaxed);
            self.put_q(q, fc.id, breaker::dead_message(fc.id, node, &f.key, &f.value, trig, &f.error_msg, attempts), "dlq").await;
        }
    }

    /// The event is done with: an etcd message is deleted from its queue
    async fn ack(&self, f: &RpFnLog) {
        #[cfg(not(feature = "etcd-external"))]
        if let Some(key) = &f.key {
            self.etcd.read().await.aks(key).await;
        }
    }

    /// At startup: this node's breakers as its rppd_function_state rows left them -
    /// an open one stays open until its back-off ends
    pub(crate) async fn breaker_restore(&self) {
        let schema = self.cfg.read().await.schema.clone();
        let exists = sqlx::query_scalar::<_, bool>("select exists (select 1 from information_schema.tables \
            where table_schema = $1 and table_name = 'rppd_function_state')")
            .bind(&schema).fetch_one(&self.db()).await.unwrap_or(false);
        self.breaker_store.store(exists, Ordering::Relaxed);
        if !exists {
            return;
        }
        let node = self.node_id.load(Ordering::Relaxed);
        let rows = match sqlx::query_as::<_, (i32, String, Option<DateTime<Utc>>, i32, Option<String>)>(
            sqlx::AssertSqlSafe(STATE_SELECT.replace("%SCHEMA%", schema.as_str())))
            .bind(node).fetch_all(&self.db()).await {
            Ok(r) => r,
            Err(e) => { warn!(self.log, "{}breaker: reading rppd_function_state: {}", LP, e); return; }
        };
        let fns = self.fns.read().await;
        let mut map = self.breakers.lock().unwrap();
        for (fn_id, state, since, attempt, last_error) in rows {
            let Some(fc) = fns.get(&fn_id) else { continue };
            let cfg = fc.breaker_cfg();
            if !cfg.on() {
                continue;
            }
            let b = Breaker::restored(State::parse(&state), since.unwrap_or_else(Utc::now), attempt.max(0) as u32,
                last_error, &cfg);
            info!(self.log, "{}breaker: fn {} restored {} until {:?}", LP, fn_id, b.state.as_str(), b.until);
            map.insert(fn_id, BreakerRt { b, held: VecDeque::new(), timer: false });
        }
    }
}
