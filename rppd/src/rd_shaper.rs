//! [shaper] The dispatcher's side of the traffic shaper (shaper.rs is the pure
//! part; yt-pgxt resilience.md, D5).
//!
//! - **Gate** (`shaper_gate`): a call of a function starts only within its
//!   adaptive limit and its rate cap. One that may not start waits in the shaper,
//!   oldest first and outside the pick loop, so the other functions keep running;
//!   a later one never overtakes it.
//! - **Done** (`shaper_done`): every end of a started call adapts the limit and
//!   releases the waiting events the freed slot (and the tokens) allow - already
//!   admitted (`RpFnLog::shaped`), so the gate lets them through. A call that never
//!   ran gives its slot back (`shaper_undo`).
//! - **Notices**: 'throttled' / 'recovered' and backlog 'over' / 'under' go to
//!   `notify_q`; the limit goes to the node's `rppd_function_state` row at most
//!   every 5 s while it moves.
//!
//! The gate runs before the breaker's: a call the breaker then holds gives its
//! slot back, and a probe is never parked behind the shaper while its breaker
//! waits for it.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use slog::{debug, error, info};

use crate::rd_config::RppdNodeCluster;
use crate::rd_fn::{RpFn, RpFnLog};
use crate::shaper::{self, Notice, Shaper, Wait};
use crate::LP;

/// One function's shaper on this node and the events waiting for a slot
#[derive(Debug)]
pub(crate) struct ShaperRt {
    pub(crate) s: Shaper,
    pub(crate) waiting: VecDeque<RpFnLog>,
    /// a release is scheduled for the next token
    wake: bool,
    /// the limit as last written to rppd_function_state, and when
    stored: Option<(u32, Instant)>,
}

pub(crate) type Shapers = HashMap<i32, ShaperRt>;

const LIMIT_UPSERT: &str = "insert into %SCHEMA%.rppd_function_state (fn_id, node_id, limit_now, updated) \
    values ($1, $2, $3, current_timestamp) on conflict (fn_id, node_id) do update set limit_now = excluded.limit_now, \
    updated = excluded.updated";
/// how often the limit is written while it moves
const STORE_EVERY: Duration = Duration::from_secs(5);

/// What a gate or an end leaves for the async side
#[derive(Default)]
struct After {
    release: Vec<RpFnLog>,
    wake_at: Option<DateTime<Utc>>,
    notices: Vec<(Notice, usize)>,
    store: Option<u32>,
}

/// Release what the shaper now allows, and schedule the next token
fn release(rt: &mut ShaperRt, now: DateTime<Utc>, cfg: &shaper::Cfg, after: &mut After) {
    while !rt.waiting.is_empty() {
        match shaper::ready(&mut rt.s, now, cfg) {
            Ok(()) => {
                let mut ev = rt.waiting.pop_front().unwrap();
                ev.shaped = true;
                after.release.push(ev);
            }
            Err(Wait::Rate(at)) => {
                if !rt.wake {
                    rt.wake = true;
                    after.wake_at = Some(at);
                }
                break;
            }
            Err(Wait::Limit) => break,
        }
    }
    if let Some(n) = shaper::backlog(&mut rt.s, rt.waiting.len(), cfg) {
        after.notices.push((n, rt.waiting.len()));
    }
}

impl RppdNodeCluster {
    /// May this call start now? `Some` takes a slot; `None` parks it until one frees
    pub(crate) async fn shaper_gate(&self, fc: &RpFn, f: RpFnLog) -> Option<RpFnLog> {
        if f.shaped {
            return Some(f); // released by the shaper, its slot already taken
        }
        let cfg = fc.shaper_cfg();
        let now = Utc::now();
        let mut after = After::default();
        let run = {
            let mut map = self.shapers.lock().unwrap();
            if !cfg.on() && !map.contains_key(&fc.id) {
                return Some(f); // no shaping, never had any: the common path
            }
            let rt = map.entry(fc.id).or_insert_with(|| ShaperRt { s: Shaper::new(&cfg), waiting: VecDeque::new(),
                wake: false, stored: None });
            // a later event never overtakes one that waits
            // [otel] parked from now: `rppd.shaper.wait` when it is released
            let parked = |mut f: RpFnLog| { f.parked = Some(Instant::now()); f };
            let run = if rt.waiting.is_empty() {
                match shaper::ready(&mut rt.s, now, &cfg) {
                    Ok(()) => Some(f),
                    Err(Wait::Rate(at)) => {
                        rt.waiting.push_back(parked(f));
                        if !rt.wake { rt.wake = true; after.wake_at = Some(at); }
                        None
                    }
                    Err(Wait::Limit) => { rt.waiting.push_back(parked(f)); None }
                }
            } else {
                rt.waiting.push_back(parked(f));
                None
            };
            if let Some(n) = shaper::backlog(&mut rt.s, rt.waiting.len(), &cfg) {
                after.notices.push((n, rt.waiting.len()));
            }
            run
        };
        self.shaper_after(fc, after).await;
        run
    }

    /// A started call of the function ended: the limit adapts, waiting calls go
    pub(crate) async fn shaper_done(&self, f: &RpFnLog) {
        let fc = match self.fns.read().await.get(&f.fn_id) { Some(fc) => fc.clone(), None => return };
        let cfg = fc.shaper_cfg();
        let now = Utc::now();
        let mut after = After::default();
        {
            let mut map = self.shapers.lock().unwrap();
            let Some(rt) = map.get_mut(&fc.id) else { return };
            let failed = crate::breaker::failed(f.error_msg.is_none(), f.took_ms.unwrap_or(0), &fc.breaker_cfg());
            if let Some(n) = shaper::adapt(&mut rt.s, failed, f.took_ms.unwrap_or(0), &cfg) {
                after.notices.push((n, rt.waiting.len()));
            }
            let slots = rt.s.slots();
            let due = match rt.stored { None => cfg.max_parallel > 0, Some((was, at)) => was != slots && at.elapsed() >= STORE_EVERY };
            if due {
                rt.stored = Some((slots, Instant::now()));
                after.store = Some(slots);
            }
            release(rt, now, &cfg, &mut after);
        }
        self.shaper_after(&fc, after).await;
    }

    /// A call that took a slot never ran (the breaker held it, or no Python context)
    pub(crate) async fn shaper_undo(&self, fc: &RpFn) {
        let cfg = fc.shaper_cfg();
        let mut after = After::default();
        {
            let mut map = self.shapers.lock().unwrap();
            let Some(rt) = map.get_mut(&fc.id) else { return };
            rt.s.in_flight = rt.s.in_flight.saturating_sub(1);
            release(rt, Utc::now(), &cfg, &mut after);
        }
        self.shaper_after(fc, after).await;
    }

    async fn shaper_after(&self, fc: &RpFn, after: After) {
        let node = self.node_id.load(Ordering::Relaxed);
        for (n, queued) in after.notices {
            info!(self.log, "{}shaper: fn {} {:?} ({} waiting)", LP, fc.id, n, queued);
            if let Some(q) = fc.notify_q.as_ref().filter(|q| !q.trim().is_empty()) {
                let body = {
                    let map = self.shapers.lock().unwrap();
                    map.get(&fc.id).map(|rt| shaper::message(fc.id, node, &rt.s, n, queued, &fc.shaper_cfg()))
                };
                if let Some(body) = body {
                    self.put_q(q, fc.id, body, "notify_q").await;
                }
            }
        }
        if let Some(limit) = after.store {
            if self.breaker_store.load(Ordering::Relaxed) {
                let schema = self.cfg.read().await.schema.clone();
                if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(LIMIT_UPSERT.replace("%SCHEMA%", schema.as_str())))
                    .bind(fc.id).bind(node).bind(limit as i32).execute(&self.db()).await {
                    error!(self.log, "{}shaper: storing fn {}'s limit: {}", LP, fc.id, e);
                }
            }
        }
        for ev in after.release {
            debug!(self.log, "{}shaper: fn {} releases a waiting call", LP, fc.id);
            #[cfg(feature = "tracer")]
            if let Some(at) = ev.parked {
                self.metric_shaper_wait(fc.id, at.elapsed()); // [otel]
            }
            // try_send: this runs on the dispatcher's own task, which must never
            // wait on its own full channel
            if let Err(tokio::sync::mpsc::error::TrySendError::Full(ev)) = self.sender.try_send(Some(ev)) {
                let sender = self.sender.clone();
                tokio::spawn(async move { let _ = sender.send(ev).await; });
            }
        }
        if let Some(at) = after.wake_at {
            let (ctx, fc) = (self.clone(), fc.clone());
            tokio::spawn(async move {
                tokio::time::sleep((at - Utc::now()).to_std().unwrap_or_default()).await;
                let mut after = After::default();
                {
                    let mut map = ctx.shapers.lock().unwrap();
                    if let Some(rt) = map.get_mut(&fc.id) {
                        rt.wake = false;
                        release(rt, Utc::now(), &fc.shaper_cfg(), &mut after);
                    }
                }
                ctx.shaper_after_boxed(fc, after).await;
            });
        }
    }

    /// `shaper_after` from its own timer task (a boxed future: the call is recursive)
    fn shaper_after_boxed(&self, fc: RpFn, after: After) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move { self.shaper_after(&fc, after).await })
    }
}
