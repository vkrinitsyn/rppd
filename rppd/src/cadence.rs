//! Cadence: a watch on a pipeline process (pp) - an etcd queue that is expected
//! to receive a message on a cadence.
//!
//! A pp is a row of `rppd_cron` with `cadence = true`:
//! - `fn_id` names the rppd function consuming the queue; its `schema_table` (a
//!   `/q/` path) is the queue watched;
//! - `cron` is the EXPECTED cadence: a message is due by the end of the first
//!   full cron period after the last one seen (the second fire after it), so a
//!   producer need not be aligned with the clock;
//! - `watch_min` - minutes on watch before the deadline (0: deadline at once);
//!   `retry_sec` - the watch message repeats this often while on watch (0: once);
//! - `watch_q`, `deadline_q` - the queues the messages go to (empty: none);
//! - `state` (ok | watch | deadline), `last_seen`, `since`, `attempt` - kept here.
//!
//! Missed cadence -> `watch` and a message to watch_q, repeated every retry_sec;
//! after watch_min -> `deadline` and ONE message to deadline_q; a message on the
//! queue again -> `ok` and an ok message to the queue of the state it left.
//!
//! Arrivals are noted by the queue watcher on whichever node consumes the queue
//! (`seen`, persisted to the row at most every [PERSIST_SEEN_SEC]); only the
//! master evaluates (`cadence_tick`, on the monitor loop), and the state lives in
//! the row, so a new master continues where the old one stopped: it builds its
//! runtime from the row as STORED NOW (what the last master wrote, replicated),
//! never from the row it loaded at startup, nor from a runtime it kept from an
//! earlier term of its own (see [Stored], `CronContext::new_term`).
use std::collections::HashMap;
use chrono::{DateTime, Duration, SubsecRound, Utc};
use serde_json::json;
use slog::{debug, error, info, warn};
use crate::cron::RpFnCron;
use crate::rd_config::RppdNodeCluster;
use crate::LP;

/// how often a consuming node writes `last_seen` to its row, at most
pub(crate) const PERSIST_SEEN_SEC: i64 = 5;

/// the state of a pp
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CState { Ok, Watch, Deadline }

impl CState {
    pub(crate) fn parse(s: &str) -> Self {
        match s { "watch" => CState::Watch, "deadline" => CState::Deadline, _ => CState::Ok }
    }
    pub(crate) fn as_str(&self) -> &'static str {
        match self { CState::Ok => "ok", CState::Watch => "watch", CState::Deadline => "deadline" }
    }
}

/// what a tick decided to send
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Emit {
    /// to watch_q
    Watch,
    /// to deadline_q, once
    Deadline,
    /// to the queue of the state left
    Ok { was: CState },
}

/// the runtime of one pp
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Cadence {
    pub(crate) state: CState,
    /// entered the current state
    pub(crate) since: Option<DateTime<Utc>>,
    pub(crate) attempt: i32,
    pub(crate) last_seen: Option<DateTime<Utc>>,
    /// the last watch message sent, for retry_sec
    pub(crate) last_sent: Option<DateTime<Utc>>,
}

impl Cadence {
    /// From the row this node loaded (startup or a reload) - only when the row as
    /// stored now could not be read.
    pub(crate) fn of(c: &RpFnCron) -> Self {
        Self::from_stored(&Stored::of(c), c.retry_sec)
    }

    /// From a stored state: the row the last master wrote. The watch retry
    /// schedule carries on - attempt N went out at about since + (N-1)*retry_sec,
    /// so the next one is due at since + N*retry_sec, not at once.
    pub(crate) fn from_stored(s: &Stored, retry_sec: i32) -> Self {
        let last_sent = match (s.state, s.since) {
            (CState::Watch, Some(since)) if retry_sec > 0 && s.attempt > 1 =>
                Some(since + Duration::seconds(retry_sec as i64 * (s.attempt - 1) as i64)),
            (_, since) => since,
        };
        Cadence { state: s.state, since: s.since, attempt: s.attempt, last_seen: s.last_seen, last_sent }
    }
}

/// A row's cadence state as stored, read by the master every tick.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Stored {
    pub(crate) state: CState,
    pub(crate) since: Option<DateTime<Utc>>,
    pub(crate) attempt: i32,
    pub(crate) last_seen: Option<DateTime<Utc>>,
}

impl Stored {
    fn of(c: &RpFnCron) -> Self {
        Stored { state: CState::parse(c.state.as_str()), since: c.since, attempt: c.attempt, last_seen: c.last_seen }
    }

    /// The row says what the runtime does. `last_seen` aside: it only grows, the
    /// consuming nodes write it, and the master merges it in before each step.
    pub(crate) fn matches(&self, st: &Cadence) -> bool {
        self.state == st.state && self.since == st.since && self.attempt == st.attempt
    }
}

/// The runtime the master evaluates a row with: its own, kept while it stays
/// master; else the row as stored now; else (stored unreadable) the loaded row.
///
/// Topology 92c C6: the new master built its runtime from the row it LOADED AT
/// STARTUP (`state = ok`) while the replicated row said `watch` since the old
/// master's time - so it found the pp overdue, started a second watch with a new
/// `since` and sent a second attempt-1 watch message.
pub(crate) fn runtime(c: &RpFnCron, stored: Option<&Stored>) -> Cadence {
    match (&c.cadence_rt, stored) {
        (Some(rt), _) => rt.clone(),
        (None, Some(s)) => Cadence::from_stored(s, c.retry_sec),
        (None, None) => Cadence::of(c),
    }
}

/// Whether the master writes its runtime to the row: the row does not say what
/// the runtime does - a transition this tick, an earlier write that failed (a
/// cluster commit refused), or the row overwritten since (a replicated image, a
/// previous master's late write). Without the row (it could not be read): a
/// change this tick.
pub(crate) fn needs_store(stored: Option<&Stored>, before: &Cadence, after: &Cadence) -> bool {
    match stored {
        Some(s) => !s.matches(after),
        None => after.state != before.state || after.attempt != before.attempt || after.since != before.since,
    }
}

/// The queue an emission goes to, before the empty-name filter.
pub(crate) fn emit_queue<'a>(emit: &Emit, watch_q: &'a Option<String>, deadline_q: &'a Option<String>) -> &'a Option<String> {
    match emit {
        Emit::Watch => watch_q,
        Emit::Deadline => deadline_q,
        Emit::Ok { was: CState::Watch } => watch_q,
        Emit::Ok { .. } => deadline_q,
    }
}

/// When the next message is due: the end of the first full cron period after
/// `from` - the second fire after it.
pub(crate) fn due_after(cron: &str, from: &DateTime<Utc>) -> Result<DateTime<Utc>, String> {
    let first = cron_parser::parse(cron, from).map_err(|e| format!("{:?}", e))?;
    cron_parser::parse(cron, &first).map_err(|e| format!("{:?}", e))
}

/// One evaluation of a pp: moves its state and says what to send, if anything.
/// `baseline` stands in for `last_seen` while nothing was ever seen (the master's
/// start). `watch_min` and `retry_sec` below 0 count as 0.
pub(crate) fn step(cron: &str, watch_min: i32, retry_sec: i32, st: &mut Cadence,
                   now: DateTime<Utc>, baseline: DateTime<Utc>) -> Result<Option<Emit>, String> {
    // a message after the state was entered: back to ok
    if st.state != CState::Ok {
        if let (Some(seen), Some(since)) = (st.last_seen, st.since) {
            if seen > since {
                let was = st.state;
                st.state = CState::Ok;
                st.since = Some(now);
                st.attempt = 0;
                st.last_sent = None;
                return Ok(Some(Emit::Ok { was }));
            }
        }
    }
    match st.state {
        CState::Ok => {
            let from = st.last_seen.unwrap_or(baseline);
            let due = due_after(cron, &from)?;
            if now <= due {
                return Ok(None);
            }
            st.since = Some(now);
            st.attempt = 1;
            st.last_sent = Some(now);
            if watch_min <= 0 {
                st.state = CState::Deadline;
                Ok(Some(Emit::Deadline))
            } else {
                st.state = CState::Watch;
                Ok(Some(Emit::Watch))
            }
        }
        CState::Watch => {
            let since = st.since.unwrap_or(now);
            if now >= since + Duration::minutes(watch_min.max(0) as i64) {
                st.state = CState::Deadline;
                st.since = Some(now);
                st.last_sent = Some(now);
                return Ok(Some(Emit::Deadline));
            }
            if retry_sec > 0 {
                let last = st.last_sent.unwrap_or(since);
                if now >= last + Duration::seconds(retry_sec as i64) {
                    st.attempt += 1;
                    st.last_sent = Some(now);
                    return Ok(Some(Emit::Watch));
                }
            }
            Ok(None)
        }
        // one deadline message only; waits for the next message
        CState::Deadline => Ok(None),
    }
}

/// the message body
pub(crate) fn body(c: &RpFnCron, queue: &str, st: &Cadence, emit: &Emit, now: DateTime<Utc>, baseline: DateTime<Utc>) -> String {
    let ts = |d: Option<DateTime<Utc>>| d.map(|d| d.to_rfc3339());
    match emit {
        Emit::Ok { was } => json!({
            "pp": c.id, "queue": queue, "fn_id": c.fn_id, "state": "ok", "was": was.as_str(),
            "down_sec": st.last_seen.zip(c.since).map(|(s, b)| (s - b).num_seconds().max(0)).unwrap_or(0),
            "last_seen": ts(st.last_seen),
        }).to_string(),
        _ => {
            let due = due_after(c.cron.as_str(), &st.last_seen.unwrap_or(baseline)).ok();
            json!({
                "pp": c.id, "queue": queue, "fn_id": c.fn_id, "state": st.state.as_str(),
                "due_at": ts(due),
                "last_seen": ts(st.last_seen),
                "late_sec": due.map(|d| (now - d).num_seconds().max(0)).unwrap_or(0),
                "attempt": st.attempt,
            }).to_string()
        }
    }
}

/// a queue path a message can be put on: `/q/<name>` or `/queue/<name>`
pub(crate) fn is_queue(path: &str) -> bool {
    let p: Vec<&str> = path.split('/').collect();
    p.len() >= 3 && p[0].is_empty() && (p[1] == "q" || p[1] == "queue") && !p[2].trim().is_empty()
}

/// the producer key of a message: `<q>/producer/<pp>-<epoch ms>`
pub(crate) fn producer_key(q: &str, pp: i32, now: DateTime<Utc>) -> String {
    format!("{}/producer/{}-{}", q.trim_end_matches('/'), pp, now.timestamp_millis())
}

/// arrivals per queue on this node: (last seen, last written to the db)
pub(crate) type SeenMap = HashMap<String, (DateTime<Utc>, Option<DateTime<Utc>>)>;

const SEEN_SQL: &str = "update %SCHEMA%.rppd_cron c set last_seen = greatest(coalesce(c.last_seen, $1), $1) \
    from %SCHEMA%.rppd_function f where c.cadence and c.fn_id = f.id and f.schema_table = $2";
/// the rows as stored: `last_seen` the consuming nodes wrote, and the state the last master did
const STORED_SQL: &str = "select id, state, since, attempt, last_seen from %SCHEMA%.rppd_cron where cadence";
const STATE_SQL: &str = "update %SCHEMA%.rppd_cron set state = $2, since = $3, attempt = $4, \
    last_seen = greatest(coalesce(last_seen, $5), $5), error_msg = NULL where id = $1";
const ERR_SQL: &str = "update %SCHEMA%.rppd_cron set error_msg = $2 where id = $1 and error_msg is distinct from $2";
const NO_ERR_SQL: &str = "update %SCHEMA%.rppd_cron set error_msg = NULL where id = $1 and error_msg is not null";

impl RppdNodeCluster {
    /// A message arrived on `queue` (the watcher of this node consumed it).
    pub(crate) async fn cadence_seen(&self, queue: &str) {
        let now = Utc::now();
        let write = {
            let mut seen = match self.cadence_seen.lock() { Ok(s) => s, Err(p) => p.into_inner() };
            let e = seen.entry(queue.to_string()).or_insert((now, None));
            e.0 = now;
            match e.1 {
                Some(w) if (now - w).num_seconds() < PERSIST_SEEN_SEC => false,
                _ => { e.1 = Some(now); true }
            }
        };
        // a table without the cadence columns refuses this; harmless, and quiet
        if write {
            let schema = self.cfg.read().await.schema.clone();
            if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(SEEN_SQL.replace("%SCHEMA%", schema.as_str())))
                .bind(now).bind(queue).execute(&self.db()).await {
                debug!(self.log, "{}cadence: last_seen of {}: {}", LP, queue, e);
            }
        }
    }

    /// The master's evaluation of every pp, on the monitor loop.
    pub(crate) async fn cadence_tick(&self) {
        if !self.master.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        let epoch = self.master_epoch.load(std::sync::atomic::Ordering::Relaxed);
        {
            let cron = self.cron.read().await;
            if !cron.cadence_on || !cron.crons.values().any(|c| c.cadence) {
                return;
            }
        }
        let schema = self.cfg.read().await.schema.clone();
        let db = self.db();
        // the rows as stored (a synced rppd_cron): last_seen written by the
        // consuming nodes, merged below, and the state the last master wrote
        let stored: Option<HashMap<i32, Stored>> = match sqlx::query_as::<_, (i32, String, Option<DateTime<Utc>>, i32, Option<DateTime<Utc>>)>(
            sqlx::AssertSqlSafe(STORED_SQL.replace("%SCHEMA%", schema.as_str()))).fetch_all(&db).await {
            Ok(rows) => Some(rows.into_iter().map(|(id, state, since, attempt, last_seen)|
                (id, Stored { state: CState::parse(state.as_str()), since, attempt, last_seen })).collect()),
            Err(e) => { debug!(self.log, "{}cadence: reading the stored rows: {}", LP, e); None }
        };
        let seen: HashMap<String, DateTime<Utc>> = {
            let s = match self.cadence_seen.lock() { Ok(s) => s, Err(p) => p.into_inner() };
            s.iter().map(|(k, v)| (k.clone(), v.0)).collect()
        };
        let queues: HashMap<i32, String> = self.fns.read().await.iter()
            .map(|(id, f)| (*id, f.schema_table.clone())).collect();
        // whole microseconds: `since` is set from it and round-trips through a
        // timestamptz exactly, so the stored row can be compared with the runtime
        let now = Utc::now().trunc_subsecs(6);
        let mut out: Vec<(String, String)> = vec![];
        {
            let mut cron = self.cron.write().await;
            if cron.new_term(epoch) {
                info!(self.log, "{}cadence: master (term {}): the runtimes start from the stored rows", LP, epoch);
            }
            let baseline = cron.started;
            for (id, c) in cron.crons.iter_mut().filter(|(_, c)| c.cadence) {
                let queue = match queues.get(&c.fn_id) {
                    Some(q) if is_queue(q) => q.clone(),
                    Some(q) => {
                        self.cadence_err(&db, &schema, *id, format!("cadence: function {} consumes {}, not a /q/ queue", c.fn_id, q)).await;
                        continue;
                    }
                    None => {
                        self.cadence_err(&db, &schema, *id, format!("cadence: no function {}", c.fn_id)).await;
                        continue;
                    }
                };
                let row = stored.as_ref().and_then(|s| s.get(id));
                let mut st = runtime(c, row);
                let latest = [st.last_seen, row.and_then(|r| r.last_seen), seen.get(&queue).cloned()]
                    .into_iter().flatten().max();
                st.last_seen = latest;
                let before = st.clone();
                match step(c.cron.as_str(), c.watch_min, c.retry_sec, &mut st, now, baseline) {
                    Err(e) => {
                        self.cadence_err(&db, &schema, *id, format!("cadence: cron [{}]: {}", c.cron, e)).await;
                        continue;
                    }
                    Ok(None) => {}
                    Ok(Some(emit)) => {
                        let q = emit_queue(&emit, &c.watch_q, &c.deadline_q).clone()
                            .filter(|q| !q.trim().is_empty());
                        // `since` before the transition, for the ok message's down time
                        let mut cc = c.clone();
                        cc.since = before.since;
                        info!(self.log, "{}cadence: pp {} on {} {} -> {} (attempt {})", LP, id, queue,
                            before.state.as_str(), st.state.as_str(), st.attempt);
                        if let Some(q) = q {
                            if is_queue(q.as_str()) {
                                out.push((producer_key(q.as_str(), *id, now), body(&cc, queue.as_str(), &st, &emit, now, baseline)));
                            } else {
                                warn!(self.log, "{}cadence: pp {}: {} is not a /q/ queue, nothing sent", LP, id, q);
                            }
                        }
                    }
                }
                if needs_store(row, &before, &st) {
                    if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(STATE_SQL.replace("%SCHEMA%", schema.as_str())))
                        .bind(*id).bind(st.state.as_str()).bind(st.since).bind(st.attempt).bind(st.last_seen)
                        .execute(&db).await {
                        error!(self.log, "{}cadence: storing pp {}: {}", LP, id, e);
                    }
                } else if c.error_msg.is_some() {
                    let _ = sqlx::query(sqlx::AssertSqlSafe(NO_ERR_SQL.replace("%SCHEMA%", schema.as_str())))
                        .bind(*id).execute(&db).await;
                }
                c.error_msg = None;
                c.cadence_rt = Some(st);
            }
        }
        for (key, value) in out {
            // [db-tag] in this instance's namespace
            let key = self.cfg.read().await.qkey(&key);
            if let Err(e) = self.etcd.write().await.put(&key, value.into_bytes()).await {
                error!(self.log, "{}cadence: put {}: {}", LP, key, e);
            }
        }
    }

    async fn cadence_err(&self, db: &sqlx::Pool<sqlx::Postgres>, schema: &str, id: i32, msg: String) {
        debug!(self.log, "{}{}", LP, msg);
        if let Err(e) = sqlx::query(sqlx::AssertSqlSafe(ERR_SQL.replace("%SCHEMA%", schema)))
            .bind(id).bind(msg).execute(db).await {
            error!(self.log, "{}cadence: storing an error of pp {}: {}", LP, id, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t(h: u32, m: u32, s: u32) -> DateTime<Utc> { Utc.with_ymd_and_hms(2026, 10, 7, h, m, s).unwrap() }
    fn fresh(seen: Option<DateTime<Utc>>) -> Cadence {
        Cadence { state: CState::Ok, since: None, attempt: 0, last_seen: seen, last_sent: None }
    }

    #[test]
    fn due_is_the_end_of_the_first_full_period() {
        assert_eq!(due_after("* * * * *", &t(12, 0, 10)).unwrap(), t(12, 2, 0));
        assert_eq!(due_after("*/5 * * * *", &t(12, 1, 0)).unwrap(), t(12, 10, 0));
        // business hours: Friday 17:30 is due Monday 10:00 (2026-10-09 is a Friday)
        let fri = Utc.with_ymd_and_hms(2026, 10, 9, 17, 30, 0).unwrap();
        assert_eq!(due_after("0 9-17 * * 1-5", &fri).unwrap(), Utc.with_ymd_and_hms(2026, 10, 12, 10, 0, 0).unwrap());
    }

    #[test]
    fn on_time_stays_ok() {
        let mut st = fresh(Some(t(12, 0, 10)));
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 1, 59), t(11, 0, 0)).unwrap(), None);
        assert_eq!(st.state, CState::Ok);
    }

    #[test]
    fn watch_retries_then_one_deadline_then_ok() {
        let base = t(11, 0, 0);
        let mut st = fresh(Some(t(12, 0, 10)));
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 2, 1), base).unwrap(), Some(Emit::Watch));
        assert_eq!((st.state, st.attempt), (CState::Watch, 1));
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 2, 20), base).unwrap(), None);
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 2, 31), base).unwrap(), Some(Emit::Watch));
        assert_eq!(st.attempt, 2);
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 4, 1), base).unwrap(), Some(Emit::Deadline));
        assert_eq!(st.state, CState::Deadline);
        // never repeated
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 30, 0), base).unwrap(), None);
        // a message: back to ok, saying where from
        st.last_seen = Some(t(12, 31, 0));
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 31, 5), base).unwrap(), Some(Emit::Ok { was: CState::Deadline }));
        assert_eq!((st.state, st.attempt), (CState::Ok, 0));
    }

    #[test]
    fn retry_zero_sends_watch_once() {
        let mut st = fresh(Some(t(12, 0, 10)));
        assert_eq!(step("* * * * *", 5, 0, &mut st, t(12, 2, 1), t(11, 0, 0)).unwrap(), Some(Emit::Watch));
        assert_eq!(step("* * * * *", 5, 0, &mut st, t(12, 4, 0), t(11, 0, 0)).unwrap(), None);
    }

    #[test]
    fn watch_zero_goes_straight_to_deadline() {
        let mut st = fresh(Some(t(12, 0, 10)));
        assert_eq!(step("* * * * *", 0, 30, &mut st, t(12, 2, 1), t(11, 0, 0)).unwrap(), Some(Emit::Deadline));
        assert_eq!(st.state, CState::Deadline);
        st.last_seen = Some(t(12, 3, 0));
        assert_eq!(step("* * * * *", 0, 30, &mut st, t(12, 3, 1), t(11, 0, 0)).unwrap(), Some(Emit::Ok { was: CState::Deadline }));
    }

    #[test]
    fn a_message_while_on_watch_recovers() {
        let mut st = fresh(Some(t(12, 0, 10)));
        assert_eq!(step("* * * * *", 2, 0, &mut st, t(12, 2, 1), t(11, 0, 0)).unwrap(), Some(Emit::Watch));
        st.last_seen = Some(t(12, 2, 30));
        assert_eq!(step("* * * * *", 2, 0, &mut st, t(12, 2, 35), t(11, 0, 0)).unwrap(), Some(Emit::Ok { was: CState::Watch }));
    }

    #[test]
    fn never_seen_counts_from_the_baseline() {
        let mut st = fresh(None);
        assert_eq!(step("* * * * *", 1, 0, &mut st, t(12, 1, 30), t(12, 0, 10)).unwrap(), None);
        assert_eq!(step("* * * * *", 1, 0, &mut st, t(12, 2, 1), t(12, 0, 10)).unwrap(), Some(Emit::Watch));
    }

    #[test]
    fn a_bad_cron_is_an_error() {
        let mut st = fresh(None);
        assert!(step("not a cron", 1, 0, &mut st, t(12, 0, 0), t(11, 0, 0)).is_err());
    }

    fn row(state: &str, since: Option<DateTime<Utc>>, attempt: i32, last_seen: Option<DateTime<Utc>>,
           watch_min: i32, retry_sec: i32) -> RpFnCron {
        RpFnCron { id: 9213, fn_id: 9203, cron: "* * * * *".into(), column_name: String::new(), column_value: None,
            cadence: true, timeout_sec: None, started_at: None, finished_at: None, error_msg: None,
            watch_min, retry_sec, watch_q: Some("/q/w".into()), deadline_q: Some("/q/d".into()),
            state: state.into(), last_seen, since, attempt, cadence_rt: None }
    }
    fn stored(state: CState, since: Option<DateTime<Utc>>, attempt: i32, last_seen: Option<DateTime<Utc>>) -> Stored {
        Stored { state, since, attempt, last_seen }
    }

    /// topology 92c C6: the old master put C on watch at 17:17:03 and the row
    /// replicated; the new master loaded the row at startup, when it was `ok`
    #[test]
    fn a_new_master_continues_the_stored_watch() {
        let base = t(17, 8, 47);
        let loaded = row("ok", None, 0, None, 3, 0);
        let s = stored(CState::Watch, Some(t(17, 17, 3)), 1, Some(t(17, 15, 54)));
        let mut st = runtime(&loaded, Some(&s));
        assert_eq!((st.state, st.since, st.attempt), (CState::Watch, Some(t(17, 17, 3)), 1));
        // takes over at 17:17:22: no second watch, `since` kept
        assert_eq!(step("* * * * *", 3, 0, &mut st, t(17, 17, 22), base).unwrap(), None);
        assert!(s.matches(&st));
        assert!(!needs_store(Some(&s), &st.clone(), &st));
        // the deadline on the old schedule, once
        assert_eq!(step("* * * * *", 3, 0, &mut st, t(17, 20, 2), base).unwrap(), None);
        assert_eq!(step("* * * * *", 3, 0, &mut st, t(17, 20, 3), base).unwrap(), Some(Emit::Deadline));
        assert_eq!(step("* * * * *", 3, 0, &mut st, t(17, 20, 8), base).unwrap(), None);

        // what it did before: the startup row, found overdue -> a NEW watch
        let mut old = runtime(&loaded, None);
        old.last_seen = Some(t(17, 15, 54));
        assert_eq!(step("* * * * *", 3, 0, &mut old, t(17, 17, 22), base).unwrap(), Some(Emit::Watch));
        assert_eq!(old.since, Some(t(17, 17, 22)));
    }

    #[test]
    fn the_own_runtime_wins_over_the_row_while_master() {
        let mut c = row("ok", None, 0, None, 2, 30);
        let mine = Cadence { state: CState::Deadline, since: Some(t(12, 4, 1)), attempt: 4, last_seen: Some(t(12, 0, 10)), last_sent: Some(t(12, 4, 1)) };
        c.cadence_rt = Some(mine.clone());
        // a stale replicated image of the row
        let s = stored(CState::Watch, Some(t(12, 2, 1)), 2, Some(t(12, 0, 10)));
        let st = runtime(&c, Some(&s));
        assert_eq!(st, mine);
        // ...and the row gets written back, though nothing moved this tick
        assert!(needs_store(Some(&s), &st, &st));
    }

    #[test]
    fn the_stored_retry_schedule_carries_on() {
        let base = t(11, 0, 0);
        // attempts 1 and 2 went out at 12:02:01 and ~12:02:31
        let s = stored(CState::Watch, Some(t(12, 2, 1)), 2, Some(t(12, 0, 10)));
        let mut st = runtime(&row("ok", None, 0, None, 2, 30), Some(&s));
        assert_eq!(st.last_sent, Some(t(12, 2, 31)));
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 2, 36), base).unwrap(), None);
        assert_eq!(step("* * * * *", 2, 30, &mut st, t(12, 3, 1), base).unwrap(), Some(Emit::Watch));
        assert_eq!(st.attempt, 3);
        // attempt 1 only: the next is a retry_sec after since
        let s1 = stored(CState::Watch, Some(t(12, 2, 1)), 1, None);
        assert_eq!(Cadence::from_stored(&s1, 30).last_sent, Some(t(12, 2, 1)));
    }

    /// topology 92c C3/C4: an `ok` after a deadline belongs on the deadline queue
    /// only, and a row with watch_min = 0 never sends anything to watch_q
    #[test]
    fn messages_go_to_the_queue_of_their_state() {
        let (w, d) = (Some("/q/w".to_string()), Some("/q/d".to_string()));
        assert_eq!(emit_queue(&Emit::Watch, &w, &d), &w);
        assert_eq!(emit_queue(&Emit::Deadline, &w, &d), &d);
        assert_eq!(emit_queue(&Emit::Ok { was: CState::Watch }, &w, &d), &w);
        assert_eq!(emit_queue(&Emit::Ok { was: CState::Deadline }, &w, &d), &d);

        let base = t(17, 8, 47);
        let mut st = runtime(&row("ok", None, 0, None, 0, 0), None);
        let mut sent = vec![];
        for (m, s) in [(9, 0), (10, 2), (10, 7), (12, 0), (16, 0)] {
            if let Some(e) = step("* * * * *", 0, 0, &mut st, t(17, m, s), base).unwrap() { sent.push(e); }
        }
        st.last_seen = Some(t(16, 16, 17)); // before since: still deadline
        assert_eq!(step("* * * * *", 0, 0, &mut st, t(17, 16, 20), base).unwrap(), None);
        st.last_seen = Some(t(17, 16, 17));
        if let Some(e) = step("* * * * *", 0, 0, &mut st, t(17, 16, 22), base).unwrap() { sent.push(e); }
        assert_eq!(sent, vec![Emit::Deadline, Emit::Ok { was: CState::Deadline }]);
        assert!(sent.iter().all(|e| emit_queue(e, &w, &d) == &d));
    }

    #[test]
    fn the_row_is_written_when_it_does_not_say_what_the_runtime_does() {
        let a = Cadence { state: CState::Ok, since: None, attempt: 0, last_seen: Some(t(12, 0, 10)), last_sent: None };
        let mut b = a.clone();
        b.state = CState::Watch; b.since = Some(t(12, 2, 1)); b.attempt = 1;
        let s_ok = stored(CState::Ok, None, 0, Some(t(12, 0, 0)));
        let s_watch = stored(CState::Watch, Some(t(12, 2, 1)), 1, Some(t(12, 0, 10)));
        assert!(needs_store(Some(&s_ok), &a, &b));       // a transition
        assert!(!needs_store(Some(&s_ok), &a, &a));      // nothing moved; last_seen is not ours to write
        assert!(!needs_store(Some(&s_watch), &b, &b));   // already stored
        assert!(needs_store(Some(&s_ok), &b, &b));       // the write was lost or overwritten
        assert!(needs_store(None, &a, &b));              // no row read: a change
        assert!(!needs_store(None, &b, &b));
    }

    #[test]
    fn a_runtime_from_an_earlier_term_is_dropped() {
        let log = slog::Logger::root(slog::Discard, slog::o!());
        let mut c = row("ok", None, 0, None, 3, 0);
        c.cadence_rt = Some(Cadence { state: CState::Watch, since: Some(t(12, 2, 1)), attempt: 1, last_seen: None, last_sent: None });
        let mut ctx = crate::cron::CronContext { crons: Default::default(), jobs: Default::default(), log,
            started: t(11, 0, 0), cadence_on: true, cadence_epoch: 1 };
        ctx.crons.insert(c.id, c);
        assert!(!ctx.new_term(1));
        assert!(ctx.crons.values().all(|c| c.cadence_rt.is_some()));
        assert!(ctx.new_term(2));
        assert!(ctx.crons.values().all(|c| c.cadence_rt.is_none()));
    }

    #[test]
    fn since_round_trips_through_a_timestamptz() {
        let now = Utc::now().trunc_subsecs(6);
        let back = DateTime::<Utc>::from_timestamp_micros(now.timestamp_micros()).unwrap();
        assert_eq!(now, back);
    }

    #[test]
    fn queues_and_keys() {
        assert!(is_queue("/q/orders"));
        assert!(is_queue("/queue/orders"));
        assert!(!is_queue("public.orders"));
        assert!(!is_queue("/q/"));
        assert_eq!(producer_key("/q/w", 7, t(12, 0, 0)), format!("/q/w/producer/7-{}", t(12, 0, 0).timestamp_millis()));
    }
}
