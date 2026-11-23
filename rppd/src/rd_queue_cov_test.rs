//! Coverage of the queue's line rules: returns, marks, picks, status and sizes.

use super::*;
use std::time::Instant;
use rppd_common::protogen::rppc::{fn_status, status_request, status_response};

fn fid(id: i32, priority: i32, queue: bool) -> RpFnId {
    RpFnId { id, queue, priority, save: false, trig_value: false }
}

fn ev(id: i64, f: &RpFnId) -> RpFnLog {
    RpFnLog { id, fn_idp: Some(f.clone()), ..RpFnLog::default() }
}

fn status_of(r: Option<status_response::FnLog>) -> Option<fn_status::Status> {
    match r {
        Some(status_response::FnLog::Status(s)) => s.status,
        _ => panic!("expected a status response"),
    }
}

#[test]
fn put_one_builds_priorities_and_lines_in_order() {
    let a = fid(1, 1, true);
    let b = fid(2, 2, true);
    let mut q = QueueType::default();
    // a new priority, then a new topic in it, then the same topic: front and back
    q.put_one(ev(1, &a), "t1".into(), a.clone(), false);
    q.put_one(ev(2, &a), "t1".into(), a.clone(), false);
    q.put_one(ev(3, &a), "t1".into(), a.clone(), true);
    q.put_one(ev(4, &b), "t2".into(), b.clone(), false);
    assert_eq!(q.size(), (4, 0));
    // an event without fn_idp takes the given id
    let bare = RpFnLog { id: 5, ..RpFnLog::default() };
    q.put_one(bare, "t2".into(), b.clone(), true);
    assert_eq!(q.size(), (5, 0));
    // a pick takes the back of the line: event 3 was put at the back, the others at the front
    let first = q.pick_one().unwrap();
    assert_eq!(first.id, 3);
}

#[test]
fn return_back_to_a_line_in_progress_replaces_its_mark() {
    let a = fid(1, 1, true);
    let mut q = QueueType::default();
    q.put_one(ev(1, &a), "a".into(), a.clone(), false);
    let got = q.pick_one().unwrap();
    assert_eq!(got.id, 1);
    // the mark is the line's back: returning the event takes it and puts the event there
    q.return_back(got, "a".into());
    assert_eq!(q.size(), (1, 0));
    assert_eq!(q.pick_one().unwrap().id, 1);
}

#[test]
fn return_back_keeps_a_line_entry_that_is_no_mark() {
    let a = fid(1, 1, true);
    let mut q = QueueType::default();
    q.put_one(ev(1, &a), "a".into(), a.clone(), false);
    q.put_one(ev(2, &a), "a".into(), a.clone(), true);
    // the back of the line is a local call, not a mark: it is pushed back, the event too
    let x = RpFnLog { id: 9, ..ev(9, &a) };
    q.return_back(x, "a".into());
    assert_eq!(q.size(), (3, 0));
}

#[test]
fn return_back_without_a_line_or_a_function_starts_one() {
    let a = fid(1, 1, true);
    let mut q = QueueType::default();
    // no priority in the map yet
    q.return_back(ev(7, &a), "x".into());
    // priority known, topic not
    q.return_back(ev(8, &a), "y".into());
    assert_eq!(q.size(), (2, 0));
    // the default function id is still a function: the event starts a line of its own
    q.return_back(RpFnLog { id: 10, ..RpFnLog::default() }, "z".into());
    assert_eq!(q.size(), (3, 0));
}

#[test]
fn done_and_unmark_take_only_the_matching_mark() {
    let a = fid(1, 1, true);
    let mut q = QueueType::default();
    // no fn id: nothing to do
    q.done(&RpFnLog::default(), "a".into());
    // no priority, no line: nothing
    q.unmark(5, 1, "a".into());
    q.unmark(1, 1, "missing".into());
    q.put_one(ev(1, &a), "a".into(), a.clone(), false);
    q.put_one(ev(2, &a), "a".into(), a.clone(), false);
    let got = q.pick_one().unwrap();
    assert_eq!(got.id, 1);
    // a mark for another event leaves this one's mark where it is
    q.unmark(1, 99, "a".into());
    assert!(q.pick_one().is_none());
    q.done(&got, "a".into());
    assert_eq!(q.pick_one().unwrap().id, 2);
}

#[test]
fn pick_one_skips_a_busy_line_and_removes_empty_ones() {
    let a = fid(1, 1, true);
    let b = fid(2, 1, false); // not a queue: picked without a mark
    let mut q = QueueType::default();
    q.put_one(ev(1, &a), "a".into(), a.clone(), false);
    // an in-progress mark leaves a busy line with no local call
    assert_eq!(q.pick_one().unwrap().id, 1);
    q.put_one(ev(2, &b), "b".into(), b.clone(), false);
    q.put_one(ev(3, &a), "a".into(), a.clone(), false);
    // a busy line "a" is skipped in favour of "b" (not a queue, no mark)
    let next = q.pick_one().unwrap();
    assert_eq!(next.id, 2);
    assert_eq!(q.size().1, 1, "only line a's mark: a non queue event leaves none");
    assert_eq!(q.pick_one().map(|c| c.id), None, "line a is busy");
    // no fn id: the queue flag defaults to the caller's value
    let mut r = QueueType::default();
    r.put_one(RpFnLog { id: 4, ..RpFnLog::default() }, "t".into(), fid(3, 1, false), false);
    assert_eq!(r.pick_one().unwrap().id, 4);
    assert_eq!(r.size(), (0, 0));
}

#[tokio::test]
async fn status_reports_queue_position_progress_and_remote() {
    let a = fid(1, 1, true);
    let mut q = QueueType::default();
    q.put_one(ev(1, &a), "a".into(), a.clone(), false);
    q.put_one(ev(2, &a), "a".into(), a.clone(), false);
    let u = uuid::Uuid::new_v4();
    let uq = RpFnLog { id: 3, uuid: Some(u), fn_idp: Some(a.clone()), ..RpFnLog::default() };
    q.put_one(uq, "b".into(), fid(2, 2, true), true);
    // no request: nothing
    assert!(q.status(None).await.is_none());
    // queue position of a local call
    let pos = q.status(Some(status_request::FnLog::FnLogId(2))).await;
    assert!(matches!(status_of(pos), Some(fn_status::Status::QueuePos(_))));
    // an unknown id: a status without a value
    let none = q.status(Some(status_request::FnLog::FnLogId(77))).await;
    assert!(status_of(none).is_none());
    // a call whose uuid is given in full is found by it
    let full = q.status(Some(status_request::FnLog::Uuid(u.to_string()))).await;
    assert!(matches!(status_of(full), Some(fn_status::Status::QueuePos(_))));
    // a short uuid list collects the ids of the local calls that have one
    let listed = q.status(Some(status_request::FnLog::Uuid("ab".into()))).await;
    match listed {
        Some(status_response::FnLog::Uuid(l)) => assert_eq!(l.uuid.len(), 1),
        _ => panic!("uuid list"),
    }
    // a full uuid nobody has: no position
    let unknown = q.status(Some(status_request::FnLog::Uuid(uuid::Uuid::new_v4().to_string()))).await;
    assert!(matches!(unknown, Some(status_response::FnLog::Uuid(l)) if l.uuid.is_empty()));

    // in progress: saved and unsaved marks, a remote host, a remote unsaved call
    let mut p = QueueType::default();
    p.0.entry(1).or_default().insert("a".into(), VecDeque::from(vec![
        PyCall::InProgressSaved(5, Instant::now()),
        PyCall::RemoteSaved(3, 6),
        PyCall::RemoteUnSaved(4, uuid::Uuid::new_v4()),
    ]));
    let inproc = p.status(Some(status_request::FnLog::FnLogId(5))).await;
    assert!(matches!(status_of(inproc), Some(fn_status::Status::InProcSec(_))));
    let remote = p.status(Some(status_request::FnLog::FnLogId(6))).await;
    assert!(matches!(status_of(remote), Some(fn_status::Status::RemoteHost(3))));
    // a remote unsaved call is never matched by id
    let nothing = p.status(Some(status_request::FnLog::FnLogId(9))).await;
    assert!(status_of(nothing).is_none());

    let mut un = QueueType::default();
    let mine = uuid::Uuid::new_v4();
    un.0.entry(1).or_default().insert("a".into(), VecDeque::from(vec![
        PyCall::InProgressUnSaved(mine, Instant::now()),
    ]));
    let by_uuid = un.status(Some(status_request::FnLog::Uuid(mine.to_string()))).await;
    assert!(matches!(status_of(by_uuid), Some(fn_status::Status::InProcSec(_))));
    let short = un.status(Some(status_request::FnLog::Uuid("x".into()))).await;
    assert!(matches!(short, Some(status_response::FnLog::Uuid(l)) if l.uuid.len() == 1));
    // a remote unsaved one listed by uuid is not in progress
    let mut rem = QueueType::default();
    rem.0.entry(1).or_default().insert("a".into(), VecDeque::from(vec![
        PyCall::RemoteUnSaved(2, uuid::Uuid::new_v4()),
    ]));
    let r = rem.status(Some(status_request::FnLog::Uuid(uuid::Uuid::new_v4().to_string()))).await;
    assert!(matches!(r, Some(status_response::FnLog::Uuid(l)) if l.uuid.is_empty()));
}

#[test]
fn size_counts_local_and_in_progress() {
    let mut q = QueueType::default();
    q.0.entry(1).or_default().insert("a".into(), VecDeque::from(vec![
        PyCall::InProgressSaved(1, Instant::now()),
        PyCall::InProgressUnSaved(uuid::Uuid::new_v4(), Instant::now()),
        PyCall::RemoteSaved(1, 2),
        PyCall::Local(RpFnLog::default()),
    ]));
    assert_eq!(q.size(), (1, 2));
}
