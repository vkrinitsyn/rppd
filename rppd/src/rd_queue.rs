use std::collections::{BTreeMap, HashMap, VecDeque};
use std::time::Instant;
use rppd_common::protogen::rppc::{fn_status, status_request, status_response, FnStatus, StatusFnsResponse};
use crate::py::PyCall;
use crate::rd_config::TopicDef;
use crate::rd_fn::{RpFnId, RpFnLog};

#[derive(Default, Debug, Clone)]
pub(crate) struct QueueType(BTreeMap<i32, HashMap<TopicDef, VecDeque<PyCall>>>);

impl QueueType {
    /// return back to the queue previously taken. uuid not set for topic event returning back
// split into method for the unit testing
    #[inline]
    pub(crate) fn return_back(&mut self, l: RpFnLog, topic: TopicDef) {
        // if let Some(uid) = &l.started {
        if let Some(f) = &l.fn_idp {
            if let Some(queue) = self.0.get_mut(&f.priority) {
                if let Some(line) = queue.get_mut(&topic) {
                    // call must be InProgress with uuid match
                    if let Some(x) = line.pop_back() {
                        match x {
                            PyCall::InProgressSaved(_id, _t) => {
                                // if &u != uid {
                                // taken more than one events to execute from the same queue
                                // } // else <- this is an expected only path!
                            }
                            _ => { // if not or not match, return this back
                                line.push_back(x);
                            }
                        }
                        line.push_back(PyCall::Local(l));
                    }
                } // all other conditions handle by q.put_one()
            }
        }
    }

    /// build a queue
    #[inline]
    pub(crate) fn put_one(&mut self, l: RpFnLog, topic: TopicDef, fid: RpFnId, push_back: bool) {
        let l = if l.fn_idp.is_none() {
            RpFnLog { fn_idp: Some(fid.clone()), ..l }
        } else { l };
        match self.0.get_mut(&fid.priority) {
            None => {
                let mut queue = HashMap::new();
                queue.insert(topic, l.to_line());
                self.0.insert(fid.priority.clone(), queue);
            }
            Some(queue) => {
                match queue.get_mut(&topic) {
                    None => {
                        queue.insert(topic, l.to_line());
                    }
                    Some(line) => {
                        if push_back {
                            line.push_back(PyCall::Local(l));
                        } else {
                            line.push_front(PyCall::Local(l));
                        }
                    }
                }
            }
        }
    }

    // for the unit testing
    #[inline]
    #[allow(unused_variables, unused_mut, dead_code)]
    pub(crate) fn pick_one(&mut self) -> Option<RpFnLog> {
        let mut result = None;
        'it: for queue in self.0.values_mut() {
            for line in queue.values_mut() {
                if line.is_empty() { continue; }
                let calls = line.pop_back().unwrap();
                if match &calls {
                    PyCall::Local(_) => false,
                    _ => true,
                } {
                    line.push_back(calls); // return unchanged
                    break; // take next function topic
                } else {
                    if let PyCall::Local(mut call) = calls {
                        if call.is_queue(false) {
                            let py = PyCall::InProgressSaved(call.id, call.started.clone().unwrap_or(Instant::now()));
                            line.push_back(py);
                        }
                        result = Some(call.to_owned());
                        break 'it; // use this event
                    }
                }
            }
        }

        // cleanup
        #[allow(unused_mut)]
        self.0.retain(|_k, mut v| {
            v.retain(|_x, y| !y.is_empty());
            !v.is_empty()
        });

        result
    }

    /// state
    #[inline]
    #[allow(unused_variables, dead_code)]
    pub(crate) async fn status(&self, fn_log: Option<status_request::FnLog>) -> Option<status_response::FnLog> {
        let fn_log = match fn_log {
            None => {
                return None;
            }
            Some(fn_log) => fn_log
        };
        let mut uuid = Vec::new();
        let mut qq = 0;
        for (k, v) in self.0.iter() {
            for q in v.values() {
                for c in q {
                    qq += 1;
                    match c {
                        PyCall::Local(l) => {
                            match &fn_log {
                                status_request::FnLog::FnLogId(fn_log_id) => {
                                    if &l.id == fn_log_id {
                                        return Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::QueuePos(qq)) }));
                                    }
                                }
                                status_request::FnLog::Uuid(fn_log_uuid) => {
                                    if let Some(u) = &l.uuid {
                                        let u = u.to_string();
                                        if fn_log_uuid.len() <= 3 {
                                            uuid.push(u)
                                        } else if fn_log_uuid == &u {
                                            return Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::QueuePos(qq)) }));
                                        }
                                    }
                                }
                            }
                        }
                        PyCall::InProgressSaved(l, u) => if let status_request::FnLog::FnLogId(fn_log_id) = &fn_log {
                            if l == fn_log_id {
                                return Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::InProcSec(u.elapsed().as_secs() as u32)) }));
                            }
                        }
                        PyCall::InProgressUnSaved(l, t) => if let status_request::FnLog::Uuid(fn_log_uuid) = &fn_log {
                            let u = fn_log_uuid.to_string();
                            if fn_log_uuid.len() <= 3 {
                                uuid.push(u)
                            } else if fn_log_uuid == &u {
                                return Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::InProcSec(t.elapsed().as_secs() as u32)) }));
                            }
                        }
                        PyCall::RemoteSaved(n, l) => {
                            match &fn_log {
                                status_request::FnLog::FnLogId(fn_log_id) => {
                                    if l == fn_log_id {
                                        return Some(status_response::FnLog::Status(FnStatus { status: Some(fn_status::Status::RemoteHost(*n)) }));
                                    }
                                }
                                _ => {}
                            }
                        }
                        PyCall::RemoteUnSaved(_, _) => {
                            //
                        }
                    }
                }
            }
        }
        match fn_log {
            status_request::FnLog::FnLogId(_) => Some(status_response::FnLog::Status(FnStatus { status: None })),
            status_request::FnLog::Uuid(_) => Some(status_response::FnLog::Uuid(StatusFnsResponse { uuid }))
        }
    }

    /// queued, in progress
    #[inline]
    pub(crate) fn size(&self) -> (usize, usize) {
        let mut cnt_l = 0usize;
        let mut cnt_i = 0usize;
        for v in self.0.values() {
            for q in v.values() {
                for l in q {
                    match l {
                        PyCall::Local(_) => { cnt_l += 1; }
                        PyCall::InProgressSaved(_, _)
                        | PyCall::InProgressUnSaved(_, _) => { cnt_i += 1; }
                        _ => {}
                    }
                }
            }
        }
        (cnt_l, cnt_i)
    }
}


#[cfg(test)]
mod tests {
    #![allow(warnings, unused)]

    use std::time::Instant;

    use super::*;

    #[tokio::test]
    async fn test_p() {
        let mut q: QueueType = Default::default();
        assert!(q.pick_one().is_none());

        let mut lines = HashMap::new();
        let mut line = VecDeque::new();
        line.push_front(PyCall::InProgressSaved(0, Instant::now()));
        lines.insert("".to_string(), line);
        q.0.insert(1, lines);
        assert!(q.pick_one().is_none());

        let mut lines = HashMap::new();
        let mut line = VecDeque::new();
        line.push_front(PyCall::InProgressSaved(0, Instant::now()));
        lines.insert("".to_string(), line);
        q.0.insert(1, lines);
        assert!(q.pick_one().is_none());

        q.put_one(RpFnLog::default(), "".to_string(), RpFnId::default(), false);
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_none());

        q.put_one(RpFnLog::default(), "".to_string(), RpFnId::default(), false);
        q.put_one(RpFnLog::default(), "".to_string(), RpFnId::default(), false);
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_none());

        q.put_one(RpFnLog::default(), "".to_string(), RpFnId::default(), false);
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_none());
    }
}