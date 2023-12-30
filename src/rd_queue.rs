use std::collections::{BTreeMap, HashMap, VecDeque};
use uuid::Uuid;
use crate::rd_config::TopicDef;
use crate::rd_fn::{RpFnId, RpFnLog};
use crate::py::PyCall;

#[derive(Default, Debug, Clone)]
pub(crate) struct QueueType (BTreeMap<i32, HashMap<TopicDef, VecDeque<PyCall>>>);

impl QueueType {
    /// return back to the queue previously taken. uuid not set for topic event returning back
// split into method for the unit testing
    #[inline]
    pub(crate) fn return_back(&mut self, l: RpFnLog, topic: TopicDef) {
        if let Some(uid) = &l.uid {
            if let Some(f) = &l.fn_idp {
                if let Some(queue) = self.0.get_mut(&f.priority) {
                    if let Some(line) = queue.get_mut(&topic) {
                        // call must be InProgress with uuid match
                        if let Some(x) = line.pop_back() {
                            match x {
                                PyCall::InProgress(u) => {
                                    if &u != uid {
                                        // taken more than one events to execute from the same queue
                                    } // else <- this is an expected only path!
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
    pub(crate) fn pick_one(&mut self) -> Option<RpFnLog> {
        let mut result = None;
        'it: for queue in self.0.values_mut() {
            for line in queue.values_mut() {
                if line.is_empty() { continue; }
                let calls = line.pop_back().unwrap();
                if match &calls {
                    PyCall::Local(call) => false,
                    PyCall::InProgress(_) => true,
                    _ => {
                        break; // todo not defined yet for remote host execution
                    }
                } {
                    line.push_back(calls); // return unchanged
                    break; // take next function topic
                } else {
                    if let PyCall::Local(mut call) = calls {
                        if call.is_queue(false) {
                            let py = PyCall::InProgress(call.uid.clone().unwrap_or(Uuid::new_v4()));
                            line.push_back(py);
                        }
                        result = Some(call.to_owned());
                        break 'it; // use this event
                    }
                }
            }
        }

        // cleanup
        self.0.retain(|_k, mut v| {
            v.retain(|_x, y| !y.is_empty());
            !v.is_empty()
        });

        result
    }

    /// build a queue
    #[inline]
    pub(crate) fn size(&self) -> (usize, usize) {
        let mut cnt_l = 0usize;
        let mut cnt_i = 0usize;
        for v in self.0.values() {
            for q in v.values() {
                for l in q {
                    match l {
                        PyCall::Local(_) => { cnt_l += 1; },
                        PyCall::InProgress(_) => { cnt_i += 1; },
                        _ => {},
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
    use super::*;

    #[tokio::test]
    async fn test_p() {
        let mut q:QueueType = Default::default();
        assert!(q.pick_one().is_none());

        let mut lines = HashMap::new();
        let mut line = VecDeque::new();
        line.push_front(PyCall::InProgress(Uuid::new_v4()));
        lines.insert("".to_string(), line);
        q.0.insert(1, lines);
        assert!(q.pick_one().is_none());

        let mut lines = HashMap::new();
        let mut line = VecDeque::new();
        line.push_front(PyCall::InProgress(Uuid::new_v4()));
        lines.insert("".to_string(), line);
        q.0.insert(1, lines);
        assert!(q.pick_one().is_none());

        q.put_one(RpFnLog::default(), "".to_string(),RpFnId::default(), false);
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_none());

        q.put_one(RpFnLog::default(), "".to_string(), RpFnId::default(), false);
        q.put_one(RpFnLog::default(), "".to_string(), RpFnId::default(), false);
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_none());

        q.put_one(RpFnLog::default(), "".to_string(),RpFnId::default(), false);
        assert!(q.pick_one().is_some());
        assert!(q.pick_one().is_none());

    }

}