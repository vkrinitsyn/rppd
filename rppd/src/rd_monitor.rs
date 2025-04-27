#![allow(unused_imports)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;
use std::time::Duration;

use shims::Histogram;
use slog::{error, info};
use tokio::time::sleep;
use rppd_common::protogen::rppc::{StatusRequest, StatusResponse};
#[cfg(not(feature = "lib-embedded"))]
use crate::rd_config::{DWN_MASTER, MAX_ERRORS, SELECT_MASTER, UP_MASTER};
use crate::rd_config::{RppdNodeCluster, TIMEOUT_MS};

#[derive(Clone)]
#[allow(unused_variables, dead_code)]
pub(crate) struct ClusterStat {
    pub(crate) node_stat: BTreeMap<i32, NodeStat>,
    sort: BTreeMap<u64, BTreeSet<i32>>,
    /// self status
    pub(crate) this_stat: NodeStat,
}

impl Default for ClusterStat {
    #[inline]
    fn default() -> Self {
        ClusterStat {
            node_stat: Default::default(),
            sort: Default::default(),
            this_stat: Default::default(),
        }
    }
}

impl Default for NodeStat {
    #[inline]
    fn default() -> NodeStat {
        NodeStat {
            node_id: 0,
            queue: Histogram::new(shims::Config::default()),
            in_proc: Histogram::new(shims::Config::default()),
            weight: 0,
            self_master: false,
            online: false,
        }
    }
}

#[derive(Clone)]
#[allow(unused_variables, dead_code)]
pub(crate) struct NodeStat {
    node_id: i32,
    /// zero is better
    queue: Histogram,

    /// zero is better
    in_proc: Histogram,

    weight: u64,

    self_master: bool,
    online: bool,
}

impl NodeStat {
    /// set new value return previous
    #[inline]
    #[cfg(not(feature = "lib-embedded"))]
    pub(crate) fn weight(&mut self) -> u64 {
        let w = self.weight;
        self.weight = self.queue.median() + self.in_proc.median();
        w
    }

    #[inline]
    #[cfg(not(feature = "lib-embedded"))]
    pub(crate) fn new_from(s: StatusResponse) -> Self {
        let mut n = NodeStat::default();
        n.node_id = s.node_id;
        n.online = true;
        n.queue.append(s.queued as u64);
        n.in_proc.append(s.in_proc as u64);
        n
    }
}

impl ClusterStat {
    #[inline]
    #[cfg(not(feature = "lib-embedded"))]
    fn apply(&mut self, node: i32) {
        if !self.node_stat.contains_key(&node) {
            self.node_stat.insert(node, NodeStat::default());
        }
        if let Some(n) = self.node_stat.get_mut(&node) {
            let old_w = n.weight();
            if let Some(set) = self.sort.get_mut(&old_w) {
                let _ = set.remove(&node);
            }
            match self.sort.get_mut(&n.weight) {
                None => {
                    let _ = self.sort.insert(n.weight.clone(), new_set(node));
                }
                Some(set) => {
                    let _ = set.insert(node);
                }
            }
        }
    }
}

#[cfg(not(feature = "lib-embedded"))]
fn new_set(node: i32) -> BTreeSet<i32> {
    let mut set = BTreeSet::new();
    set.insert(node.clone());
    set
}

/*
impl Ord for NodeStat {
    #[inline]
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let w = self.weight();
        let o = other.weight();
        if w > o {
            std::cmp::Ordering::Greater
        } else if w < o {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Equal
        }
    }


}

impl Eq for NodeStat {}

impl PartialEq for NodeStat {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.weight() == other.weight()
    }
}

impl PartialOrd for NodeStat {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }

}
*/

impl RppdNodeCluster {

    /// call by exec on master node
    pub(crate) async fn pick_node(&self) -> Option<i32> {
        let stat = self.stat.read().await;
        for (_w, nodes) in &stat.sort {
            for id in nodes {
                if let Some(ok) = self.node_connections.read().await.get(id) {
                    if ok.is_ok() {
                        return Some(id.clone());
                    }
                }
            }
        }
        None
    }


    /// periodically check master node availability and consistency in DB and memory
    #[cfg(not(feature = "lib-embedded"))]
    pub(crate) async fn start_monitoring(&self) {
        let node_id = self.node_id.load(Ordering::Relaxed);
        info!(self.log, "start_monitoring node {}", node_id);
        let (schema, table) = { let cfg = self.cfg.read().await;
            (cfg.schema.clone(), cfg.table.clone())
        };

        let sql_id = SELECT_MASTER.replace("%SCHEMA%", schema.as_str())
            .replace("%TABLE%", table.as_str());
        assert!(MAX_ERRORS > 1);
        let mut prev_db_master = None;
        let mut errors = 0;

        loop {
            // check stored master id in DB -- might be changed because of network, restart etc,
            // DB is must have and DB have a trusted data, but DB data might be obsolete in case of master is down
            match sqlx::query_scalar::<_, i32>(sql_id.as_str()).fetch_one(&self.db()).await {
                Ok(master_id) => {
                    prev_db_master = Some(master_id); // just in case of use on reset in db
                    if node_id == master_id {
                        self.master.store(true, Ordering::Relaxed);
                    }
                    self.master_id.store(master_id, Ordering::Relaxed);

                    if self.master.load(Ordering::Relaxed) { // run over cluster to collect stats
                        // TODO update self statistics
                        // c.stat.write().await.this_stat.

                        let nodes: Vec<i32> = self.node_ids.read().await.values().map(|v| v.clone()).collect();
                        for id in nodes {
                            let mut err = None;
                            if let Some(node) = self.node_connections.read().await.get(&id) {
                                match node {
                                    Ok(n) => {
                                        match n.lock().await.status(StatusRequest {
                                            config_schema_table: schema.clone(),
                                            node_id: id.clone(), fn_log: None }).await {
                                            Ok(r) => {
                                                let r = r.into_inner();
                                                let mut stat = self.stat.write().await;
                                                match stat.node_stat.get_mut(&id) {
                                                    None => {
                                                        let s: NodeStat = NodeStat::new_from(r);
                                                        stat.node_stat.insert(id.clone(), s);
                                                    }
                                                    Some(s) => {
                                                        s.queue.append(r.queued as u64);
                                                        s.in_proc.append(r.in_proc as u64);
                                                    }
                                                };
                                                stat.apply(id);
                                            }
                                            Err(e) => err = Some(e.to_string())
                                        }
                                    }
                                    Err(_e) => { // TODO if node in error, try to re-connect
                                    }
                                }
                            }
                            if let Some(e) = err {
                                let _ = self.node_connections.write().await.insert(id.clone(), Err(e));
                            }
                        }
                    } else { //  call master for availability

                        if let Some(node) = self.node_connections.read().await.get(&master_id) {
                            match node {
                                Ok(n) => {
                                    match n.lock().await.status(StatusRequest {
                                        config_schema_table: schema.clone(),
                                        node_id: master_id, fn_log: None }).await {
                                        Ok(r) => {
                                            let r = r.into_inner();
                                            if !r.is_master {  // discrepancy
                                                errors += 1;
                                                error!(self.log, "discrepancy on call master - replied: 'not a master'");
                                            } else {
                                                errors = 0;
                                            }
                                        }
                                        Err(e) => { // master having error
                                            errors += 1;
                                            error!(self.log, "error on call master: {}", e);
                                        }
                                    }
                                }
                                Err(e) => { // master having error
                                    errors += 1;
                                    error!(self.log, "no connection to master host: {}", e);
                                }
                            }
                        }
                    }
                }
                Err(e) => { // scenarios: DB not available, no one marked as master: either wait a second than act
                    errors += 1;
                    error!(self.log, "can't load master from db {}", e);
                }
            }

            if errors >= MAX_ERRORS { // trying register self as master
                errors = 0;
                self.become_master(prev_db_master).await;
            }
            self.cronjob().await;
            self.cleanup_fn_logs().await;
            let _ = sleep(Duration::from_millis(5*TIMEOUT_MS)).await;
        }
    }

    #[cfg(feature = "lib-embedded")]
    pub(crate) async fn start_monitoring(&self) {
        loop {
            self.cronjob().await;
            self.cleanup_fn_logs().await;
            let _ = sleep(Duration::from_millis(5*TIMEOUT_MS)).await;
        }
    }


    #[cfg(not(feature = "lib-embedded"))]
    #[inline]
    pub(crate) async fn become_master(&self, prev_db_master: Option<i32>) {
        let (schema, table) = { let cfg = self.cfg.read().await;
            (cfg.schema.clone(), cfg.table.clone())
        };

        let sql_dwn = DWN_MASTER
            .replace("%SCHEMA%", schema.as_str())
            .replace("%TABLE%", table.as_str());
        let sql_up = UP_MASTER
            .replace("%SCHEMA%", schema.as_str())
            .replace("%TABLE%", table.as_str());

        if let Some(master_id) = prev_db_master { // unregister previous master
            if let Ok(_ok) = sqlx::query(sql_dwn.as_str()).bind(master_id).execute(&self.db()).await {
                info!(self.log, "removed previous load master from DB row ID = {}", master_id);
            }
        }

        let node_id = self.node_id.load(Ordering::Relaxed);
        match sqlx::query(sql_up.as_str()).bind(node_id).execute(&self.db()).await {
            Ok(_ok) => {
                self.master.store(true, Ordering::Relaxed);
                self.master_id.store(node_id, Ordering::Relaxed);
            }
            Err(e) => {
                error!(self.log, "marking self as master: {}", e);
            }
        }
    }

    /// TODO delete old records
    pub(crate) async fn cleanup_fn_logs(&self) {
        let sql = crate::rd_fn::DELETE_LOG.replace("%SCHEMA%", self.cfg.read().await.schema.as_str());

        if let Err(e) = sqlx::query(sql.as_str()).execute(&self.db()).await {
            error!(self.log, "Cleanup logs by [{}] {}", sql, e);
        }

    }
}

#[cfg(test)]
mod tests {
    #![allow(warnings, unused)]

    #[tokio::test]
    async fn test_compile() {
        assert!(true);
    }
}
