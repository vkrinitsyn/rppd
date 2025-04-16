/*%LPH%*/

#![allow(unused_variables)]

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::ffi::CString;
use std::time::Instant;

use chrono::Utc;
use pyo3::PyErr;
use pyo3::exceptions::PyTypeError;
use slog::{error, Logger};
use sqlx::{Pool, Postgres};
use uuid::Uuid;
use rppd_common::protogen::rppc::{db_event_request, pk_column, DbEventRequest, PkColumn};
use crate::py::PyCall;
use crate::rd_config::{RppdNodeCluster, TopicType};

pub(crate) const SELECT_FN: &str = "select id, code, checksum, schema_table, topic, queue, cleanup_logs_min, priority, fn_logging, verbose_debug from %SCHEMA%.rppd_function ";

/// Rust Python Function
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct RpFn {
    pub(crate) id: i32,
    pub(crate) code: String,
    pub(crate) checksum: String,
    /// The rppd_event() must trigger on the corresponding "schema_table" to fire the function, 
    /// see topic to identify execution order';
    pub(crate) schema_table: String,
    /// Options are:
    /// - "": queue/topic per table (see schema_table);
    /// - ".{column}": queue per value of the column "id" in this table, the value type must be int;
    /// - "{any_name}": global queue i.e. multiple tables can share the same queue/topic';
    pub(crate) topic: String,
    /// max concurrent event execution on the topic to make queue
    /// 'if queue, then perform consequence events execution for the same topic. 
    /// Ex: if the "topic" is ".id" and not queue, then events for same row will execute in parallel';
    pub(crate) queue: bool,
    pub(crate) cleanup_logs_min: i32,
    /// queue priority
    pub(crate) priority: i32,
    pub(crate) fn_logging: bool,
    pub(crate) verbose_debug: bool,
    // env json, -- TODO reserved for future usage: db pool (read only)/config python param name prefix (mapping)
    // sign json -- TODO reserved for future usage: approve sign, required RSA private key on startup config

}

impl RpFnId {
    pub(crate) fn fromf(f: &RpFn) -> Self {
        RpFnId {
            id: f.id,
            queue: f.queue,
            priority: f.priority,
            save: f.fn_logging,
            trig_value: f.is_dot(),
        }
    }
}

#[derive(Debug, Clone)]
#[allow(unused_variables, dead_code)]
pub struct RpFnId {
    pub(crate) id: i32,
    pub(crate) queue: bool,
    pub(crate) priority: i32,
    pub(crate) save: bool,
    /// The value of column to trigger if topic is ".{column}". 
    /// Use stored value to load on restore queue on startup and continue. 
    /// Must be column type int to continue otherwise will save null and not able to restore.
    pub(crate) trig_value: bool,
}


impl Default for RpFnId {
    fn default() -> Self {
        RpFnId {
            id: 0,
            queue: false,
            priority: 0,
            save: false,
            trig_value: false,
        }
    }
}

impl Ord for RpFnId {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.id.cmp(&other.id)
    }

    #[inline]
    fn max(self, other: Self) -> Self where Self: Sized {
        if self.id < other.id { self } else { other }
    }

    #[inline]
    fn min(self, other: Self) -> Self where Self: Sized {
        if self.id > other.id { self } else { other }
    }

    #[inline]
    fn clamp(self, min: Self, max: Self) -> Self where Self: Sized, Self: PartialOrd {
        if self.id < min.id { min } else if self.id > max.id { max } else { self }
    }
}

impl PartialOrd for RpFnId {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for RpFnId {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for RpFnId {}

/// Rust Python Function Log
#[derive(sqlx::FromRow, PartialEq, Debug, Clone)]
pub struct RpFnLog {
    pub(crate) id: i64,
    /// the host where the even been executed
    pub(crate) node_id: i32,
    /// schema.table (topic)
    pub(crate) fn_id: i32,
    #[sqlx(skip)] /// ID or uuid. Must set while queue executing
    pub(crate) started: Option<Instant>,
    #[sqlx(skip)] /// ID or uuid. Must set while queue executing
    pub(crate) uuid: Option<Uuid>,
    #[sqlx(skip)] /// transient copy from RpFn
    pub(crate) fn_idp: Option<RpFnId>,

    pub(crate) took_ms: Option<i64>,

    pub(crate) trig_value: Option<sqlx::types::Json<HashMap<String, String>>>,
    #[sqlx(skip)] /// use for etcd KV
    pub(crate) key: Option<String>,
    #[sqlx(skip)] /// use for etcd KV
    pub(crate) value: Option<Vec<u8>>,
    #[sqlx(skip)] /// transient copy from RpFn
    pub(crate) rn_fn_id: Option<RpFnId>,

    /// DbAction
    pub(crate) trig_type: i32,
    pub(crate) started_at: chrono::DateTime<Utc>,
    pub(crate) error_msg: Option<String>,
}

impl Default for RpFnLog {
    fn default() -> Self {
        RpFnLog {
            id: 0,
            node_id: 0,
            fn_id: 0,
            started: None,
            uuid: None,
            fn_idp: Some(RpFnId::default()),
            took_ms: None,
            trig_value: None,
            key: None,
            value: None,
            rn_fn_id: None,
            trig_type: 0,
            started_at: Utc::now(),
            error_msg: None,
        }
    }
}


impl RpFn {
    /// topic starts with dot, means topic value required
    #[inline]
    pub(crate) fn is_dot(&self) -> bool {
        self.topic.len() > 0 && self.topic.as_bytes()[0] == b'.'
    }

    #[inline]
    pub(crate) fn to_pk_names(&self) -> HashSet<String> {
        let mut names = HashSet::new();
        let n: Vec<&str> = self.topic.split(".").collect();
        for pk in n {
            if pk.len() > 0 {
                names.insert(pk.to_string());
            }
        }
        names
    }

    /// create real topic name
    #[inline]
    pub(crate) fn to_topic(&self, l: &RpFnLog) -> TopicType {
        if self.topic.len() == 0 {
            self.schema_table.clone()
        } else if self.is_dot() {
            let mut name = String::new();
            if let Some(v) = &l.trig_value {
                let names = self.to_pk_names();
                for (k, v) in &v.0 {
                    if names.contains(k) {
                        name.push_str(format!(".{}={}", k, v).as_str());
                    }
                }
            }
            if name.len() == 0 {
                format!("{}{}=NULL", self.schema_table, self.topic)
            } else {
                format!("{}{}", self.schema_table, name)
            }
        } else {
            self.topic.clone()
        }
    }

    #[inline]
    pub(crate) fn to_repeat_result(&self) -> PkColumn {
        PkColumn {
            column_name: self.topic.clone().split_off(1),
            column_type: 0,
            pk_value: None,
        }
    }

    #[inline]
    pub(crate) async fn merge(&mut self, f: RpFn, cluster: &RppdNodeCluster) {
        if f.topic != self.topic {
            self.topic = f.topic;
            let mut map = cluster.fn_id.write().await;
            match map.get_mut(&self.topic) {
                None => {
                    let mut set = BTreeSet::new();
                    set.insert(RpFnId::fromf(&self));
                    map.insert(self.topic.clone(), set);
                }
                Some(set) => {
                    set.insert(RpFnId::fromf(&self));
                }
            }
        }

        if f.schema_table != self.schema_table {
            self.watch_merge(Some(&f.schema_table), &cluster).await;
            
            self.schema_table = f.schema_table;
            let mut map = cluster.fn_tt.write().await;
            match map.get_mut(&self.schema_table) {
                None => {
                    let mut set = HashSet::new();
                    set.insert(self.topic.clone());
                    map.insert(self.schema_table.clone(), set);
                }
                Some(set) => {
                    set.insert(self.topic.clone());
                }
            }
        }

        if f.code != self.code {
            self.code = f.code;
        }
        if f.cleanup_logs_min != self.cleanup_logs_min {
            self.cleanup_logs_min = f.cleanup_logs_min;
        }
        if f.queue != self.queue {
            self.queue = f.queue;
        }
        if f.checksum != self.checksum {
            self.checksum = f.checksum;
        }
    }

    #[inline]
    pub(crate) fn err_msg(&self, err: PyErr, log: &RpFnLog) -> String {
        let mut input = String::new();
        match &log.trig_value {
            None => { input = "".to_string(); }
            Some(map) => for (k, v) in map.0.iter() {
                if input.len() > 0 {
                    input.push_str(", ");
                }
                input.push_str(format!("{}={}", k, v).as_str());
            }
        };
        let msg = if self.verbose_debug {
            format!("python:[\n{}\n] input: ({})", self.code, input)
        } else {
            "".to_string()
        };
        format!("{} on run [{}]@{}: {}", err, self.schema_table, self.topic, msg)
    }

    #[inline]
    pub(crate) fn code(&self) -> Result<CString, PyErr> {
        CString::new(self.code.as_str())
            .map_err(|e| PyErr::new::<PyTypeError, _>(format!("Code on Fn#{} failed to read: {}", self.id, e)))
    }
}

const SELECT_LOG: &str = "select id, node_id, fn_id, trig_value, trig_type, started_at, took_ms, error_msg from %SCHEMA%.rppd_function_log ";

const INSERT_LOG_V: &str = "insert into %SCHEMA%.rppd_function_log (node_id, fn_id, trig_type, trig_value) values ($1, $2, $3, $4) returning id";
const INSERT_LOG: &str = "insert into %SCHEMA%.rppd_function_log (node_id, fn_id, trig_type) values ($1, $2, $3) returning id";

pub(crate) const DELETE_LOG: &str = "delete from %SCHEMA%.rppd_function_log fnl using %SCHEMA%.rppd_function fn \
 where fnl.fn_id = fn.id and cleanup_logs_min > 0 and started_at < current_timestamp - make_interval(mins => cleanup_logs_min)";

impl RpFnLog {
    #[inline]
    pub(crate) fn insert_v(schema: &String) -> String {
        INSERT_LOG_V.replace("%SCHEMA%", schema.as_str())
    }
    #[inline]
    pub(crate) fn insert(schema: &String) -> String {
        INSERT_LOG.replace("%SCHEMA%", schema.as_str())
    }

    #[inline]
    pub(crate) fn select(schema: &String) -> String {
        SELECT_LOG.replace("%SCHEMA%", if schema.len() == 0 { "public" } else { schema.as_str() })
    }

    /// prefix is " where " OR " and "
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn select_sql(&self, prefix: &str) -> String {
        let mut sql = String::new();
        if let Some(map) = &self.trig_value {
            let mut b = false;
            for (k, v) in map.iter() {
                if !b {
                    sql.push_str(prefix); // " where "
                    sql.push(' ');
                    b = true;
                } else {
                    sql.push_str(" and ");
                }
                sql.push_str(k.as_str());
                sql.push_str(" = '");
                sql.push_str(esca(v).as_str());
                sql.push_str("' ");
            }
        }
        sql
    }

    #[inline]
    pub(crate) async fn update(&self, took: i64, r: Option<String>, db: Pool<Postgres>, schema: &String, log: &Logger) {
        if self.id == 0 { return; }
        let sql = "update %SCHEMA%.rppd_function_log set took_ms = $1, error_msg = $2 where id = $3";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sql.as_str())
            .bind(took)
            .bind(r)
            .bind(self.id)
            .execute(&db).await {
            error!(log, "{}", e);
        }
    }

    #[inline]
    pub(crate) async fn update_err(&self, r: String, db: Pool<Postgres>, schema: &String, log: &Logger) {
        error!(log, "update_err {}", r);

        if self.id == 0 { return; }
        let sql = "update %SCHEMA%.rppd_function_log set error_msg = $1 where id = $2";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sql.as_str())
            .bind(Some(r))
            .bind(self.id)
            .execute(&db).await {
            error!(log, "{}", e);
        }
    }

    #[inline]
    pub(crate) fn is_queue(&self, def: bool) -> bool {
        match &self.fn_idp {
            None => def,
            Some(f) => f.queue
        }
    }

    #[inline]
    pub(crate) fn to_line(self) -> VecDeque<PyCall> {
        let mut line = VecDeque::new();
        line.push_front(PyCall::Local(self));
        line
    }

    #[inline]
    pub(crate) fn to_event(&self, table_name: String, node_id: i32) -> DbEventRequest {
        let mut pks = Vec::new();
        if let Some(val) = &self.trig_value {
            for (c, v) in val.0.iter() {
                let (column_type, pk_value) = match v.parse::<i32>() {
                    Ok(v) => (0, Some(pk_column::PkValue::IntValue(v))),
                    Err(_) => match v.parse::<i64>() {
                        Ok(v) => (1, Some(pk_column::PkValue::BigintValue(v))),
                        Err(_) => (0, None)
                    }
                };
                pks.push(PkColumn {
                    column_name: c.clone(),
                    column_type,
                    pk_value,
                });
            }
        }
        DbEventRequest {
            table_name,
            event_type: self.trig_type,
            id_value: true,
            pks,
            optional_caller: Some(db_event_request::OptionalCaller::CallBy(node_id)),
        }
    }
}

#[inline]

#[allow(dead_code)]
fn esca(input: &String) -> String {
    if input.len() == 0 {
        "".to_string()
    } else if input.as_str() == "'" {
        "''".to_string()
    } else {
        let i: Vec<&str> = input.split("'").collect();
        if i.len() == 1 {
            input.to_string()
        } else {
            let mut x = String::new();
            for a in 0..i.len() {
                if !(a == 0 && i[a].len() == 0) {
                    x.push_str("''");
                    x.push_str(i[a]);
                }
            }
            x
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(warnings, unused)]

    use std::fs;

    use sqlx::postgres::PgPoolOptions;

    use super::*;

    const DB_URL_FILE: &str = "test_db_url.local";

    #[tokio::test]
    async fn test_p() {
        let v = "a".to_string();
        assert_eq!(esca(&v).as_str(), "a");

        let v = "'a'".to_string();
        assert_eq!(esca(&v).as_str(), "''a''");

        let v = "'a''".to_string();
        assert_eq!(esca(&v).as_str(), "''a''''");

        let v = "'a''b'".to_string();
        assert_eq!(esca(&v).as_str(), "''a''''b''");
    }

    #[tokio::test]
    async fn test_db() -> Result<(), String> {
        let url = fs::read_to_string(DB_URL_FILE)
            .map_err(|e| format!("{}\n\nAdd a one text file with \"postgresql://$USER:PWD@localhost/$DB\" to {} ox exclude from testing", e, DB_URL_FILE))?;
        let pool = PgPoolOptions::new()
            .connect(url.as_str()).await
            .map_err(|e| e.to_string())?;

        let r = sqlx::query_as::<_, RpFnLog>(RpFnLog::select(&"".to_string()).as_str())
            .fetch_all(&pool).await.map_err(|e| e.to_string())?;
        assert!(r.len() >= 0);

        let mut v = HashMap::new();
        let id = sqlx::query_scalar::<_, i64>(RpFnLog::insert_v(&"public".to_string()).as_str())
            .bind(0)
            .bind(0)
            .bind(0)
            .bind(Some(sqlx::types::Json(v.clone())))
            .fetch_one(&pool).await.map_err(|e| e.to_string())?;

        v.insert("id".to_string(), id.to_string());
        sqlx::query("update rppd_function_log set trig_value = $1 where id = $2")
            .bind(Some(sqlx::types::Json(v.clone())))
            .bind(id)
            .execute(&pool).await.map_err(|e| e.to_string())?;

        let r = sqlx::query_as::<_, RpFnLog>("select * from rppd_function_log where id = $1")
            .bind(id)
            .fetch_one(&pool).await
            .map_err(|e| e.to_string())?;

        assert_eq!(r.id, id);
        assert!(r.trig_value.is_some());
        assert_eq!(r.trig_value.as_ref().unwrap().len(), v.len());
        println!("{:?}", r);

        let r2 = sqlx::query_as::<_, RpFnLog>(format!("select * from rppd_function_log {} "
                                                      , r.select_sql("where")).as_str()) // instead of .bind()
            .fetch_one(&pool).await
            .map_err(|e| e.to_string())?;

        assert_eq!(r, r2);

        sqlx::query("delete from rppd_function_log where id = $1").bind(id)
            .execute(&pool).await.map_err(|e| e.to_string())?;

        Ok(())
    }
}
