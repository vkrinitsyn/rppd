/*%LPH%*/

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::mem::ManuallyDrop;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use pyo3::prelude::PyModule;
use pyo3::{IntoPy, Py, PyAny, PyErr, PyObject, PyResult, Python};
use pyo3::types::{IntoPyDict, PyDict};
use sqlx::{Pool, Postgres};
use tokio::sync::RwLock;
use tokio::sync::mpsc::Sender;
use tonic::Status;
use crate::gen::rg::{event_request, EventRequest, PkColumn};
use crate::rd_config::{Cluster, ScheduleResult};

/// ManuallyDrop for PyModule and db: PyObject
pub struct PyContext {
    py_rt: ManuallyDrop<Py<PyModule>>,
    py_db: ManuallyDrop<PyObject>,
    created: Instant,
}


impl Drop for PyContext {
    fn drop(&mut self) {
        Python::with_gil(|_py| {
            unsafe {
                ManuallyDrop::drop(&mut self.py_rt);
                ManuallyDrop::drop(&mut self.py_db);
            }
        });
    }
}


pub enum PyCall {
    /// local call
    Local(PyContext),
    /// host id
    Remote(i32)
}

pub(crate) const SELECT_FN: &str = "select id, code, checksum, schema_table, topic, queue, cleanup_logs_min from %SCHEMA%.rppd_function ";

/// Rust Python Function
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct RpFn {
    pub(crate) id: i32,
    pub(crate) code: String,
    pub(crate) checksum: String,
    pub(crate) schema_table: String,
    /// use max_concur = 0 for queue
    pub(crate) topic: String,
    /// max concurrent event execution on the topic to make queue
    pub(crate) queue: bool,
    pub(crate) cleanup_logs_min: i32,
}

/// Rust Python Function Log
#[derive(sqlx::FromRow, PartialEq, Debug, Clone)]
pub struct RpFnLog {
    pub(crate) id: i64,
    pub(crate) node_id: i32,
    /// schema.table (topic)
    pub(crate) fn_id: i32,
    pub(crate) took_sec: i32,

    // TODO implement any
    pub(crate) trig_value: Option<sqlx::types::Json<HashMap<String, String>>>,

    /// DbAction
    pub(crate) trig_type: i32,
    pub(crate) finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) error_msg: Option<String>,
}

pub(crate) const SELECT_CRON: &str = "select id, fn_id, cron, timeout_sec, started_at, finished_at, error_msg from %SCHEMA%.rppd_cron";

/// Rust Python Function Cron
#[derive(sqlx::FromRow, PartialEq, Debug, Clone)]
pub struct RpFnCron {
    pub(crate) id: i64,
    pub(crate) fn_id: i32,
    pub(crate) cron: String, // cron-parser
    pub(crate) timeout_sec: Option<i32>,
    pub(crate) started_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) error_msg: Option<String>,
}


/// few links to collections from the [Cluster] to run load and operate on background
#[derive(Clone)]
pub(crate) struct RpFnBgCtx {
    pub(crate) fns: Arc<RwLock<BTreeMap<i32, RpFn>>>,
    pub(crate) pys: Arc<RwLock<BTreeMap<i32, Mutex<PyCall>>>>,
    pub(crate) db: Pool<Postgres>,

}

/// topicID(or0), subtopicID
#[derive(Clone)]
pub(crate) struct RpFnTpId {
    pub(crate) topic_id: i32,
    pub(crate) subtopic_id: i32,
}

pub(crate) struct RpFnCtx {
    pub(crate) fns: RpFn,

    /// comment on column @extschema@.rppd_function.topic see fns.topic
    /// is '"": queue per table; => "schema.table"
    /// ".{column_name}": queue per value of the column in this table; => "schema.table.column.value"
    /// "{any_name}": global queue'; => "...name"

    pub(crate) topics: HashMap<RpFnTpId, VecDeque<RpFnLog>>,

}



impl RpFn {
    /// topic starts with dot, means topic value required
    #[inline]
    pub(crate) fn is_dot(&self) -> bool {
        self.topic.len() > 0 && self.topic.as_bytes()[0] == b'.'
    }

    /// trigger re-run event and re-schedule this
    #[inline]
    pub(crate) fn to_repeat_result(&self) -> PkColumn {
        PkColumn {
            column_name: self.topic.clone().split_off(1),
            column_type: 0,
            pk_value: None,
        }
    }

    #[inline]
    pub(crate) async fn merge(&mut self, f: RpFn, cluster: &Cluster) {
        if f.topic != self.topic {
            self.topic = f.topic;
            let mut map = cluster.fn_id.write().await;
            match map.get_mut(&self.topic) {
                None => {
                    let mut set = BTreeSet::new();
                    set.insert(self.id.clone());
                    map.insert(self.topic.clone(), set);
                }
                Some(set) => {
                    set.insert(self.id.clone());
                }
            }
        }

        if f.schema_table != self.schema_table {
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

}

/// python module to import
pub const POSTGRES_PY: &str = "psycopg2";

pub const DB: &str = "DB";
pub const TOPIC: &str = "TOPIC";
pub const TABLE: &str = "TABLE";
pub const PK: &str = "PK";
/// environment level: local, dev, test, prod
pub const TRIG: &str = "TRIG";

impl PyContext {
    #[inline]
    pub(crate) fn new(f: &RpFn, db_url: &String) -> Result<PyContext, PyErr> {

        let module: Py<PyModule> = Python::with_gil(|py| -> PyResult<_> {
            Ok(PyModule::import(py, POSTGRES_PY)?.into())
        })?;

        // let libpq_kv = format!("host=localhost sslmode=disable user={} password={} dbname={}", username, password, db);
        let client: PyObject = Python::with_gil(|py| -> PyResult<_> {
            Ok(module.as_ref(py).getattr("connect")?.call1((db_url,))?.into())
        })?;

        Ok(PyContext {
            py_rt: ManuallyDrop::new(module),
            py_db: ManuallyDrop::new(client),
            created: Instant::now(),
        })
    }

    pub fn invoke(&self, x: &RpFnLog, fc: &RpFnCtx) -> Result<(), String> {

    // pub fn invoke(&self, script: String,  fn_name: String,  table: String, env: HashMap<String, String>, pks: Vec<u64>) -> Result<(), String> {
        Python::with_gil(|py| {
            let mut locals = Vec::new();
            if let Some(pks) = &x.trig_value {
                for (pk, pk_val) in pks.iter() {
                    locals.push((pk.to_ascii_uppercase(), pk_val.into_py(py)));
                }
            }
            for i in 0..locals.len() { locals.push((format!("_nil{}", i), "".into_py(py))); } // to avoid out of index
            let locals = [
                (DB, self.py_db.as_ref(py)),
                (TOPIC, fc.fns.topic.clone().into_py(py).as_ref(py)),
                (TABLE, fc.fns.schema_table.clone().into_py(py).as_ref(py)),
                (TRIG, x.trig_type.clone().into_py(py).as_ref(py)),
                (locals[0].0.as_str(), locals[0].1.as_ref(py)), // TODO fix reference problem
                (locals[1].0.as_str(), locals[1].1.as_ref(py)),
                (locals[2].0.as_str(), locals[2].1.as_ref(py)),
            ].into_py_dict(py);

            py.run(fc.fns.code.as_str(), None, Some(locals))
        }).map_err(|e| format!("error on run [{}]: {}", fc.fns.schema_table, e))

    }

}


const SELECT_LOG: &str = "select id, node_id, fn_id, trig_value, trig_type, finished_at, took_sec, error_msg from %SCHEMA%.rppd_function_log";

const INSERT_LOG_V: &str = "insert into %SCHEMA%.rppd_function_log (node_id, fn_id, trig_type, trig_value) values ($1, $2, $3, $4) returning id";
const INSERT_LOG: &str = "insert into %SCHEMA%.rppd_function_log (node_id, fn_id, trig_type) values ($1, $2, $3) returning id";

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
    pub(crate) async fn update(&self, took: u64, db: Pool<Postgres>, schema: &String)  {
        if self.id == 0 { return; }
        let sql = "update %SCHEMA%.rppd_function_log set finished_at = current_timestamp, took_sec = $1 where id = 2";
        let sql = sql.replace("%SCHEMA%", schema.as_str());
        if let Err(e) = sqlx::query(sql.as_str())
            .bind(took as i32)
            .bind(self.id)
            .execute(&db).await {
            eprintln!("{}", e);
        }
    }


}

#[inline]
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
    use crate::rd_config::RpHost;
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
        v.insert("id".to_string(), "0".to_string());
        // v.insert("x".to_string(), "'0'".to_string());
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
