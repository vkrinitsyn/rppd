/*
RPPD - Rust Python Postgres Discovery

This file is part of RPPD.

RPPD is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version OR the.

RPPD is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with RPPD.  If not, see <http://www.gnu.org/licenses/>.

The portion above shall not be modified.
*/

/*

CREATE table if not exists @extschema@.rppd_function (
    id serial primary key,
    code text not null,
    checksum varchar(64) not null,
    schema_table varchar(64) not null,
    topic varchar(64) not null default '', -- ''
    recovery_logs bool not null default true,
    capture_logs bool not null default false
);

comment on table @extschema@.rppd_function 'Python function code, linked table and topic';

comment on column @extschema@.rppd_function.topic is '"": queue per table; ".{column_name}": queue per value of the column in this table; "{any_name}": global queue';



-- function log
CREATE table if not exists @extschema@.rppd_function_log (
    id bigserial primary key,
    node_id int not null,
    fn_id int not null,
    finished_at timestamp, -- not null default current_timestamp,
    took_sec int not null default 0,
    output_msg text,
    error_msg text
);

comment on table @extschema@.rppd_function_log 'Python function code execution on app nodes';

CREATE TRIGGER @extschema@_rppd_function_log_event AFTER INSERT OR UPDATE OR DELETE ON
    @extschema@.rppd_function_log FOR EACH ROW EXECUTE PROCEDURE @extschema@.rppd_event();

 */

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::mem::ManuallyDrop;
use std::sync::Arc;
use pyo3::prelude::PyModule;
use pyo3::{IntoPy, Py, PyErr, PyObject, PyResult, Python};
use pyo3::types::IntoPyDict;
use sqlx::{Pool, Postgres};
use tokio::sync::{Mutex, RwLock};
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
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
    pub(crate) max_connections: i32,
    pub(crate) recovery_logs: bool,
    pub(crate) capture_logs: bool
}

/// Rust Python Function Log
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct RpFnLog {
    pub(crate) id: i64,
    pub(crate) node_id: i32,
    /// schema.table (topic)
    pub(crate) fn_id: i32,
    pub(crate) took_sec: i32,
    // TODO implement any
    pub(crate) trig_value: Option<i64>,
    /// DbAction
    pub(crate) trig_type: i32,
    pub(crate) finished_at: Option<chrono::DateTime<chrono::Utc>>,
    pub(crate) output_msg: Option<String>,
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

    // pub(crate) ss: Sender<i64>,

    /// todo will dispose when unused
    /// max is max_concurrent
    pub(crate) exec: VecDeque<Mutex<PyContext>>,
}



impl RpFn {
    /// topic starts with dot, means topic value required
    #[inline]
    pub(crate) fn is_dot(&self) -> bool {
        self.topic.len() > 0 && self.topic.as_bytes()[0] == b'.'
    }

    /// trigger re-run event and re-schedule this
    #[inline]
    pub(crate) fn to_repeat_result(&self) -> ScheduleResult {
        ScheduleResult::Repeat(PkColumn {
            column_name: self.topic.clone().split_off(1),
            column_type: 0
        })
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
            let locals = [
                (DB, self.py_db.as_ref(py)),
                (TOPIC, fc.fns.topic.clone().into_py(py).as_ref(py)),
                (TABLE, fc.fns.schema_table.clone().into_py(py).as_ref(py)),
                (PK, x.trig_value.into_py(py).as_ref(py)),
                (TRIG, x.trig_type.into_py(py).as_ref(py)),
            ].into_py_dict(py);
            py.run(fc.fns.code.as_str(), None, Some(locals))
        }).map_err(|e| format!("error on run [{}]: {}", fc.fns.schema_table, e))
    }

}


const SELECT_LOG: &str = "select id, node_id, fn_id, trig_value, trig_type, finished_at, took_sec, output_msg, error_msg from %SCHEMA%.rppd_function_log";

const INSERT_LOG_V: &str = "insert into %SCHEMA%.rppd_function_log (node_id, fn_id, trig_value, trig_type) values ($1, $2, $3, $4) returning id";
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
    async fn test_db() -> Result<(), String> {
        let url = fs::read_to_string(DB_URL_FILE)
            .map_err(|e| format!("{}\n\nAdd a one text file with \"postgresql://$USER:PWD@localhost/$DB\" to {} ox exclude from testing", e, DB_URL_FILE))?;
        let pool = PgPoolOptions::new()
            .connect(url.as_str()).await
            .map_err(|e| e.to_string())?;

        let r = sqlx::query_as::<_, RpFnLog>(RpFnLog::select(&"".to_string()).as_str())
            .fetch_all(&pool).await.map_err(|e| e.to_string())?;
        assert!(r.len() >= 0);

        let id = sqlx::query_scalar::<_, i64>(RpFnLog::insert(&"public".to_string()).as_str())
            .bind(0)
            .bind(0)
            .bind(0)
            .fetch_one(&pool).await.map_err(|e| e.to_string())?;

        let r = sqlx::query_as::<_, RpFnLog>("select * from rppd_function_log where id = $1")
            .bind(id)
            .fetch_one(&pool).await
            .map_err(|e| e.to_string())?;

        assert_eq!(r.id, id);
        println!("{:?}", r);
        sqlx::query("delete from rppd_function_log where id = $1")
            .bind(id)
            .execute(&pool).await.map_err(|e| e.to_string())?;
        Ok(())
    }
}
