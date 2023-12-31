use sqlx::{Pool, Postgres};
use tokio::sync::RwLockWriteGuard;

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

impl RpFnCron {
    /// load all or reload one or remove one schedules
    #[inline]
    pub(crate) async fn load(schema: &String, db: Pool<Postgres>, list: &mut Vec<RpFnCron>) -> Result<(), String> {
        let mut cl = sqlx::query_as::<_, RpFnCron>(SELECT_CRON.replace("%SCHEMA%", schema.as_str()).as_str())
            .fetch_all(&db).await.map_err(|e| e.to_string())?;

        list.clear();
        list.append(&mut cl);
        Ok(())
    }
}