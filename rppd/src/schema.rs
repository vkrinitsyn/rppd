//! [breaker] rppd's tables brought up to date at startup by SchemaGuard over
//! rppd.yaml (yt-pgxt resilience.md, D4): no hand-written CREATE or ALTER.
//!
//! At startup only an existing install is upgraded: a database with no
//! `rppd_function` is left alone. [rppd-tables] Creating them is ytserv's, through
//! `ensure_schema` (the same pass, without that guard): with `serve_rppd` on, in its
//! catalog and each database of C `rppd_dbs` (yt-pgxt combined-config.md D11). SchemaGuard
//! is append-only - a column this database lacks is added, nothing is dropped - and
//! here it never touches a trigger (those belong to CREATE EXTENSION) and skips,
//! rather than refuses over, a difference it would have to destroy something to
//! converge. A failure is logged, never fatal: whatever columns are missing keep
//! their feature off (the breaker, cadence).

use slog::{debug, info, warn, Logger};
use sqlx::{Pool, Postgres};
use crate::LP;

pub(crate) async fn upgrade(db: &Pool<Postgres>, db_url: &str, schema: &str, log: &Logger) {
    let installed = sqlx::query_scalar::<_, bool>("select exists (select 1 from information_schema.tables \
        where table_schema = $1 and table_name = 'rppd_function')")
        .bind(schema).fetch_one(db).await.unwrap_or(false);
    if !installed {
        debug!(log, "{}schema: no {}.rppd_function - nothing to bring up to date", LP, schema);
        return;
    }
    match migrate(db_url, schema).await {
        Ok(0) => debug!(log, "{}schema: rppd's tables are up to date", LP),
        Ok(n) => info!(log, "{}schema: rppd's tables brought up to date ({} change(s))", LP, n),
        Err(e) => warn!(log, "{}schema: rppd's tables not brought up to date: {} - what their missing \
            columns serve stays off", LP, e),
    }
}

/// [rppd-tables] The SchemaGuard pass over rppd.yaml: what is missing is created - a
/// table, a column - and nothing else changes; never a trigger. The number of changes.
pub(crate) async fn migrate(db_url: &str, schema: &str) -> Result<usize, String> {
    let yaml = match yaml_rust2::YamlLoader::load_from_str(&crate::get_schema_def(schema)) {
        Ok(mut docs) if !docs.is_empty() => docs.remove(0),
        Ok(_) => return Err("rppd.yaml is empty".to_string()),
        Err(e) => return Err(format!("rppd.yaml: {}", e)),
    };
    let opt = schema_guard_tokio::MigrationOptions { exclude_triggers: true, without_failfast: true,
        ..Default::default() };
    schema_guard_tokio::migrate_opt(yaml, db_url, &opt).await
}

#[cfg(test)]
mod tests {
    /// SchemaGuard writes a description into `COMMENT ... IS '<text>'` as it is: one
    /// quote in rppd.yaml's text and the whole upgrade fails and rolls back
    /// (found by topology 92b - no breaker columns, nothing loaded)
    #[test]
    fn rppd_yaml_texts_carry_no_quote() {
        let docs = yaml_rust2::YamlLoader::load_from_str(&crate::get_schema_def("public")).expect("rppd.yaml parses");
        fn walk(y: &yaml_rust2::Yaml, path: &str, bad: &mut Vec<String>) {
            match y {
                yaml_rust2::Yaml::Hash(h) => for (k, v) in h {
                    let k = k.as_str().unwrap_or("?");
                    if k == "description" {
                        if let Some(t) = v.as_str() { if t.contains('\'') { bad.push(format!("{}: {}", path, t)); } }
                    }
                    walk(v, &format!("{}/{}", path, k), bad);
                },
                yaml_rust2::Yaml::Array(a) => for (i, v) in a.iter().enumerate() { walk(v, &format!("{}[{}]", path, i), bad); },
                _ => {}
            }
        }
        let mut bad = vec![];
        walk(&docs[0], "", &mut bad);
        assert!(bad.is_empty(), "a quote in a description breaks the upgrade: {:?}", bad);
    }

    /// SchemaGuard makes a column's index only from `index: true` or a block with a
    /// `name` ("+": idx_<table>_<column>) - one without is no index, and no error.
    /// rppd_config's three unique ones were such blocks: ytserv's roster upsert
    /// (`on conflict (host)`) failed 42P10 on every row, so the roster held only its
    /// seed and no master (found by topology 92_rppd and 92c, 2026-10-17)
    #[test]
    fn every_index_rppd_yaml_declares_is_one_schemaguard_makes() {
        let docs = yaml_rust2::YamlLoader::load_from_str(&crate::get_schema_def("public")).expect("rppd.yaml parses");
        // every table, wherever the file nests it
        fn tables<'a>(y: &'a yaml_rust2::Yaml, out: &mut Vec<&'a yaml_rust2::Yaml>) {
            match y {
                yaml_rust2::Yaml::Hash(h) => {
                    if !y["tableName"].is_badvalue() { out.push(y); }
                    for v in h.values() { tables(v, out); }
                }
                yaml_rust2::Yaml::Array(a) => for v in a { tables(v, out); },
                _ => {}
            }
        }
        let mut all = vec![];
        tables(&docs[0], &mut all);
        assert!(all.len() >= 5, "rppd.yaml's tables: {}", all.len());
        let mut ignored = vec![];
        let mut unique = vec![];
        for t in all {
            let table = t["tableName"].as_str().unwrap_or("?");
            for c in t["columns"].as_vec().map(|v| v.as_slice()).unwrap_or(&[]) {
                let (col, index) = (c["column"]["name"].as_str().unwrap_or("?"), &c["column"]["index"]);
                if index.is_badvalue() || index.is_null() || index.as_bool().is_some() {
                    continue;
                }
                if index["name"].as_str().map_or(true, |n| n.trim().is_empty()) {
                    ignored.push(format!("{}.{}", table, col));
                } else if index["unique"].as_bool() == Some(true) {
                    unique.push(format!("{}.{}", table, col));
                }
            }
        }
        assert!(ignored.is_empty(), "an index block without a name is no index to SchemaGuard: {:?}", ignored);
        for need in ["rppd_config.host", "rppd_config.host_name", "rppd_config.master"] {
            assert!(unique.iter().any(|u| u == need), "{} must be unique (ytserv's roster upsert, one master): {:?}", need, unique);
        }
    }
}
