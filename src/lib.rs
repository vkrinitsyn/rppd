/*%LPH%*/


use std::str::FromStr;
use std::sync::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use once_cell::sync::Lazy;
use pgrx::*;
use pgrx::prelude::*;
use pgrx::spi::SpiError;
use pgrx::WhoAllocated;
use tokio::runtime::{Builder, Runtime};
use tonic::transport::{Channel, Endpoint};

use crate::gen::rg::{DbAction, EventRequest, EventResponse, PkColumn, PkColumnType, StatusRequest};
use crate::gen::rg::grpc_client::GrpcClient;
use crate::gen::rg::pk_column::PkValue::{BigintValue, IntValue};
use crate::gen::rg::status_request::FnLog;

mod gen;

pgrx::pg_module_magic!();

pub const CONFIG_TABLE: &str = "rppd_config";
pub const TIMEOUT_MS: u64 = 100;


extension_sql_file!("../pg_setup.sql", requires = [rppd_event] );

/// config and conenction
pub(crate) static CONFIG: Lazy<MetaConfig> = Lazy::new(|| {
    let runtime = Builder::new_multi_thread()
        .worker_threads(1)
        .thread_name("rd")
        .thread_stack_size(3 * 1024 * 1024)
        .enable_all()
        .build().map_err(|e| e.to_string()).unwrap();

    MetaConfig {
        server_path: Arc::new(RwLock::new("".into())),
        loaded: Arc::new(AtomicBool::new(false)),
        server: Arc::new(RwLock::new(Err("Error_Not_Yet_Connected".into()))),
        runtime: Arc::new(Mutex::new(runtime)),
    }
});

pub struct MetaConfig {
    /// path to connect
    pub server_path: Arc<RwLock<String>>,
    pub loaded: Arc<AtomicBool>,
    pub server: Arc<RwLock<Result<tokio::sync::Mutex<GrpcClient<Channel>>, String>>>,
    pub runtime: Arc<Mutex<Runtime>>,
}

impl MetaConfig {
    #[inline]
    fn needs_re_connect(&self, host: &str) -> Option<String> {
        match self.server_path.read() {
            Ok(path) => {
                match self.server.read() {
                    Ok(s) => match &*s {
                        Ok(_s) => if host == path.as_str() { None } else {
                            Some(format!("connected to {}, required {}", path, host))
                        },
                        Err(e) => Some(e.clone()),
                    }
                    Err(e) => Some(e.to_string()),
                }
            }
            Err(e) => Some(e.to_string())
        }
    }
}

fn connect(host: &String) -> Result<GrpcClient<Channel>, String> {
    let path = format!("http://{}", host);
    // pgrx::warning!("connecting to: {}", path);
    match CONFIG.runtime.lock() {
        Ok(runtime) => {
            runtime.block_on(async move {
                GrpcClient::connect(Duration::from_millis(TIMEOUT_MS),
                                    Endpoint::from_str(path.as_str())
                                        .map_err(|e| e.to_string())?)
                    .await.map_err(|e| format!("connecting {}", e))
            })
        }
        Err(e) => {
            pgrx::warning!("connecting error. please restart session");
            Err(e.to_string())
        }
    }
}

fn sql(config_schema_table: &String) -> String {
    let st = if config_schema_table.len() == 0 {
        format!("public.{}", CONFIG_TABLE)
    } else if config_schema_table.starts_with(".") {
        format!("public{}", config_schema_table)
    } else if config_schema_table.ends_with(".") {
        format!("{}{}", config_schema_table, CONFIG_TABLE)
    } else {
        config_schema_table.into()
    };
    "SELECT host FROM %ST% where master".replace("%ST%", st.as_str()).to_string()
}

#[pg_trigger]
fn rppd_event<'a>(
    trigger: &'a pgrx::PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgTriggerError> {
    let current =
        if trigger.event().fired_by_delete() {
            trigger.old().ok_or(PgTriggerError::NotTrigger)?//.into_owned()
        } else {
            trigger.new().ok_or(PgTriggerError::NotTrigger)?//.into_owned()
        };

    let mut pks = vec![];
    if let Ok(v) = current.get_by_name::<i32>("id") {
        pks.push(PkColumn { column_name: "id".to_string(), column_type: 0, pk_value: v.map(|v| IntValue(v)) });
    }

    let table_name = trigger.table_name().unwrap_or("".into());
    let not_loaded = !CONFIG.loaded.load(Ordering::Relaxed);
    let cfg_table = table_name.as_str() == CONFIG_TABLE
        && current.get_by_name::<bool>("master")
        .map_err(|_e| PgTriggerError::NullTriggerData)?.unwrap_or(false);
    if not_loaded || cfg_table {
        let host = if cfg_table {
            let host = current.get_by_name::<String>("host")
                .map_err(|_e| PgTriggerError::NullTriggerData)?.unwrap_or("".into());
            if host.contains(":") {
                host
            } else {
                format!("{}:8881", host)
            }
        } else if not_loaded { // do not load from the table event triggered - there will be a deadlock
            match Spi::get_one::<String>("SELECT host FROM rppd_config where master") {
                Ok(path) => path.unwrap_or("".to_string()),
                Err(e) => {
                    pgrx::warning!("error loading host: {}", e);
                    return Err(PgTriggerError::InvalidPgTriggerWhen(1));
                }
            }
        } else {
            CONFIG.server_path.read().unwrap().clone()
        };

        if let Some(e) = CONFIG.needs_re_connect(host.as_str()) {
            if e.len() > 0 {
                pgrx::notice!("Connecting to RPPD server at {}", host);
            } else {
                pgrx::warning!("Re-connecting to RPPD server at {}, previously: {}", host, e);
            }

            let client = connect(&host);

            {
                let mut path = CONFIG.server_path.write().unwrap();
                *path = host.clone();
            }
            let mut server = CONFIG.server.write().unwrap();
            *server = match client {
                Ok(the_client) => Ok(tokio::sync::Mutex::new(the_client)),
                Err(e) => {
                    pgrx::warning!("error on connect to: {} : {}", host, e);
                    Err(format!("server={}, {}", host, e))
                }
            };
        }
    }

    let event_type = if trigger.event().fired_by_update() {
        DbAction::Update as i32
    } else if trigger.event().fired_by_insert() {
        DbAction::Insert as i32
    } else if trigger.event().fired_by_delete() {
        DbAction::Delete as i32
    } else if trigger.event().fired_by_truncate() {
        DbAction::Truncate as i32
    } else {
        DbAction::Dual as i32
    };

    // if event.event_type == DbAction::Insert as i32 && event.table_name.ends_with(CONFIG_TABLE)
    // 	&& CONFIG.server.read().unwrap().is_err() {
    // 	event.
    // }

    let table_name = format!("{}.{}", trigger.table_schema().unwrap_or("public".into()), table_name);
    let event = EventRequest { table_name, event_type, id_value: false, pks: pks.clone(), optional_caller: None };

    match call(event.clone()) {
        Ok(event_response) => {
            for column in &event_response.repeat_with {
                let pk_value = match column.column_type {
                    1 => {
                        assert_eq!((PkColumnType::BigInt as i32), 1);
                        current.get_by_name::<i64>(column.column_name.as_str()).unwrap_or(None)
                            .map(|v| BigintValue(v))
                    }
                    _ => current.get_by_name::<i32>(column.column_name.as_str()).unwrap_or(None)
                        .map(|v| IntValue(v))
                };
                pks.push(PkColumn { pk_value, ..column.clone() });
            }
            if event_response.repeat_with.len() > 0 {
                let _ = call(EventRequest { pks, id_value: true, ..event });
            }

            Ok(Some(current))
        }
        Err(e) => {
            let host = CONFIG.server_path.read().unwrap().clone();
            let mut server = CONFIG.server.write().unwrap();
            *server = Err(e.clone());
            pgrx::warning!("error on event call to host [{}]: {} (will try to connect on next call) ", host, e);
            Err(PgTriggerError::InvalidPgTriggerWhen(2))
        }
    }
}

fn call(event: EventRequest) -> Result<EventResponse, String> {
    let client = CONFIG.server.clone();
    let runtime = CONFIG.runtime.lock().unwrap();
    let client = client.read().unwrap();
    runtime.block_on(async move {
        match &*client {
            Err(e) => Err(e.to_string()),
            Ok(the_client) => {
                let mut client = the_client.lock().await;
                // DO a call:
                client.event(tonic::Request::new(event)).await
                    .map(|response| response.into_inner())
                    .map_err(|e| e.message().to_string())
            }
        }
    })
}


/// output is json
#[pg_extern(name = "rppd_info")]
pub fn rppd_info_master() -> String {
    rppd_info_impl(StatusRequest { config_schema_table: "".to_string(), node_id: -1, fn_log: None })
}
/// output is json
#[pg_extern(name = "rppd_info")]
pub fn rppd_info_master_st(cfg: String) -> String {
    rppd_info_impl(StatusRequest {config_schema_table: cfg, node_id: -1, fn_log: None })
}

/// input is host.id, output is json
#[pg_extern(name = "rppd_info")]
pub fn rppd_info_host(node_id: i32) -> String {
    rppd_info_impl(StatusRequest { config_schema_table: "".to_string(), node_id, fn_log: None })
}


#[pg_extern(name = "rppd_info")]
pub fn rppd_info_host_st(cfg: String, node_id: i32) -> String {
    rppd_info_impl(StatusRequest {config_schema_table: cfg,  node_id, fn_log: None })
}

#[pg_extern(name = "rppd_info")]
pub fn rppd_info_id(node_id: i32, fn_log_id: i64) -> String {
    rppd_info_impl(StatusRequest {
        config_schema_table: "".to_string(), node_id,
        fn_log: Some(
            FnLog::FnLogId(fn_log_id)
        ),
    })
}

#[pg_extern(name = "rppd_info")]
pub fn rppd_info_id_st(cfg: String, node_id: i32, fn_log_id: i64) -> String {
    rppd_info_impl(StatusRequest {
        config_schema_table: cfg, node_id,
        fn_log: Some(
            FnLog::FnLogId(fn_log_id)
        ),
    })
}

#[pg_extern(name = "rppd_info")]
pub fn rppd_info_uuid(node_id: i32, fn_log_uuid: String) -> String {
    rppd_info_impl(StatusRequest {
        config_schema_table: "".to_string(), node_id,
        fn_log: Some(
            FnLog::Uuid(fn_log_uuid)
        ),
    })
}
#[pg_extern(name = "rppd_info")]
pub fn rppd_info_uuid_st(cfg: String, node_id: i32, fn_log_uuid: String) -> String {
    rppd_info_impl(StatusRequest {
        config_schema_table: cfg, node_id,
        fn_log: Some(
            FnLog::Uuid(fn_log_uuid)
        ),
    })
}

/// input is host.id, output is json
pub fn rppd_info_impl(input: StatusRequest) -> String {
    let client = CONFIG.server.clone();
    if client.read().unwrap().is_err() {
        let host = match Spi::get_one::<String>(sql(&input.config_schema_table).as_str()) {
            Ok(path) => path.unwrap_or("".to_string()),
            Err(e) => {
                let msg = match &e {
                    SpiError::InvalidPosition => "\"message\"=\"Check server is running",
                    _ => ""
                };
                return wrap_to_json(format!("{}\", \"sql\"=\"{}\", {}", e, sql(&input.config_schema_table), msg));
            }
        };
        let client = connect(&host);
        let mut server = CONFIG.server.write().unwrap();
        *server = match client {
            Ok(the_client) => Ok(tokio::sync::Mutex::new(the_client)),
            Err(e) => {
                return wrap_to_json(format!("{}\", \"server\"=\"{}", e, host));
            }
        };
    }

    let runtime = CONFIG.runtime.lock().unwrap();
    let client = client.read().unwrap();
    let r = runtime.block_on(async move {
        match &*client {
            Err(e) => Err(e.to_string()),
            Ok(the_client) => {
                let mut client = the_client.lock().await;
                // DO a call:
                client.status(tonic::Request::new(input)).await
                    .map(|response| response.into_inner())
                    .map_err(|e| e.message().to_string())
            }
        }
    });

    match r.map(|r| serde_json::to_string(&r)) {
        Ok(r) => r.unwrap_or_else(|e| wrap_to_json(e.to_string())),
        Err(e) => wrap_to_json(e)
    }
}

fn wrap_to_json(input: String) -> String {
    format!("{{ \"error\":\"{}\" }}", input)
}


#[cfg(any(test, feature = "pg_test"))]
#[pg_trigger]
fn trigger_example<'a>(
    trigger: &'a pgrx::PgTrigger<'a>,
) -> Result<Option<PgHeapTuple<'a, impl WhoAllocated>>, PgTriggerError> {
    let current =
        if trigger.event().fired_by_delete() {
            trigger.old().ok_or(PgTriggerError::NotTrigger)?
        } else {
            trigger.new().ok_or(PgTriggerError::NotTrigger)?
        };

    let id = current.get_by_name::<i64>("id")
        .map_err(|_e| PgTriggerError::NotTrigger)?.unwrap_or(0);
    let title = current.get_by_name::<String>("title")
        .map_err(|_e| PgTriggerError::NotTrigger)?.unwrap_or("".into());
    println!("OUT> {} = {} by {}  {}.{}", id, title, trigger.name().unwrap_or("NA"),
             trigger.table_schema().unwrap_or("".into()), trigger.table_name().unwrap_or("".into())
    );

    Ok(Some(current))
}



#[cfg(any(test, feature = "pg_test"))]

extension_sql!(
    r#"
CREATE TABLE ti (
    id bigserial NOT NULL PRIMARY KEY,
    data text,
    tx bigint
);

CREATE TABLE test (
    id bigserial NOT NULL PRIMARY KEY,
    title varchar(50),
    description text,
    payload jsonb default '{}',
    payload_n jsonb,
    flag bool default true,
    flag_n bool,
    df date default current_date,
    df_n date,
    dtf timestamp default current_timestamp,
    uf uuid default '672124b6-9894-11e5-be38-002d42e813fe',
    uf_n uuid,
    nrf numrange,
    tsrf tsrange
);

CREATE TRIGGER test_trigger AFTER INSERT OR UPDATE OR DELETE ON test FOR EACH ROW EXECUTE PROCEDURE trigger_example();
INSERT INTO test (title, description, payload) VALUES ('Fox', 'a description', '{"key": "value"}');
"#,
    name = "create_trigger",
    requires = [trigger_example]
);


#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use super::*;

    #[pg_test]
    fn test_insert() {
        println!("\nTEST\n");

        let _ = Spi::run(r#"INSERT INTO test (title, description, payload)
        VALUES ('a different title', 'a different description', '{"key": "value"}');
        "#, );

        println!("\n");

        let _ = Spi::run(r#"INSERT INTO test (title) VALUES ('update');"#);

        println!("\nupdating:");
        let _ = Spi::run(r#"update test set description = 'a different description updated', title = 'tbd' where title= 'update';"#);

        println!("\ndeleting:");
        let _ = Spi::run(r#"delete from test where title= 'tbd';"#);

        assert!(true);
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        println!("// perform one-off initialization when the pg_test framework starts");
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        // use in, but no longer available: pgrx::pg_sys::GetConfigOptionByName();
        vec![
            "yt.test = 1111"
        ]
    }

    #[test]
    pub fn test_pg_compile() {
        assert!(true)
    }
}
