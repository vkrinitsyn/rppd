/*%LPH%*/
#![allow(unused_imports)]

use std::{env, fs};
use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use uuid::Uuid;
use rppd_common::CFG_TABLE;
use crate::fl;

/// DEFAULT_SCHEMA_TABLEe: ytcc::DEFAULT_SCHEMA . ytcc::DEFAULT_TABLE)
// pub const DEFAULT_DB_URL: &str = "postgresql://postgres@postgres?host=/var/run/postgresql";
pub const DEFAULT_DB_URL: &str = "postgresql://$USER@$USER?host=/var/run/postgresql";
pub const DEFAULT_SCHEMA: &str = "rppd.";

pub const CFG_FN_TABLE: &str = "rppd_function";
pub const CFG_FNL_TABLE: &str = "rppd_function_log";
pub const CFG_CRON_TABLE: &str = "rppd_cron";

pub const SCHEMA: &str = "schema";
pub const URL: &str = "url";
pub const BIND: &str = "bind";
pub const LOCALHOST: &str = "localhost";

pub const MAX_QUEUE_SIZE: usize = 1000;
pub const DEFAULT_PORT: u16 = 8881;


///  - a text file in any format OR a dir with a file: rppd.rppd_config names are: 'schema', 'url', 'bind'
///  - three optional args, in any order: config schema, db connection url (starts with 'postgres://', binding IP and port default is 'localhost:8881'
///  - use PGPASSWORD env variable (or from file) to postgres connection
///  - use PGUSER env variable (or read from file) to postgres connection
///  - priority is: env, if no set, than param, than file, than default

pub fn usage() -> String {
    format!("{}\n\n{}\n  {} {}.{}\n{}",
            fl!("rppd-usage"),
            fl!("rppd-default-args"),
            DEFAULT_DB_URL,
            DEFAULT_SCHEMA,
            CFG_TABLE,
            fl!("rppd-more-args"),
    )
}

/// App start args
///  - a text file in any format OR a dir with a file: rppd.rppd_config names are: 'schema', 'url', 'bind'
///  - three optional args, in any order: config schema, db connection url (starts with 'postgres://', binding IP and port default is 'localhost:8881'
///  - use PGPASSWORD env variable (or from file) to postgres connection
///  - use PGUSER env variable (or read from file) to postgres connection
///  - priority is: env, if no set, than param, than file, than default
#[derive(Debug, Clone)]
pub struct RppdConfig {
    /// The node uuid
    pub node: Uuid,
    /// The cluster uuid
    pub cluster: Uuid,
    /// The readable name for a node host instance.
    pub name: String,
    pub schema: String,
    pub table: String,
    pub db_url: String,
    pub user: String,
    pub pwd: String,
    pub file: Option<String>,

    pub bind: String,
    pub port: u16,
    /// MAX_QUEUE_SIZE
    pub max_queue_size: usize,
    pub force_master: bool,
    pub verbose: bool,
}

impl Default for RppdConfig {
    fn default() -> Self {
        let user = match std::env::var_os("PGUSER") {
            Some(a) => a.to_str().unwrap_or("postgres").to_string(),
            _ => match std::env::var_os("USER") {
                Some(a) => a.to_str().unwrap_or("postgres").to_string(),
                _ => "postgres".to_string(),
            }
        };
        let pwd = match std::env::var_os("PGPASSWORD") {
            Some(a) => a.to_str().unwrap_or("").to_string(),
            _ => "".to_string(),
        };
        let this = match std::env::var_os("HOSTNAME") {
            Some(a) => a.to_str().unwrap_or(LOCALHOST).to_string(),
            _ => fs::read_to_string("/etc/hostname").unwrap_or(LOCALHOST.to_string()).trim().to_string(),
        };

        RppdConfig {
            node: Uuid::new_v4(),
            cluster: Uuid::new_v4(),
            name: this,
            db_url: DEFAULT_DB_URL.replace("$USER", user.as_str()),
            user,
            pwd,
            file: None,
            schema: "public".to_string(),
            table: CFG_TABLE.to_string(),
            bind: LOCALHOST.to_string(),
            port: DEFAULT_PORT,
            max_queue_size: RppdConfig::max_queue_size(),
            force_master: false,
            verbose: false,
        }
    }
}

impl Display for RppdConfig {
    #[inline(always)]
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        writeln!(f, "db: {}", self.db_url)?;
        if let Some(fl) = &self.file {
            writeln!(f, "config: {}", fl)?;
        }
        Ok(())
    }
}

impl RppdConfig {
    #[cfg(not(feature = "lib-embedded"))]

    /// first arg is an app itself
    pub fn new(input: Vec<String>) -> Result<Self, String> {
        
        if input.len() > 6 {
            return Err(fl!("err-too-many-args", count = (input.len()  - 1)));
        } else if input.len() == 1 { // default using env
            return Ok(RppdConfig::default());
        }

        let mut cfg = HashMap::new();
        let mut file_idx = 0;
        for i in 1..input.len() {
            match RppdConfig::try_load(&input[i], &mut cfg) {
                Ok(_) => {
                    if cfg.len() > 0 {
                        file_idx = i;
                        break;
                    }
                }
                Err(_e) => {}
            }
        }
        let def = RppdConfig::default();
        let mut user = def.user.clone();
        let mut db_url = def.db_url.clone();
        let mut pwd = def.pwd.clone();
        let file = if file_idx > 0 { Some((&input[file_idx]).to_string()) } else { None };
        let mut schema = def.schema.clone();
        let mut name = def.name.clone();
        let mut node = def.node.clone();
        let mut cluster = def.cluster.clone();
        let mut bind = def.bind.clone();
        let mut port = DEFAULT_PORT;
        let mut force_master = false;
        let mut verbose = false;

        for i in 1..input.len() {
            if i != file_idx {
                if input[i].as_str() == "--verbose" {
                    verbose = true;
                } else if let Some(v) = RppdConfig::try_parse_env(&cfg, "PGUSER") {
                    user = v;
                } else if let Some(v) = RppdConfig::try_parse_env(&cfg, "PGPASSWORD") {
                    pwd = v;
                } else if let Some(v) = RppdConfig::try_parse_cfg(&input[i], &cfg, "postgresql://", URL) {
                    db_url = v;
                } else if input[i].starts_with("--force_master=") {
                    force_master = input[i].ends_with("=true") || input[i].ends_with("=yes");
                } else if input[i].starts_with("--name=") {
                    let v: Vec<&str> = input[i].split("=").collect();
                    if v.len() > 1 { name = v[1].to_string(); }
                } else if let Some(v) = RppdConfig::try_parse_cfg(&input[i], &cfg, ":", BIND) {
                    let b: Vec<&str> = v.split(":").collect();
                    bind = b[0].to_string();
                    if b.len() > 1 {
                        match b[1].parse::<u16>() {
                            Ok(v) => { port = v; }
                            Err(e) => {
                                return Err(fl!("err-wrong-port-format", string = e.to_string(), value = v));
                            }
                        }
                    }
                } else if let Some(v) = RppdConfig::try_parse_cfg(&input[i], &cfg, "", SCHEMA) {
                    let s: Vec<&str> = v.split(".").collect();
                    if s.len() > 2 {
                        return Err(fl!("err-wrong-schema-format", string = v));
                    }
                    schema = s[0].to_string();
                } else if input[i].starts_with("--node=") {
                    let v: Vec<&str> = input[i].split("=").collect();
                    if v.len() > 1 { node = Uuid::from_str(v[1]).unwrap_or(node); }
                } else if input[i].starts_with("--cluster=") {
                    let v: Vec<&str> = input[i].split("=").collect();
                    if v.len() > 1 { cluster = Uuid::from_str(v[1]).unwrap_or(cluster); }
                }
            }
        }

        Ok(RppdConfig {
            node, cluster, name, db_url, user, pwd, file,
            schema, table: CFG_TABLE.to_string(),
            bind, port,
            max_queue_size: RppdConfig::max_queue_size(),
            force_master,
            verbose,
        })
    }

    #[inline]
    fn max_queue_size() -> usize {
        match env::var_os("MAX_QUEUE_SIZE") {
            Some(a) => a.to_str().unwrap_or("").to_string(),
            _ => "".to_string(),
        }.parse::<usize>().unwrap_or(MAX_QUEUE_SIZE)
    }

    /// taking values from env, than file
    #[inline]
    #[cfg(not(feature = "lib-embedded"))]
    fn try_parse_cfg(input: &String, cfg: &HashMap<String, String>, value: &str, name: &str) -> Option<String> {
        if value.len() == 0 || input.contains(value) {
            Some(input.to_string())
        } else {
            cfg.get(name).map(|v| v.to_string())
        }
    }

    /// taking values from env, than file
    #[inline]
    fn _try_parse_uu(input: &String, cfg: &HashMap<String, String>, value: &str, name: &str) -> Option<String> {
        if value.len() == 0 || input.contains(value) {
            Some(input.to_string())
        } else {
            cfg.get(name).map(|v| v.to_string())
        }
    }

    /// taking values from args, than file
    #[inline]
    #[cfg(not(feature = "lib-embedded"))]
    fn try_parse_env(cfg: &HashMap<String, String>, name: &str) -> Option<String> {
        let res = env::var_os(name).map(|v| v.to_str().unwrap_or("").to_string());

        if res.is_none() {
            cfg.get(name).map(|v| v.to_string())
        } else {
            res
        }
    }

    ///  - priority is: env, if no set, than param, than file, than default
    #[inline]
    #[cfg(not(feature = "lib-embedded"))]
    fn try_load(file: &String, cfg: &mut HashMap<String, String>) -> Result<(), String> {
        let p = Path::new(file);
        if p.is_dir() {
            RppdConfig::try_load(&format!("{}/{}{}", file, DEFAULT_SCHEMA, CFG_TABLE), cfg)
        } else if p.is_file() {
            let f = File::open(file).map_err(|e| e.to_string())?;
            RppdConfig::load(f, cfg);
            Ok(())
        } else {
            Err("wrong format".into())
        }
    }

    #[inline]
    #[cfg(not(feature = "lib-embedded"))]
    fn load(f: File, cfg: &mut HashMap<String, String>) {
        let reader = BufReader::new(f);
        for line in reader.lines() {
            if let Ok(l) = line {
                if let Some(i) = l.chars().position(|c| c == '=' || c == ':' || c == '#' || c == ';' || c == '/' || c == '[') {
                    if l.as_bytes()[i] != b'#' && l.as_bytes()[i] != b';' && l.as_bytes()[i] != b'/' && l.as_bytes()[i] != b'[' {
                        let key = l[..i].trim().to_lowercase();
                        let value = l[i + 1..].trim().to_string();
                        cfg.insert(key, value);
                    }
                }
            }
        }
    }


    #[allow(dead_code)]
    fn parse_table_name(src: &String, t: &String) -> String {
        if t.len() >= 1 {
            if t.ends_with(".") {
                format!("{}{}", t, CFG_TABLE)
            } else if t.starts_with(".") {
                format!("{}{}", DEFAULT_SCHEMA, t)
            } else if t.find(".").is_none() {
                format!("{}.{}", DEFAULT_SCHEMA, t)
            } else {
                src.to_string()
            }
        } else {
            src.to_string()
        }
    }

    pub(crate) fn db_url(&self) -> String {
        let urls: Vec<&str> = self.db_url.split("@").collect();
        format!("postgres://{}:{}@{}", self.user, self.pwd, urls[urls.len() - 1])
    }
}

#[allow(warnings)]
#[cfg(test)]
#[cfg(not(feature = "lib-embedded"))]
mod tests {
    use std::fs;

    use uuid::Uuid;

    use super::*;

    #[inline]
    fn uuid() -> String {
        let x = Uuid::new_v4();
        x.hyphenated().to_string()[..8].into()
    }

    // #[test]
    fn config_args_file1_test() {
        let f = format!("/tmp/yt-{}.cfg", uuid());
        fs::write(&f, "db=p\nconfig=c").ok();
        let cfg = ["test".to_string(), f.clone()].to_vec();
        let cfg = RppdConfig::new(cfg).unwrap();
        assert_eq!(cfg.db_url.as_str(), "p");
        assert_eq!(cfg.schema.as_str(), "public");
        fs::remove_file(&f).ok();
    }

    // #[test]
    fn config_args_file2_test() {
        let f = format!("/tmp/yt-{}.cfg", uuid());
        fs::write(&f, "db = p\nconfig : c").ok();
        let cfg = ["test".to_string(), f.clone()].to_vec();
        let cfg = RppdConfig::new(cfg).unwrap();
        assert_eq!(cfg.db_url.as_str(), "p");
        assert_eq!(cfg.schema.as_str(), "public.c");
        fs::remove_file(&f).ok();
    }

    #[test]
    fn config_args_1_test() {
        let cfg = ["test".to_string(), "postgresql://db".to_string()].to_vec();
        let cfg = RppdConfig::new(cfg).unwrap();
        assert_eq!(cfg.db_url.as_str(), "postgresql://db");
    }

    #[test]
    fn config_args_2_test() {
        let cfg = ["test".to_string(), "postgresql://db".to_string(), "c".to_string()].to_vec();
        let cfg = RppdConfig::new(cfg).unwrap();
        assert_eq!(cfg.db_url.as_str(), "postgresql://db");
        assert_eq!(cfg.schema.as_str(), "c");
    }

    #[test]
    fn config_args_4_test() {
        assert!(RppdConfig::new(vec![]).is_ok())
    }


    #[test]
    fn config_args_5_test() {
        let cfg = ["test".to_string(), "--force_master=true".to_string(), "postgresql://db".to_string(), "c".to_string()].to_vec();
        let cfg = RppdConfig::new(cfg).unwrap();
        assert_eq!(cfg.db_url.as_str(), "postgresql://db");
        assert_eq!(cfg.schema.as_str(), "c");
        assert!(cfg.force_master);
    }

    #[test]
    fn config_args_parsing_test() {
        assert_eq!("".to_string(), RppdConfig::parse_table_name(&"".to_string(), &"".to_string()));
        assert_eq!(format!("s.{}", CFG_TABLE),
                   RppdConfig::parse_table_name(&"".to_string(), &"s.".to_string()));
        assert_eq!(format!("{}.t", DEFAULT_SCHEMA),
                   RppdConfig::parse_table_name(&"".to_string(), &".t".to_string()));
        assert_eq!(format!("{}.t", DEFAULT_SCHEMA),
                   RppdConfig::parse_table_name(&"".to_string(), &"t".to_string()));
    }

    #[test]
    fn dns_test() {
        let ips: Vec<std::net::IpAddr> = dns_lookup::lookup_host("localhost").unwrap();
        assert!(ips.len() > 0);
        let ipsd: Vec<std::net::IpAddr> = dns_lookup::lookup_host("127.0.0.1").unwrap();
        assert_eq!(ipsd.len(), 1);
    }
}
