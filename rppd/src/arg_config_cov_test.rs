#![cfg(not(feature = "lib-embedded"))]
//! [coverage] the argument, file and environment parsing of RppdConfig

use std::fs;
use std::sync::atomic::{AtomicU32, Ordering};

use super::*;

static FILES: AtomicU32 = AtomicU32::new(0);

/// A unique path under the temp dir, removed by the caller
fn scratch(what: &str) -> String {
    let n = FILES.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("rppd-args-{}-{}-{}", std::process::id(), n, what)).display().to_string()
}

/// Sets the variables for the closure, then puts each back as it was
fn with_env<T>(vars: &[(&str, Option<&str>)], f: impl FnOnce() -> T) -> T {
    // the binary's one lock of the environment (cov_lock.rs): a test of any module that builds a
    // configuration from it waits, and so does another test that sets a variable
    let _g = crate::cov_lock::env_write();
    let before: Vec<(String, Option<std::ffi::OsString>)> = vars.iter()
        .map(|(k, _)| (k.to_string(), env::var_os(k))).collect();
    for (k, v) in vars {
        // SAFETY: every test that changes the environment holds ENV_LOCK to write, and the tests that
        // read it through RppdConfig hold it to read
        unsafe {
            match v {
                Some(v) => env::set_var(k, v),
                None => env::remove_var(k),
            }
        }
    }
    let out = f();
    for (k, v) in before {
        // SAFETY: as above
        unsafe {
            match v {
                Some(v) => env::set_var(&k, v),
                None => env::remove_var(&k),
            }
        }
    }
    out
}

fn args(v: &[&str]) -> Vec<String> {
    v.iter().map(|s| s.to_string()).collect()
}

/// The arguments parsed with no PGUSER and no PGPASSWORD in the environment - with one set the
/// parser takes an argument for it - whatever the machine's own environment says, and never
/// while another test has set one
fn parsed(v: &[&str]) -> Result<RppdConfig, String> {
    with_env(&[("PGUSER", None), ("PGPASSWORD", None)], || RppdConfig::new(args(v)))
}

#[test]
fn the_display_lists_only_the_urls_that_are_set() {
    let _env = crate::cov_lock::env_read();
    let mut c = RppdConfig { db_url: "postgresql://d".into(), ..Default::default() };
    let plain = format!("{}", c);
    assert_eq!(plain, "db: postgresql://d\n");
    c.df_url = Some("df://s:1".into());
    c.ch_url = Some("ch:8123".into());
    c.file = Some("/etc/rppd".into());
    let full = format!("{}", c);
    assert_eq!(full, "db: postgresql://d\ndf: df://s:1\nch: ch:8123\nconfig: /etc/rppd\n");
}

#[test]
fn the_usage_names_the_default_database() {
    let u = usage();
    assert!(u.contains(DEFAULT_DB_URL), "{}", u);
}

#[test]
fn too_many_arguments_are_refused_and_none_is_the_default() {
    let err = parsed(&["test", "a", "b", "c", "d", "e", "f"]).unwrap_err();
    assert!(err.starts_with("RPPD-E001"), "{}", err);
    let c = with_env(&[("MAX_QUEUE_SIZE", Some("42"))], || RppdConfig::new(args(&["test"])).unwrap());
    assert_eq!(c.schema, "public");
    assert_eq!(c.max_queue_size, 42, "MAX_QUEUE_SIZE is read");
    let bad = with_env(&[("MAX_QUEUE_SIZE", Some("many"))], RppdConfig::default);
    assert_eq!(bad.max_queue_size, MAX_QUEUE_SIZE, "a value that is no number is the default");
}

#[test]
fn name_verbose_bind_port_and_ids_are_read_from_the_arguments() {
    let c = parsed(&["test", "--name=n1", "--verbose", "127.0.0.1:9100"]).unwrap();
    assert_eq!(c.name, "n1");
    assert!(c.verbose);
    assert_eq!(c.bind, "127.0.0.1");
    assert_eq!(c.port, 9100);
    assert!(c.consume && !c.force_master);
    let d = parsed(&["test", "--force_master=true"]).unwrap();
    assert!(d.force_master);
}

#[test]
fn a_port_that_is_no_number_is_refused() {
    let err = parsed(&["test", "127.0.0.1:x"]).unwrap_err();
    assert!(err.starts_with("RPPD-E002"), "{}", err);
}

#[test]
fn a_schema_with_two_dots_is_refused_and_one_takes_its_first_part() {
    let err = parsed(&["test", "a.b.c"]).unwrap_err();
    assert!(err.starts_with("RPPD-E003"), "{}", err);
    let c = parsed(&["test", "a.b"]).unwrap();
    assert_eq!(c.schema, "a");
}

#[test]
fn urls_of_the_three_kinds_are_taken_by_their_prefix() {
    let c = parsed(&["test", "postgresql://db", "df://scheduler:50050", "http://ch.lan:8123"]).unwrap();
    assert_eq!(c.db_url, "postgresql://db");
    assert_eq!(c.df_url.as_deref(), Some("df://scheduler:50050"));
    assert_eq!(c.ch_url.as_deref(), Some("http://ch.lan:8123"));
}

#[test]
fn a_file_argument_supplies_the_values_the_others_leave_out() {
    let f = scratch("db.cfg");
    fs::write(&f, "# a comment\n; another\n/ slashes\n[section]\n=empty key\nURL=postgresql://fromfile\n").unwrap();
    let c = parsed(&["test", &f, "x"]).unwrap();
    fs::remove_file(&f).ok();
    assert_eq!(c.db_url, "postgresql://fromfile");
    assert_eq!(c.file.as_deref(), Some(f.as_str()));
    assert_eq!(c.schema, "public");
}

#[test]
fn a_file_with_a_bind_supplies_it_to_a_bare_argument() {
    let f = scratch("bind.cfg");
    fs::write(&f, "bind : 127.0.0.1:9200\n").unwrap();
    let c = parsed(&["test", &f, "q"]).unwrap();
    fs::remove_file(&f).ok();
    assert_eq!((c.bind.as_str(), c.port), ("127.0.0.1", 9200));
}

#[test]
fn a_directory_argument_reads_the_config_file_in_it() {
    let d = scratch("dir");
    fs::create_dir_all(&d).unwrap();
    let file = format!("{}/{}{}", d, DEFAULT_SCHEMA, CFG_TABLE);
    fs::write(&file, "bind=127.0.0.1:9300\n").unwrap();
    let c = parsed(&["test", &d, "q"]).unwrap();
    fs::remove_dir_all(&d).ok();
    assert_eq!(c.port, 9300);
    assert_eq!(c.file.as_deref(), Some(d.as_str()));
}

#[test]
fn an_argument_that_is_no_file_is_taken_as_a_schema() {
    let missing = scratch("missing.cfg");
    assert!(RppdConfig::try_load(&missing, &mut HashMap::new()).unwrap_err().starts_with("RPPD-E004"));
    let c = parsed(&["test", &missing]).unwrap();
    assert_eq!(c.file, None);
}

#[test]
fn the_environment_wins_for_the_user_password_and_host() {
    let c = with_env(&[("PGUSER", Some("envuser")), ("PGPASSWORD", Some("envpw")), ("HOSTNAME", Some("envhost"))],
        RppdConfig::default);
    assert_eq!(c.user, "envuser");
    assert_eq!(c.pwd, "envpw");
    assert_eq!(c.name, "envhost");
    assert_eq!(c.db_url, DEFAULT_DB_URL.replace("$USER", "envuser"));
    // the arguments read the same variables: with PGUSER set, the next argument is the user
    let a = with_env(&[("PGUSER", Some("envuser")), ("PGPASSWORD", Some("envpw"))],
        || RppdConfig::new(args(&["test", "--verbose", "postgresql://db"])).unwrap());
    assert_eq!((a.user.as_str(), a.pwd.as_str()), ("envuser", "envpw"));
    assert!(a.verbose);
}

#[test]
fn without_a_user_variable_the_default_is_postgres() {
    let c = with_env(&[("PGUSER", None), ("USER", None), ("PGPASSWORD", None), ("HOSTNAME", None)], RppdConfig::default);
    assert_eq!(c.user, "postgres");
    assert_eq!(c.pwd, "");
    assert!(!c.name.is_empty());
}

#[test]
fn a_user_variable_without_pguser_is_the_user() {
    let c = with_env(&[("PGUSER", None), ("USER", Some("osuser"))], RppdConfig::default);
    assert_eq!(c.user, "osuser");
}

#[test]
fn the_parsing_helpers_take_the_argument_or_the_file_value() {
    let mut cfg = HashMap::new();
    cfg.insert("url".to_string(), "fromfile".to_string());
    // an empty marker always takes the argument
    assert_eq!(RppdConfig::_try_parse_uu(&"x".to_string(), &cfg, "", "url").as_deref(), Some("x"));
    // the marker is in the argument
    assert_eq!(RppdConfig::_try_parse_uu(&"postgresql://a".to_string(), &cfg, "postgresql://", "url").as_deref(), Some("postgresql://a"));
    // otherwise the file's value, or none
    assert_eq!(RppdConfig::_try_parse_uu(&"x".to_string(), &cfg, "postgresql://", "url").as_deref(), Some("fromfile"));
    assert_eq!(RppdConfig::_try_parse_uu(&"x".to_string(), &cfg, "postgresql://", "missing"), None);
    assert_eq!(RppdConfig::try_parse_cfg(&"x".to_string(), &cfg, "postgresql://", "url").as_deref(), Some("fromfile"));
    assert_eq!(RppdConfig::try_parse_env(&cfg, "RPPD_SURELY_UNSET_VARIABLE").as_deref(), None);
    let mut with = cfg.clone();
    with.insert("RPPD_TEST_FILE_ONLY".to_string(), "v".to_string());
    assert_eq!(RppdConfig::try_parse_env(&with, "RPPD_TEST_FILE_ONLY").as_deref(), Some("v"));
}

#[test]
fn a_table_name_is_put_in_its_schema() {
    let src = "given".to_string();
    assert_eq!(RppdConfig::parse_table_name(&src, &"a.b".to_string()), "given", "a schema and a table: as given");
    assert_eq!(RppdConfig::parse_table_name(&src, &"s.".to_string()), format!("s.{}", CFG_TABLE));
}

#[test]
fn the_database_url_carries_the_user_and_password() {
    let _env = crate::cov_lock::env_read();
    let c = RppdConfig { db_url: "postgresql://old@host:5432/db".into(), user: "u".into(), pwd: "p".into(),
        ..Default::default() };
    assert_eq!(c.db_url(), "postgres://u:p@host:5432/db");
    assert_eq!(c.connection_url(), c.db_url());
}

#[test]
fn the_password_variable_is_read_for_an_argument() {
    let c = with_env(&[("PGUSER", None), ("PGPASSWORD", Some("pw"))], || RppdConfig::new(args(&["test", "q"])).unwrap());
    assert_eq!(c.pwd, "pw");
}

#[test]
fn a_loaded_file_keeps_its_last_value_for_a_key() {
    let f = scratch("dup.cfg");
    fs::write(&f, "url=first\nurl=second\n").unwrap();
    let mut cfg = HashMap::new();
    RppdConfig::try_load(&f, &mut cfg).unwrap();
    fs::remove_file(&f).ok();
    assert_eq!(cfg.get("url").map(String::as_str), Some("second"));
}
