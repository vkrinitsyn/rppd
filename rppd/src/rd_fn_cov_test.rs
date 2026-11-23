//! Coverage of a function's and its log row's plain rules: ids, topics, settings and SQL text.

use super::*;

fn rpfn(id: i32, table: &str, topic: &str) -> RpFn {
    RpFn { id, code: "def f(): pass".into(), checksum: "c".into(), schema_table: table.into(),
        topic: topic.into(), queue: true, cleanup_logs_min: 0, priority: 1,
        fn_logging: false, verbose_debug: false, secret_names: None,
        breaker_fails: 3, breaker_slow_ms: 7, breaker_open_sec: 0, breaker_open_max_sec: 90,
        max_attempts: 0, notify_q: None, dlq: None,
        max_parallel: 4, min_parallel: 0, target_ms: 9, rate_per_sec: 5, burst: 0, max_backlog: 2,
        test_level: None, test_timestamp: None }
}

fn log_with(trig: &[(&str, &str)]) -> RpFnLog {
    let map: HashMap<String, String> = trig.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    RpFnLog { trig_value: Some(sqlx::types::Json(map)), ..RpFnLog::default() }
}

#[test]
fn function_ids_order_by_id_and_compare_with_min_max_clamp() {
    let a = RpFnId { id: 1, ..RpFnId::default() };
    let b = RpFnId { id: 2, ..RpFnId::default() };
    let c = RpFnId { id: 3, ..RpFnId::default() };
    assert!(a < b && b > a && a != b && a == RpFnId { id: 1, queue: false, ..RpFnId::default() });
    assert_eq!(a.partial_cmp(&b), Some(std::cmp::Ordering::Less));
    assert_eq!(c.clone().clamp(a.clone(), b.clone()).id, 2);
    assert_eq!(a.clone().clamp(b.clone(), c.clone()).id, 2);
    assert_eq!(b.clone().clamp(a.clone(), c.clone()).id, 2);
    assert_eq!(RpFnId::fromf(&rpfn(9, "public.t", "")).id, 9);
}

#[test]
fn a_function_gives_its_breaker_and_shaper_settings_with_floors() {
    let f = rpfn(1, "public.t", "");
    let b = f.breaker_cfg();
    assert_eq!((b.fails, b.slow_ms, b.open_sec, b.open_max_sec), (3, 7, 1, 90), "open_sec at least 1");
    let s = f.shaper_cfg();
    assert_eq!((s.max_parallel, s.min_parallel, s.target_ms, s.rate_per_sec, s.burst, s.max_backlog), (4, 1, 9, 5, 1, 2));
    let mut neg = rpfn(2, "public.t", "");
    neg.breaker_fails = -1;
    neg.max_parallel = -3;
    assert_eq!(neg.breaker_cfg().fails, 0);
    assert_eq!(neg.shaper_cfg().max_parallel, 0);
}

#[test]
fn topics_say_what_the_queue_is_named() {
    // no topic: the table is the queue
    let plain = rpfn(1, "public.a", "");
    assert!(!plain.is_dot());
    assert_eq!(plain.to_topic(&RpFnLog::default()), "public.a");
    // a named global topic is itself
    let named = rpfn(2, "public.a", "shared");
    assert_eq!(named.to_topic(&RpFnLog::default()), "shared");
    // a dot topic: per value of a column of the event
    let by = rpfn(3, "public.a", ".id");
    assert!(by.is_dot());
    assert_eq!(by.to_pk_names().into_iter().collect::<Vec<_>>(), vec!["id".to_string()]);
    assert_eq!(by.to_topic(&log_with(&[("id", "5")])), "public.a.id=5");
    assert_eq!(by.to_topic(&log_with(&[("other", "5")])), "public.a.id=NULL");
    assert_eq!(by.to_topic(&RpFnLog::default()), "public.a.id=NULL", "no value at all");
    // two columns, and one that is empty in the name
    let two = rpfn(4, "public.a", ".id.kind.");
    let names = two.to_pk_names();
    assert!(names.contains("id") && names.contains("kind") && names.len() == 2);
    let t = two.to_topic(&log_with(&[("id", "1"), ("kind", "k")]));
    assert!(t.starts_with("public.a.") && t.contains("id=1") && t.contains(".kind=k"), "{}", t);
    // a dot topic's repeat column is the one after the dot
    assert_eq!(by.to_repeat_result().column_name, "id");
    assert!(by.to_repeat_result().pk_value.is_none());
}

#[test]
fn an_error_message_names_the_input_and_the_code_when_asked() {
    let mut f = rpfn(1, "public.a", "shared");
    let log = log_with(&[("id", "7")]);
    let plain = f.err_msg(pyo3::exceptions::PyValueError::new_err("boom"), &log);
    assert!(plain.contains("public.a") && plain.contains("shared"), "{}", plain);
    f.verbose_debug = true;
    let verbose = f.err_msg(pyo3::exceptions::PyValueError::new_err("boom"), &log);
    assert!(verbose.contains("id=7") && verbose.contains("def f"), "{}", verbose);
    let two = log_with(&[("id", "7"), ("kind", "k")]);
    let both = f.err_msg(pyo3::exceptions::PyValueError::new_err("boom"), &two);
    assert!(both.contains("id=7, kind=k") || both.contains("kind=k, id=7"), "{}", both);
    assert!(!f.err_msg(pyo3::exceptions::PyValueError::new_err("boom"), &RpFnLog::default()).is_empty());
    // the code is a C string; one with a nul byte is refused
    assert!(f.code().is_ok());
    f.code = "a\0b".into();
    assert!(f.code().is_err());
}

#[test]
fn a_log_row_knows_its_queue_line_and_event() {
    let f = RpFnId { id: 1, queue: false, priority: 1, save: false, trig_value: false };
    let no_fn = RpFnLog { fn_idp: None, ..RpFnLog::default() };
    assert!(no_fn.is_queue(true) && !no_fn.is_queue(false));
    let row = RpFnLog { fn_idp: Some(f.clone()), ..RpFnLog::default() };
    assert!(!row.is_queue(true), "the function's flag decides");
    let line = row.clone().into_line();
    assert_eq!(line.len(), 1);
    // the event to send to the cluster's master: the table, its key columns, the caller
    let ev = log_with(&[("id", "12"), ("big", "9999999999"), ("text", "x")]).to_event("public.a".into(), 4);
    assert_eq!(ev.table_name, "public.a");
    assert_eq!(ev.pks.len(), 3);
    assert!(ev.pks.iter().any(|p| p.column_name == "big" && p.column_type == 1));
    assert!(ev.pks.iter().any(|p| p.column_name == "text" && p.pk_value.is_none()));
    assert!(ev.id_value);
    assert_eq!(RpFnLog::default().to_event("t".into(), 1).pks.len(), 0);
}

#[test]
fn log_row_sql_is_built_for_each_part() {
    assert!(RpFnLog::select("").contains("from public.rppd_function_log"));
    assert!(RpFnLog::select("s").contains("from s.rppd_function_log"));
    assert!(RpFnLog::insert("s").contains("into s.rppd_function_log (node_id, fn_id, trig_type) values"));
    assert!(RpFnLog::insert_v("s").contains("trig_value"));
    let none = RpFnLog::default();
    assert_eq!(none.select_sql("where"), "");
    let one = log_with(&[("id", "it's")]);
    let sql = one.select_sql(" where ");
    // esca doubles every quote, the one at the start too
    assert!(sql.starts_with(" where ") && sql.contains("id = '''"), "{}", sql);
    let two = log_with(&[("a", "1"), ("b", "2")]);
    let sql2 = two.select_sql(" and ");
    assert!(sql2.contains(" and "), "{}", sql2);
}

#[test]
fn a_select_of_functions_reads_the_optional_columns_where_the_table_has_them() {
    let none = select_fn("s", &HashSet::new());
    assert!(none.contains("from s.rppd_function"));
    assert!(none.contains("null::int as test_level"));
    let all: HashSet<String> = ["test_level", "test_timestamp", "breaker_fails", "max_parallel", "secret_names"]
        .iter().map(|c| c.to_string()).collect();
    let full = select_fn("s", &all);
    assert!(full.contains("breaker_fails") && full.contains("max_parallel"), "{}", full);
}

#[test]
fn the_quote_escape_doubles_each_quote() {
    assert_eq!(esca(&"".to_string()), "");
    assert_eq!(esca(&"'".to_string()), "''");
    assert_eq!(esca(&"x".to_string()), "x");
}

#[test]
fn the_function_test_is_due_once() {
    let mut f = rpfn(1, "public.a", "");
    assert_eq!(test_due(&f), None);
    f.test_level = Some(3);
    assert_eq!(test_due(&f), Some(3));
    f.test_timestamp = Some(Utc::now());
    assert_eq!(test_due(&f), None);
}

#[test]
fn the_log_flags_and_ids_of_a_new_row() {
    let row = RpFnLog::default();
    assert_eq!(row.id, 0);
    assert_eq!(row.attempt, 0);
    assert!(!row.shaped);
    let mut same = row.clone();
    same.output = Some("o".into());
    assert_ne!(row, same);
}
