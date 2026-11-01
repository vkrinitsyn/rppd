//! [i18n] The language of rppd's messages - rppd's own (rppde::i18n, its tr!) and the ones
//! made here, in the generated gRPC code (build.rs) - as etcds's and sge's are.
//!
//! The first of: [set_lang] - what an embedder sets, as ytserv does with its own language;
//! the environment's LC_ALL, LC_MESSAGES or LANG, the first set; English. A locale such as
//! `ru_RU.UTF-8` is taken by its language, a language without its messages is English, and
//! an empty one, `C` or `POSIX` is none.
//!
//! The messages made here are in locales/<language>.yml, one file per language, each with
//! every key on the same line: en, ru, and - not reviewed - fr, de, zh (Simplified
//! Chinese), ja. An error's starts with its code, `RPPD-Ennn`, the same in every language.
//!
//! The language is the process's, not a call's: one rppd, one language.

use std::sync::{OnceLock, RwLock};

/// set_lang's
static LANG: RwLock<Option<String>> = RwLock::new(None);

/// the environment's, read once: not on every message, and not while embedded Python may
/// be writing os.environ
static ENV: OnceLock<Option<String>> = OnceLock::new();

/// The language of the messages from here on: `en`, `ru`, `fr`, `de`, `zh`, `ja`, or a
/// locale of one (`ru_RU.UTF-8`). Wins over the environment's; an empty one is none, and
/// leaves it to the environment.
pub fn set_lang(lang: &str) {
    if let Ok(mut l) = LANG.write() {
        *l = language(lang);
    }
}

/// The language the messages are in now
pub fn lang() -> String {
    choose(LANG.read().ok().and_then(|l| l.clone()), || ENV.get_or_init(environment).clone())
}

/// the first language given; the environment's asked for only when none is set
fn choose(set: Option<String>, env: impl FnOnce() -> Option<String>) -> String {
    set.or_else(env).unwrap_or_else(|| "en".to_string())
}

/// LC_ALL, LC_MESSAGES or LANG, the first set, as a language
fn environment() -> Option<String> {
    ["LC_ALL", "LC_MESSAGES", "LANG"].iter()
        .filter_map(|v| std::env::var(v).ok())
        .find(|v| !v.trim().is_empty())
        .and_then(|v| language(&v))
}

/// a locale's language: its letters before `_`, `.`, `-` or `@`, lower case; none for an
/// empty one, `C` or `POSIX`
fn language(locale: &str) -> Option<String> {
    let l = locale.trim().split(['_', '.', '-', '@']).next().unwrap_or("").to_lowercase();
    (l.len() >= 2 && l.chars().all(|c| c.is_ascii_alphabetic()) && l != "posix").then_some(l)
}

/// [i18n] A gRPC client's call refused before it was sent: the connection is not ready.
/// What the generated clients say in tonic's "Service was not ready" place (build.rs)
pub fn not_ready(err: tonic::codegen::StdError) -> String {
    rust_i18n::t!("grpc.not_ready", locale = &lang(), err = err).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// [i18n] set_lang over the environment, the environment over English
    #[test]
    fn the_language_is_the_first_one_given() {
        let s = |v: &str| Some(v.to_string());
        assert_eq!(choose(s("de"), || s("ru")), "de");
        assert_eq!(choose(None, || s("ru")), "ru");
        assert_eq!(choose(None, || None), "en");
        assert_eq!(choose(s("de"), || panic!("the environment asked for")), "de", "set: the environment is not read");
        assert_eq!(language("ru_RU.UTF-8").as_deref(), Some("ru"));
        assert_eq!(language("JA_jp").as_deref(), Some("ja"));
        assert_eq!(language("C.UTF-8"), None, "C is none");
        assert_eq!(language("POSIX"), None);
        assert_eq!(language(""), None, "an empty one is none");
    }

    /// the languages, one file each: locales/<language>.yml
    const LANGUAGES: [&str; 6] = ["de", "en", "fr", "ja", "ru", "zh"];

    /// locales/<language>.yml: (line number, key, message), in the file's order
    fn file(lang: &str) -> Vec<(usize, String, String)> {
        let f = format!("{}/locales/{}.yml", env!("CARGO_MANIFEST_DIR"), lang);
        let src = std::fs::read_to_string(&f).unwrap_or_else(|e| panic!("{}: {}", f, e));
        src.lines().enumerate()
            .filter(|(_, l)| !l.starts_with('#') && !l.trim().is_empty() && !l.starts_with("_version"))
            .map(|(n, l)| {
                let (k, v) = l.split_once(": ").unwrap_or_else(|| panic!("{}:{}: not `key: \"message\"`", f, n + 1));
                let v = v.trim();
                assert!(v.len() >= 2 && v.starts_with('"') && v.ends_with('"'), "{}:{}: a message in double quotes", f, n + 1);
                (n, k.to_string(), v[1..v.len() - 1].to_string())
            })
            .collect()
    }

    /// [i18n] one file per language and no other, each key on English's line, each message
    /// with English's placeholders and code
    #[test]
    fn every_message_is_in_every_language_on_the_same_line() {
        let dir = format!("{}/locales", env!("CARGO_MANIFEST_DIR"));
        let mut files: Vec<String> = std::fs::read_dir(&dir).unwrap().map(|f| f.unwrap().file_name().into_string().unwrap()).collect();
        files.sort();
        assert_eq!(files, LANGUAGES.map(|l| format!("{}.yml", l)));
        let shape = |m: &str| {
            let mut p: Vec<String> = m.split("%{").skip(1).map(|r| r.split('}').next().unwrap().to_string()).collect();
            p.sort();
            (m.split(' ').next().filter(|c| c.starts_with("RPPD-E")).map(str::to_string), p)
        };
        let en = file("en");
        assert!(!en.is_empty());
        for l in LANGUAGES {
            let f = file(l);
            assert_eq!(f.len(), en.len(), "{}", l);
            for ((n, k, m), (en_n, en_k, en_m)) in f.iter().zip(en.iter()) {
                assert_eq!((n, k), (en_n, en_k), "{}: a key on another line than English's", l);
                assert!(!m.is_empty(), "{} {}: empty", k, l);
                assert_eq!(shape(m), shape(en_m), "{} {}: the code and placeholders", k, l);
            }
        }
    }

    /// [i18n] the generated clients say not_ready, not tonic's English (build.rs patches them)
    #[test]
    fn the_generated_clients_say_not_ready() {
        for f in ["rppd", "rppg", "rppc"] {
            let src = std::fs::read_to_string(format!("{}/src/protogen/{}.rs", env!("CARGO_MANIFEST_DIR"), f)).unwrap();
            assert!(!src.contains("Service was not ready"), "{}.rs", f);
        }
        let en = rust_i18n::t!("grpc.not_ready", locale = "en", err = "x").into_owned();
        assert!(en.starts_with("RPPD-E160 ") && en.ends_with("x"), "{}", en);
        assert_eq!(rust_i18n::t!("grpc.not_ready", locale = "xx", err = "x").into_owned(), en, "a language without messages is English");
        assert_ne!(rust_i18n::t!("grpc.not_ready", locale = "ru", err = "x").into_owned(), en);
    }
}
