//! [i18n] The language of rppd's messages - errors, log lines, the command line's help, what
//! a Python function is refused with - as etcds's and sge's are (the owner's, 2026-11-01).
//!
//! The language is rppd-common's (rppd_common::i18n): the first of set_lang - what an
//! embedder sets, as ytserv does with its own language; the environment's LC_ALL,
//! LC_MESSAGES or LANG, the first set; English. One for both crates, so the generated gRPC
//! clients' messages are in it too.
//!
//! The messages are in locales/<language>.yml, one file per language, each with every key
//! on the same line: en, ru, and - not reviewed - fr, de, zh (Simplified Chinese), ja. An
//! error's starts with its code, `RPPD-Ennn`, the same in every language, so it can be
//! looked up whatever the language.
//!
//! The language is the process's, not a call's: one rppd, one language.

pub use rppd_common::i18n::{lang, set_lang};

/// [i18n] The message `key` (locales/<language>.yml) in the language of [lang], each
/// %{name} placeholder filled from `name = value` (any Display)
#[macro_export]
macro_rules! tr {
    ($key:expr $(, $name:ident = $value:expr)* $(,)?) => {
        rust_i18n::t!($key, locale = &$crate::i18n::lang() $(, $name = $value)*).into_owned()
    };
}

/// [i18n] tr! for a debug line: the message is looked up and filled only when the line is
/// written - slog formats a record past its level filter, so with debug off it costs nothing
#[macro_export]
macro_rules! tr_lazy {
    ($key:expr $(, $name:ident = $value:expr)* $(,)?) => {
        $crate::i18n::Lazy(|| $crate::tr!($key $(, $name = $value)*))
    };
}

/// tr_lazy!'s: a message made when it is displayed
pub struct Lazy<F: Fn() -> String>(pub F);

impl<F: Fn() -> String> std::fmt::Display for Lazy<F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&(self.0)())
    }
}

#[cfg(test)]
mod tests {
    /// [i18n] a language without messages, or a key without one in it, is English
    #[test]
    fn a_message_falls_back_to_english() {
        let en = rust_i18n::t!("etcd.not_ready", locale = "en", err = "x").into_owned();
        assert!(en.starts_with("RPPD-E"), "{}", en);
        assert_eq!(rust_i18n::t!("etcd.not_ready", locale = "xx", err = "x").into_owned(), en);
        assert_ne!(rust_i18n::t!("etcd.not_ready", locale = "ru", err = "x").into_owned(), en);
    }

    /// [i18n] tr_lazy!'s message is made when the line is written, not when it is logged
    #[test]
    fn a_lazy_message_is_made_only_when_written() {
        let made = std::cell::Cell::new(0);
        let m = super::Lazy(|| { made.set(made.get() + 1); "x".to_string() });
        assert_eq!(made.get(), 0, "not made before it is written");
        assert_eq!(m.to_string(), "x");
        assert_eq!(made.get(), 1);
        let en = crate::tr!("lib.bye");
        assert_eq!(crate::tr_lazy!("lib.bye").to_string(), en, "the same message as tr!");
    }

    /// the languages, one file each: locales/<language>.yml
    const LANGUAGES: [&str; 6] = ["de", "en", "fr", "ja", "ru", "zh"];

    /// locales/<language>.yml: (line number, key, message), in the file's order. One key to
    /// a line, `key: "message"` - the files are written so, and read so here
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

    /// [i18n] one file per language and no other, each with the same keys on the same lines
    /// as English's - a message is found at one line number in every language
    #[test]
    fn every_language_has_its_file_with_the_keys_in_order() {
        let dir = format!("{}/locales", env!("CARGO_MANIFEST_DIR"));
        let mut files: Vec<String> = std::fs::read_dir(&dir).unwrap().map(|f| f.unwrap().file_name().into_string().unwrap()).collect();
        files.sort();
        assert_eq!(files, LANGUAGES.map(|l| format!("{}.yml", l)));
        let keys = |l: &str| file(l).into_iter().map(|(n, k, _)| (n, k)).collect::<Vec<_>>();
        for l in LANGUAGES {
            assert_eq!(keys(l), keys("en"), "{}: a key on another line than English's", l);
        }
        let en = keys("en");
        let mut seen = std::collections::BTreeSet::new();
        for (_, k) in &en {
            assert!(seen.insert(k.clone()), "{} twice", k);
        }
    }

    /// [i18n] every message in every language, with the same placeholders; an error's code
    /// the same in every language, and no two errors with one code
    #[test]
    fn every_message_is_in_every_language() {
        let placeholders = |m: &str| {
            let mut p: Vec<String> = m.split("%{").skip(1).map(|r| r.split('}').next().unwrap().to_string()).collect();
            p.sort();
            p
        };
        let code = |m: &str| m.split(' ').next().filter(|c| c.starts_with("RPPD-E")).map(str::to_string);
        let en: Vec<(String, String)> = file("en").into_iter().map(|(_, k, m)| (k, m)).collect();
        let mut codes = std::collections::BTreeMap::new();
        for l in LANGUAGES {
            for ((k, m), (ek, em)) in file(l).into_iter().map(|(_, k, m)| (k, m)).zip(en.iter()) {
                assert_eq!(&k, ek);
                assert!(!m.is_empty(), "{} {}: empty", k, l);
                assert_eq!(placeholders(&m), placeholders(em), "{} {}", k, l);
                assert_eq!(code(&m), code(em), "{} {}: the code", k, l);
            }
        }
        for (k, m) in &en {
            if let Some(c) = code(m) {
                assert!(codes.insert(c.clone(), k.clone()).is_none(), "{} for {} and {}", c, codes[&c], k);
            }
        }
        assert!(codes.len() > 40, "{}", codes.len());
    }

    /// [i18n] every key the code asks for is in English's file, and every key there is asked for
    #[test]
    fn every_key_asked_for_is_there_and_every_one_there_is_asked_for() {
        // every source file but this one, whose own text names the macros
        let src: String = std::fs::read_dir(format!("{}/src", env!("CARGO_MANIFEST_DIR"))).unwrap()
            .map(|f| f.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "rs") && !p.ends_with("i18n.rs"))
            .map(|p| std::fs::read_to_string(p).unwrap())
            .collect();
        // the macros themselves, not the end of another one's name (include_str!, c_str!)
        let asked: std::collections::BTreeSet<String> = ["tr!(\"", "tr_lazy!(\""].iter()
            .flat_map(|m| src.match_indices(m).collect::<Vec<_>>())
            .filter(|(i, _)| !src[..*i].ends_with(|c: char| c.is_alphanumeric() || c == '_'))
            .map(|(i, m)| src[i + m.len()..].split('"').next().unwrap().to_string()).collect();
        let there: std::collections::BTreeSet<String> = file("en").into_iter().map(|(_, k, _)| k).collect();
        assert_eq!(asked.difference(&there).collect::<Vec<_>>(), Vec::<&String>::new(), "asked for, not in en.yml");
        assert_eq!(there.difference(&asked).collect::<Vec<_>>(), Vec::<&String>::new(), "in en.yml, asked for nowhere");
    }
}
