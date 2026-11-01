#![forbid(unsafe_code)]

/// RPPD - protibuf types and services 
pub mod protogen;

rust_i18n::i18n!("locales", fallback = "en");

/// [i18n] the messages' language, rppd's too, and the generated clients' messages
pub mod i18n;

pub const CFG_TABLE: &str = "rppd_config";
