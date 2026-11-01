//! [i18n] rust-i18n reads locales/<language>.yml untracked by cargo: an edit there builds
//! rppd again (as etcds and sge do)
fn main() {
    println!("cargo:rerun-if-changed=locales");
}
