/*%LPH%*/
#![allow(warnings, unused, dead_code)]
extern crate core;
#[macro_use]
extern crate slog;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use bb8_postgres::tokio_postgres::GenericClient;

use lazy_static::*;
use lazy_static::lazy_static;
use rust_embed::RustEmbed;
use slog::Logger;
use sloggers::Build;
use sloggers::terminal::{Destination, TerminalLoggerBuilder};
use sloggers::types::{Severity, SourceLocation};
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::Sender;
use tonic::transport::Server;

use rppd_common::protogen::rppd::rppd_node_server::*;
use rppd_common::protogen::rppd::SwitchRequest;
use rppde::arg_config::RppdConfig;
use rppde::{arg_config, fl};
use rppde::rd_config::RppdNodeCluster;

#[cfg(feature = "etcd-provided")]
compile_error!("only etcd-embeded or etcd-external feature can be enabled at this main compile; to build lib only use: '--features etcd-provided --no-default-features --lib'");

/// db connection is required
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), String> {
    let log = logger(true);

    let app_name = fl!("rppd-name");
    info!(log, "{} {}", app_name, env!("CARGO_PKG_VERSION"));
    info!(log, "{}", fl!("rppd-about"));

    let input: Vec<String> = std::env::args_os().map(|e| e.to_string_lossy().to_string()).collect();
    match RppdConfig::new(input) {
        Ok(c) => {
            let log = logger(c.verbose);
            let srv = RppdNodeCluster::init(c, log.clone()).await?;
            let _ = srv.serve().await?;
            let sht = wait_for_signal().await;
            srv.unmaster().await
        }
        Err(e) => {
            error!(log, "{} {}", fl!("error"), e);
            info!(log, "{}", arg_config::usage());
            std::process::exit(22);
        }
    }
    Ok(())
}

async fn wait_for_signal() -> Result<String, String> {
    let mut int_stream = signal(SignalKind::interrupt()).map_err(|e| e.to_string())?;
    let mut term_stream = signal(SignalKind::terminate()).map_err(|e| e.to_string())?;
    let mut hangup_stream = signal(SignalKind::hangup()).map_err(|e| e.to_string())?;
    let sig = tokio::select! {
		_ = int_stream.recv() => {"interrupt"},
		_ = term_stream.recv() => {"terminate"},
		_ = hangup_stream.recv() => {"hangup"},
	};
    Ok(sig.to_string())
}

pub fn logger(verbose: bool) -> Logger {
    let mut builder = TerminalLoggerBuilder::new();
    builder.channel_size(10240);
    builder.destination(Destination::Stdout);

    #[cfg(debug_assertions)]
    builder.source_location(SourceLocation::LocalFileAndLine);

    #[cfg(not(debug_assertions))]
    builder.source_location(SourceLocation::None);

    builder.level(severity(verbose));
    builder.overflow_strategy(sloggers::types::OverflowStrategy::Drop);
    builder.build().unwrap()
}

#[inline]
#[cfg(debug_assertions)]
fn severity(verbose: bool) -> Severity {
    if verbose { Severity::Trace }
    else { Severity::Debug }
}

#[cfg(not(debug_assertions))]
fn severity(verbose: bool) -> Severity {
    if verbose { Severity::Debug }
    else { Severity::Info }
}

#[cfg(test)]
mod tests {
    #![allow(warnings, unused)]

	#[tokio::test]
    async fn test_compile() {
        assert!(true);
    }
}
