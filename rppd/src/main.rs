/*%LPH%*/

#![allow(warnings, unused, dead_code)]
// #![allow(non_snake_case)]

extern crate core;
#[macro_use]
extern crate slog;

use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::Ordering;

use bb8_postgres::tokio_postgres::GenericClient;
use i18n_embed::{
	fluent::{fluent_language_loader, FluentLanguageLoader},
	LanguageLoader,
};
use lazy_static::*;
use lazy_static::lazy_static;
use rust_embed::RustEmbed;
use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::mpsc::Sender;
use tonic::transport::Server;

use rppd_common::gen::rppd::rppd_node_server::*;
use rppd_common::gen::rppd::SwitchRequest;
use crate::rd_config::Cluster;

mod arg_config;

mod rd_config;
mod rd_fn;
mod rd_queue;

mod py;

mod rd_rpc;
mod cron;
mod rd_monitor;

#[derive(RustEmbed)]
#[folder = "i18n/"]
struct Localizations;

type Queue = Sender<String>;

lazy_static! {
    static ref LANGUAGE_LOADER: FluentLanguageLoader = {
		let loader: FluentLanguageLoader = fluent_language_loader!();
		loader
		.load_languages(&Localizations, &[loader.fallback_language().clone()])
		.unwrap();
		loader
    };

}

#[macro_export]
macro_rules! fl {
    ($message_id:literal) => {{
        i18n_embed_fl::fl!($crate::LANGUAGE_LOADER, $message_id)
    }};

    ($message_id:literal, $($args:expr),*) => {{
        i18n_embed_fl::fl!($crate::LANGUAGE_LOADER, $message_id, $($args), *)
    }};
}

/// db connection is required
#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() -> Result<(), String> {
    let app_name = fl!("rppd-name");
    println!("{} {}", app_name, env!("CARGO_PKG_VERSION"));
    println!("{}", fl!("rppd-about"));
    println!();

    let input: Vec<String> = std::env::args_os().map(|e| e.to_string_lossy().to_string()).collect();
    match arg_config::ArgConfig::new(input) {
        Ok(c) => {
            let ips: Vec<std::net::IpAddr> = dns_lookup::lookup_host(c.bind.as_str()).expect(format!("Binding to {}", c.bind).as_str());
            if ips.len() == 0 {
                eprintln!("No IpAddr found {}", c.bind);
                std::process::exit(9);
            }
            let adr = SocketAddr::new(ips[0], c.port);
            let srv = Cluster::init(c).await?;

            let master = srv.master.clone();
            let node_id = srv.node_id.clone();
            let nodes = srv.node_connections.clone();

            tokio::spawn(async move {
                match Server::builder()
                    .add_service(RppdNodeServer::new(srv))
                    .serve(adr) // .serve_with_incoming_shutdown(uds_stream, rx.map(drop) )
                    .await {
                    Ok(()) => {
                        println!("bye");
                    }
                    Err(e) => {
                        eprintln!("{} {}", fl!("error"), e);
                        println!();
                        println!("{}", arg_config::usage());
                        std::process::exit(10);
                    }
                }
            });
            let sht = wait_for_signal().await;
            if master.load(Ordering::Relaxed) {
                let node_id = node_id.load(Ordering::Relaxed);
                let nodes = nodes.read().await;
                for (k, n) in nodes.iter() {
                    if k != &node_id { // in case of self link
                        if let Ok(node) = n {
                            if let Ok(x) = node.lock().await.switch(SwitchRequest { node_id }).await {
                                println!("{} {:?}", fl!("rppd-switch", from=node_id.to_string(), to=k.to_string()), x);
                            }
                        }
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("{} {}", fl!("error"), e);
            println!();
            println!("{}", arg_config::usage());
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

#[cfg(test)]
mod tests {
    #![allow(warnings, unused)]

	#[tokio::test]
    async fn test_compile() {
        assert!(true);
    }
}
