/*%LPH%*/

#![allow(warnings, unused, dead_code)]
// #![allow(non_snake_case)]

#[macro_use]
extern crate slog;
extern crate core;

use std::net::{IpAddr, SocketAddr};
use lazy_static::*;


use bb8_postgres::tokio_postgres::{GenericClient, NoTls};

mod arg_config;

mod gen;
mod rd_config;
mod rd_fn;
mod rd_queue;

mod py;

mod rd_rpc;
mod cron;

use i18n_embed::{
	fluent::{fluent_language_loader, FluentLanguageLoader},
	LanguageLoader,
};
use std::str::FromStr;

use rust_embed::RustEmbed;
use lazy_static::lazy_static;
use tokio::sync::mpsc::Sender;
use tonic::transport::{Channel, Server};

use crate::gen::rg::grpc_server::{Grpc, GrpcServer};
use crate::rd_config::Cluster;


#[derive(RustEmbed)]
#[folder = "i18n/"]
struct Localizations;
type Queue = Sender<String>;

lazy_static! {
    static ref LANGUAGE_LOADER: FluentLanguageLoader = {
		let loader: FluentLanguageLoader = fluent_language_loader!();
		loader
		.load_languages(&Localizations, &[loader.fallback_language()])
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

	let input:Vec<String> = std::env::args_os().map(|e| e.to_string_lossy().to_string()).collect();
	match arg_config::ArgConfig::new(input) {
		Ok(c) => {
			let ips: Vec<std::net::IpAddr> = dns_lookup::lookup_host(c.bind.as_str()).expect(format!("Binding to {}", c.bind).as_str());
			if ips.len() == 0 {
				eprintln!("No IpAddr found {}", c.bind);
				std::process::exit(9);
			}
			let adr = SocketAddr::new(ips[0], c.port);
			let srv = Cluster::init(c).await?;

			match Server::builder()
				.add_service(GrpcServer::new(srv))
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

#[cfg(test)]
mod tests {
#![allow(warnings, unused)]
	use super::*;

	#[tokio::test]
	async fn test_compile() {
		assert!(true);
	}
}
