/*
RPPD - Rust Python Postgres Discovery

This file is part of RPPD.

RPPD is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version OR the.

RPPD is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with RPPD.  If not, see <http://www.gnu.org/licenses/>.

The portion above shall not be modified.
*/

//#![allow(warnings, unused, dead_code)]
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
			let adr = SocketAddr::new(IpAddr::from_str(c.bind.as_str()).unwrap(), c.port);
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
