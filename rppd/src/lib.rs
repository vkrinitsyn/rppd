// to use this package as embeded library

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use crate::rd_config::RppdNodeCluster;
use i18n_embed::{
    fluent::{fluent_language_loader, FluentLanguageLoader},
    LanguageLoader,
};
use lazy_static::lazy_static;
use rust_embed::RustEmbed;
use slog::{error, info};
use tonic::transport::Server;
use tonic::transport::server::Router;
use rppd_common::protogen::rppd::rppd_node_server::*;
use rppd_common::protogen::rppd::SwitchRequest;
use rppd_common::protogen::rppg::rppd_trigger_server::RppdTriggerServer;

pub mod arg_config;

pub mod rd_config;
mod rd_fn;
mod rd_queue;

mod py;

mod rd_rpc;
mod cron;
mod rd_monitor;
mod rd_etcd;

#[derive(RustEmbed)]
#[folder = "i18n/"]
struct Localizations;

/// Logging prefix, to start every loggin message with a string to use in a server as a module
#[cfg(feature = "lib-embedded")]
const LP: &'static str = "[py] ";

/// Logging prefix stub
#[cfg(not(feature = "lib-embedded"))]
const LP: &'static str = "";

lazy_static! {
    pub static ref LANGUAGE_LOADER: FluentLanguageLoader = {
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

impl RppdNodeCluster {
    /// called by main.rs
    pub async fn serve(&self) -> Result<(), String> {
        let srv = Server::builder()
            .add_service(RppdNodeServer::new(self.clone()))
            .add_service(RppdTriggerServer::new(self.clone()));

        #[cfg(feature = "etcd-embeded")]
        let srv = self.etcd.read().await.add_all_services(srv);

        let (name, bind, port) = { let cfg = self.cfg.read().await;
            (cfg.name.clone(), cfg.bind.clone(), cfg.port)
        };

        let addrsv:Vec<&str> = bind.split(",").collect();
        let adr: Vec<&str> = if addrsv[0].starts_with("http") {
            let url: Vec<&str> = addrsv[0].split("//").collect();
            url[1].split(":").collect()
        } else {
            addrsv[0].split(":").collect()
        };
        
        let bind = adr[0];
        let adr = if bind.chars().next().unwrap_or(' ').is_numeric() {
            addrsv[0].parse::<SocketAddr>().map_err(|e| format!("parsing {}: {}", bind, e))?
        } else {
            let ips: Vec<std::net::IpAddr> = dns_lookup::lookup_host(bind).expect(format!("Binding to {}", bind).as_str());
            if ips.len() == 0 {
                error!(self.log, "{}No IpAddr found {}", LP, bind);
                std::process::exit(9);
            }
            SocketAddr::new(ips[0], port)
        };

        #[cfg(feature = "etcd-embeded")] info!(self.log, "Starting RPPD&ETCD server [{}] at: {}", name, bind);
        #[cfg(not(feature = "etcd-embeded"))] info!(self.log, "Starting RPPD server [{}] at: {}", name, bind);
        let log = self.log.clone();
        tokio::spawn(async move {
            match srv.serve(adr) // .serve_with_incoming_shutdown(uds_stream, rx.map(drop) )
                .await {
                Ok(()) => {
                    info!(log, "{}bye", LP);
                }
                Err(e) => {
                    error!(log, "{} {}", fl!("error"), e);
                    std::process::exit(10);
                }
            }
        });

        Ok(())
    }

    /// in case of use as embedded lib bound to same port
    pub fn add_all_services(&self, srv: Router) -> Router {
        srv.add_service(RppdNodeServer::new(self.clone()))
            .add_service(RppdTriggerServer::new(self.clone()))
    }
    
    /// giveup master
    pub async fn unmaster(&self) {
        if self.master.load(Ordering::Relaxed) {
            let node_id = self.node_id.load(Ordering::Relaxed);
            let nodes = self.node_connections.read().await;
            for (k, n) in nodes.iter() {
                if k != &node_id { // in case of self link
                    if let Ok(node) = n {
                        if let Ok(x) = node.lock().await.switch(SwitchRequest { node_id }).await {
                            info!(self.log, "{} {:?}", fl!("rppd-switch", from=node_id.to_string(), to=k.to_string()), x);
                            break;
                        }
                    }
                }
            }
        }
    }
}



#[cfg(test)]
mod tests {
    #![allow(warnings, unused)]

    #[tokio::test]
    async fn test_compile() {
        assert!(true);
    }
}
