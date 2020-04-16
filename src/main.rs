// Copyright Rivtower Technologies LLC.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod db;

use clap::Clap;
use git_version::git_version;
use log::{debug, info, warn};

const GIT_VERSION: &str = git_version!(
    args = ["--tags", "--always", "--dirty=-modified"],
    fallback = "unknown"
);
const GIT_HOMEPAGE: &str = "https://github.com/rink1969/cita_ng_storage";

/// network service
#[derive(Clap)]
#[clap(version = "0.1.0", author = "Rivtower Technologies.")]
struct Opts {
    #[clap(subcommand)]
    subcmd: SubCommand,
}

#[derive(Clap)]
enum SubCommand {
    /// print information from git
    #[clap(name = "git")]
    GitInfo,
    /// run this service
    #[clap(name = "run")]
    Run(RunOpts),
}

/// A subcommand for run
#[derive(Clap)]
struct RunOpts {
    /// Sets grpc port of config service.
    #[clap(short = "c", long = "config_port", default_value = "49999")]
    config_port: String,
    /// Sets grpc port of this service.
    #[clap(short = "p", long = "port", default_value = "50003")]
    grpc_port: String,
    /// Sets db path.
    #[clap(short = "d", long = "db", default_value = "storage.db")]
    db_path: String,
}

fn main() {
    ::std::env::set_var("RUST_BACKTRACE", "full");

    let opts: Opts = Opts::parse();

    match opts.subcmd {
        SubCommand::GitInfo => {
            println!("git version: {}", GIT_VERSION);
            println!("homepage: {}", GIT_HOMEPAGE);
        }
        SubCommand::Run(opts) => {
            // init log4rs
            log4rs::init_file("storage-log4rs.yaml", Default::default()).unwrap();
            info!("grpc port of config service: {}", opts.config_port);
            info!("grpc port of this service: {}", opts.grpc_port);
            info!("db path of this service: {}", opts.db_path);
            let _ = run(opts);
        }
    }
}

use cita_ng_proto::config::{
    config_service_client::ConfigServiceClient, Endpoint, RegisterEndpointInfo,
};

async fn register_endpoint(
    config_port: String,
    port: String,
) -> Result<bool, Box<dyn std::error::Error>> {
    let config_addr = format!("http://127.0.0.1:{}", config_port);
    let mut client = ConfigServiceClient::connect(config_addr).await?;

    // id of Storage service is 3
    let request = Request::new(RegisterEndpointInfo {
        id: 3,
        endpoint: Some(Endpoint {
            hostname: "127.0.0.1".to_owned(),
            port,
        }),
    });

    let response = client.register_endpoint(request).await?;

    Ok(response.into_inner().is_success)
}

use cita_ng_proto::common::SimpleResponse;
use cita_ng_proto::storage::{
    storage_service_server::StorageService, storage_service_server::StorageServiceServer, Content,
    ExtKey, Value,
};
use parking_lot::RwLock;
use tonic::{transport::Server, Request, Response, Status};

use db::DB;
use std::marker::Send;
use std::marker::Sync;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub struct StorageServer {
    db: Arc<RwLock<DB>>,
}

unsafe impl Sync for StorageServer {}
unsafe impl Send for StorageServer {}

impl StorageServer {
    fn new(db: DB) -> Self {
        StorageServer {
            db: Arc::new(RwLock::new(db)),
        }
    }
}

#[tonic::async_trait]
impl StorageService for StorageServer {
    async fn store(&self, request: Request<Content>) -> Result<Response<SimpleResponse>, Status> {
        debug!("store request: {:?}", request);

        let content = request.into_inner();
        let region = content.region;
        let key = content.key;
        let value = content.value;

        let ret = self.db.read().store(region, key, value);
        if ret.is_err() {
            debug!("store error: {:?}", ret);
            Err(Status::internal("db store failed"))
        } else {
            let reply = SimpleResponse { is_success: true };
            Ok(Response::new(reply))
        }
    }

    async fn load(&self, request: Request<ExtKey>) -> Result<Response<Value>, Status> {
        debug!("load request: {:?}", request);

        let ext_key = request.into_inner();
        let region = ext_key.region;
        let key = ext_key.key;

        let ret = self.db.read().load(region, key);
        if let Ok(value) = ret {
            let reply = Value { value };
            Ok(Response::new(reply))
        } else {
            debug!("load error: {:?}", ret);
            Err(Status::internal("db load failed"))
        }
    }

    async fn delete(&self, request: Request<ExtKey>) -> Result<Response<SimpleResponse>, Status> {
        debug!("delete request: {:?}", request);

        let ext_key = request.into_inner();
        let region = ext_key.region;
        let key = ext_key.key;

        let ret = self.db.read().delete(region, key);
        if ret.is_err() {
            debug!("delete error: {:?}", ret);
            Err(Status::internal("db delete failed"))
        } else {
            let reply = SimpleResponse { is_success: true };
            Ok(Response::new(reply))
        }
    }
}

#[tokio::main]
async fn run(opts: RunOpts) -> Result<(), Box<dyn std::error::Error>> {
    let addr_str = format!("127.0.0.1:{}", opts.grpc_port);
    let addr = addr_str.parse()?;

    // init db
    let db = DB::new(&opts.db_path);

    let storage_server = StorageServer::new(db);

    tokio::spawn(async move {
        loop {
            // register endpoint
            let ret = register_endpoint(opts.config_port.clone(), opts.grpc_port.clone()).await;
            if ret.is_ok() && ret.unwrap() {
                info!("register endpoint success!");
                break;
            }
            warn!("register endpoint failed! Retrying");
            thread::sleep(Duration::new(3, 0));
        }
    });

    Server::builder()
        .add_service(StorageServiceServer::new(storage_server))
        .serve(addr)
        .await?;

    Ok(())
}
