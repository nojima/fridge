extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
extern crate byteorder;
extern crate env_logger;
extern crate protobuf;

mod command;
mod database;
mod protos;
mod server;
mod wal;

use std::error::Error;
use std::net::SocketAddr;
use std::path::Path;

fn setup_logger() {
    let env = env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info");
    env_logger::Builder::from_env(env).init();
}

fn main() -> Result<(), Box<Error>> {
    setup_logger();

    let path = &Path::new("/tmp/wal-v2.bin");
    let database = database::Database::open(path)?;

    let addr: SocketAddr = "0.0.0.0:5555".parse()?;
    let mut s = server::Server::new(addr, database);
    s.listen_and_serve()?;

    Ok(())
}
