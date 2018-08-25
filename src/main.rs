extern crate serde;
extern crate serde_json;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate log;
extern crate env_logger;

mod server;
mod wal;
mod command;

use std::error::Error;
use std::fs::File;
use std::net::SocketAddr;

fn setup_logger() {
    let env = env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info");
    env_logger::Builder::from_env(env).init();
}

fn main() -> Result<(), Box<Error>> {
    setup_logger();

    let entry1 = wal::WalEntry::Write {
        transaction_id: 100,
        key: "hello".to_string(),
        value: "world".to_string(),
    };
    let entry2 = wal::WalEntry::Commit {
        transaction_id: 101,
    };

    let file = File::create("/tmp/wal.txt")?;

    let mut wal_writer = wal::WalWriter::new(file);
    wal_writer.write(&entry1)?;
    wal_writer.write(&entry2)?;

    let addr: SocketAddr = "0.0.0.0:5555".parse()?;
    let s = server::Server::new(addr);
    s.listen_and_serve()?;

    Ok(())
}
