mod config;
mod error;
mod expr;
mod memstore;
mod net;
mod pxl;
mod query;
mod types;
mod wal;

use std::sync::{Arc, Mutex, RwLock};

use crate::config::Config;
use crate::error::Result;
use crate::memstore::MemStore;
use crate::net::Server;
use crate::wal::Wal;

fn main() {
    if let Err(err) = run() {
        eprintln!("pxd error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = Config::parse()?;

    let mut mem = MemStore::new();
    let wal = Wal::open(&config.wal_path, &mut mem, config.wal_sync)?;

    let mem = Arc::new(RwLock::new(mem));
    let wal = Arc::new(Mutex::new(wal));

    let server = Server::bind(config.addr, mem, Some(wal));
    server.serve()?;
    Ok(())
}
