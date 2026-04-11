mod config;
mod error;
mod expr;
mod memstore;
mod net;
mod pxl;
mod storage;
mod types;
mod wal;

use std::fs;
use std::sync::{Arc, Mutex, RwLock};

use crate::config::Config;
use crate::error::Result;
use crate::memstore::MemStore;
use crate::net::Server;
use crate::storage::{SplayedStore, StoreConfig};
use crate::wal::Wal;

fn main() {
    if let Err(err) = run() {
        eprintln!("pxd error: {err}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let config = Config::parse()?;

    let store = SplayedStore::open(StoreConfig {
        root: config.data_root.clone(),
        partition: config.partition.clone(),
    })?;
    let mut mem = MemStore::with_store(store);
    if wal_is_empty(&config.wal_path)? {
        mem.load_from_store()?;
    }
    let wal = Wal::open(&config.wal_path, &mut mem, config.wal_sync)?;

    let mem = Arc::new(RwLock::new(mem));
    let wal = Arc::new(Mutex::new(wal));

    let server = Server::bind(config.addr, mem, Some(wal));
    server.serve()?;
    Ok(())
}

fn wal_is_empty(path: &std::path::Path) -> Result<bool> {
    match fs::metadata(path) {
        Ok(meta) => Ok(meta.len() == 0),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(err) => Err(err.into()),
    }
}
