use std::path::PathBuf;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub addr: String,
    pub wal_path: PathBuf,
    pub wal_sync: bool,
}

impl Config {
    pub fn parse() -> Result<Self> {
        let mut addr = "127.0.0.1:9000".to_string();
        let mut wal_path = PathBuf::from("proxist.wal");
        let mut wal_sync = true;

        let mut args = std::env::args();
        let _ = args.next();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--addr" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::InvalidData("missing --addr value".to_string()))?;
                    addr = value;
                }
                "--wal" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::InvalidData("missing --wal value".to_string()))?;
                    wal_path = PathBuf::from(value);
                }
                "--wal-sync" => {
                    wal_sync = true;
                }
                "--no-wal-sync" => {
                    wal_sync = false;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    return Err(Error::InvalidData(format!(
                        "unknown arg: {other}"
                    )));
                }
            }
        }

        Ok(Self {
            addr,
            wal_path,
            wal_sync,
        })
    }
}

fn print_usage() {
    let usage = r#"proxistd (std-only)\n\nUSAGE:\n  proxistd [--addr 127.0.0.1:9000] [--wal proxist.wal] [--wal-sync|--no-wal-sync]\n\nOPTIONS:\n  --addr         listen address (default 127.0.0.1:9000)\n  --wal          wal file path (default proxist.wal)\n  --wal-sync     fsync after each write (default)\n  --no-wal-sync  skip fsync for higher throughput\n  -h, --help     show help\n"#;
    println!("{usage}");
}
