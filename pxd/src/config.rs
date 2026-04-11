use std::path::PathBuf;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub addr: String,
    pub wal_path: PathBuf,
    pub wal_sync: bool,
    pub data_root: PathBuf,
    pub partition: String,
}

impl Config {
    pub fn parse() -> Result<Self> {
        let mut addr = "127.0.0.1:9000".to_string();
        let mut wal_path = PathBuf::from("pxd.wal");
        let mut wal_sync = true;
        let mut data_root = PathBuf::from("data");
        let mut partition = "default".to_string();

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
                "--data" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::InvalidData("missing --data value".to_string()))?;
                    data_root = PathBuf::from(value);
                }
                "--partition" => {
                    let value = args.next().ok_or_else(|| {
                        Error::InvalidData("missing --partition value".to_string())
                    })?;
                    partition = value;
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
            data_root,
            partition,
        })
    }
}

fn print_usage() {
    let usage = r#"pxd (std-only)\n\nUSAGE:\n  pxd [--addr 127.0.0.1:9000] [--wal pxd.wal] [--wal-sync|--no-wal-sync]\n      [--data data] [--partition default]\n\nOPTIONS:\n  --addr         listen address (default 127.0.0.1:9000)\n  --wal          wal file path (default pxd.wal)\n  --wal-sync     fsync after each write (default)\n  --no-wal-sync  skip fsync for higher throughput\n  --data         splayed data root (default data)\n  --partition    partition directory name (default default)\n  -h, --help     show help\n"#;
    println!("{usage}");
}
