use std::path::PathBuf;
use std::time::Duration;

use crate::{Error, Result};

#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse_url: String,
    pub database: String,
    pub wal_path: PathBuf,
    pub checkpoint_path: PathBuf,
    pub batch_rows: usize,
    pub poll_interval: Duration,
    pub flush_interval: Duration,
    pub once: bool,
}

impl Config {
    pub fn parse() -> Result<Self> {
        let mut clickhouse_url = "http://127.0.0.1:8123".to_string();
        let mut database = "proxist".to_string();
        let mut wal_path = PathBuf::from("pxd.wal");
        let mut checkpoint_path = PathBuf::from("px-chd.checkpoint");
        let mut batch_rows: usize = 1024;
        let mut poll_ms: u64 = 200;
        let mut flush_ms: u64 = 1000;
        let mut once = false;

        let mut args = std::env::args();
        let _ = args.next();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--ch-url" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::InvalidData("missing --ch-url value".to_string()))?;
                    clickhouse_url = value;
                }
                "--db" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::InvalidData("missing --db value".to_string()))?;
                    database = value;
                }
                "--wal" => {
                    let value = args
                        .next()
                        .ok_or_else(|| Error::InvalidData("missing --wal value".to_string()))?;
                    wal_path = PathBuf::from(value);
                }
                "--checkpoint" => {
                    let value = args.next().ok_or_else(|| {
                        Error::InvalidData("missing --checkpoint value".to_string())
                    })?;
                    checkpoint_path = PathBuf::from(value);
                }
                "--batch-rows" => {
                    let value = args.next().ok_or_else(|| {
                        Error::InvalidData("missing --batch-rows value".to_string())
                    })?;
                    batch_rows = value.parse::<usize>().map_err(|_| {
                        Error::InvalidData("invalid --batch-rows value".to_string())
                    })?;
                }
                "--poll-ms" => {
                    let value = args.next().ok_or_else(|| {
                        Error::InvalidData("missing --poll-ms value".to_string())
                    })?;
                    poll_ms = value.parse::<u64>().map_err(|_| {
                        Error::InvalidData("invalid --poll-ms value".to_string())
                    })?;
                }
                "--flush-ms" => {
                    let value = args.next().ok_or_else(|| {
                        Error::InvalidData("missing --flush-ms value".to_string())
                    })?;
                    flush_ms = value.parse::<u64>().map_err(|_| {
                        Error::InvalidData("invalid --flush-ms value".to_string())
                    })?;
                }
                "--once" => {
                    once = true;
                }
                "--help" | "-h" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    return Err(Error::InvalidData(format!("unknown arg: {other}")));
                }
            }
        }

        Ok(Self {
            clickhouse_url,
            database,
            wal_path,
            checkpoint_path,
            batch_rows,
            poll_interval: Duration::from_millis(poll_ms),
            flush_interval: Duration::from_millis(flush_ms),
            once,
        })
    }
}

fn print_usage() {
    let usage = r#"px-chd (ClickHouse WAL bridge)

USAGE:
  px-chd [--ch-url http://127.0.0.1:8123] [--db proxist] [--wal pxd.wal]
         [--checkpoint px-chd.checkpoint] [--batch-rows 1024]
         [--poll-ms 200] [--flush-ms 1000] [--once]

OPTIONS:
  --ch-url        ClickHouse HTTP endpoint (default http://127.0.0.1:8123)
  --db            ClickHouse database (default proxist)
  --wal           WAL file path (default pxd.wal)
  --checkpoint    checkpoint file path (default px-chd.checkpoint)
  --batch-rows    max rows per insert batch (default 1024)
  --poll-ms       WAL poll interval in ms (default 200)
  --flush-ms      force flush interval in ms (default 1000)
  --once          exit after draining WAL
  -h, --help      show help
"#;
    println!("{usage}");
}
