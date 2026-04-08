use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use pxd::pxl::{encode_insert_payload, encode_schema_payload, Op};
use pxd::types::{ColumnSpec, ColumnType, Schema, Value};

use px_chd::{run, Config};

const WAL_MAGIC: [u8; 2] = *b"PW";
const WAL_VERSION: u8 = 2;
const WAL_HEADER_LEN: usize = 2 + 1 + 1 + 4 + 4 + 8;

#[test]
#[ignore]
fn clickhouse_ingest_from_wal() {
    let temp_dir = std::env::temp_dir();
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    let wal_path = temp_dir.join(format!("px_chd_test_{suffix}.wal"));
    let checkpoint_path = temp_dir.join(format!("px_chd_test_{suffix}.checkpoint"));

    let schema = Schema::new(vec![
        ColumnSpec {
            name: "symbol".to_string(),
            col_type: ColumnType::String,
            nullable: false,
        },
        ColumnSpec {
            name: "ts".to_string(),
            col_type: ColumnType::I64,
            nullable: false,
        },
        ColumnSpec {
            name: "price".to_string(),
            col_type: ColumnType::F64,
            nullable: false,
        },
    ])
    .expect("schema");

    let payload = encode_schema_payload("ticks", &schema).expect("encode schema");
    write_wal_record(&wal_path, 1, Op::Create, &payload);

    let payload = encode_insert_payload(
        "ticks",
        &[
            "symbol".to_string(),
            "ts".to_string(),
            "price".to_string(),
        ],
        &[vec![
            Value::String("AAPL".to_string()),
            Value::I64(1),
            Value::F64(10.0),
        ]],
    )
    .expect("encode insert");
    write_wal_record(&wal_path, 2, Op::Insert, &payload);

    let clickhouse_url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:18123".to_string());

    let config = Config {
        clickhouse_url,
        database: "proxist".to_string(),
        wal_path: wal_path.clone(),
        checkpoint_path: checkpoint_path.clone(),
        batch_rows: 128,
        poll_interval: std::time::Duration::from_millis(10),
        flush_interval: std::time::Duration::from_millis(10),
        once: true,
    };

    run(config).expect("run px-chd");

    let count = query_scalar("SELECT count() FROM proxist.ticks");
    assert_eq!(count.trim(), "1");

    let _ = query_scalar("DROP TABLE IF EXISTS proxist.ticks");
    let _ = fs::remove_file(&wal_path);
    let _ = fs::remove_file(&checkpoint_path);
}

fn write_wal_record(path: &PathBuf, lsn: u64, op: Op, payload: &[u8]) {
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .expect("open wal");
    let checksum = checksum_u32(payload);
    let mut header = [0u8; WAL_HEADER_LEN];
    header[0..2].copy_from_slice(&WAL_MAGIC);
    header[2] = WAL_VERSION;
    header[3] = op as u8;
    header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[8..12].copy_from_slice(&checksum.to_le_bytes());
    header[12..20].copy_from_slice(&lsn.to_le_bytes());
    file.write_all(&header).expect("write header");
    file.write_all(payload).expect("write payload");
}

fn checksum_u32(bytes: &[u8]) -> u32 {
    let mut sum = 0u32;
    for b in bytes {
        sum = sum.wrapping_add(*b as u32);
    }
    sum
}

fn query_scalar(sql: &str) -> String {
    let url =
        std::env::var("CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:18123".to_string());
    let resp = ureq::post(&url)
        .set("Content-Type", "text/plain")
        .send_string(sql)
        .expect("clickhouse query");
    resp.into_string().expect("response body")
}
