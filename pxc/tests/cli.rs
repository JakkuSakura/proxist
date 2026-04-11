use std::io::Cursor;
use std::process::Command;

use pxd::pxl::{
    decode_delete_payload, decode_insert_payload, decode_query_col_payload, read_frame, Op,
};

fn hex_to_bytes(hex: &str) -> Vec<u8> {
    let hex = hex.trim();
    assert!(hex.len() % 2 == 0, "hex output length must be even");
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i + 2], 16).expect("valid hex");
        bytes.push(byte);
    }
    bytes
}

fn decode_frame(bytes: Vec<u8>) -> pxd::pxl::Frame {
    let mut cursor = Cursor::new(bytes);
    read_frame(&mut cursor)
        .expect("read frame")
        .expect("frame present")
}

#[test]
fn cli_sql_to_insert_hex() {
    let exe = env!("CARGO_BIN_EXE_pxc");
    let output = Command::new(exe)
        .args([
            "--sql",
            "INSERT INTO ticks (symbol, ts, value) VALUES ('AAPL', 7, 1.5)",
            "--req-id",
            "7",
            "--hex",
        ])
        .output()
        .expect("run pxc");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let bytes = hex_to_bytes(stdout.trim());
    let frame = decode_frame(bytes);
    assert_eq!(frame.req_id, 7);
    assert_eq!(frame.op, Op::Insert);
    let (table, cols, rows) = decode_insert_payload(&frame.payload).expect("payload");
    assert_eq!(table, "ticks");
    assert_eq!(cols, vec!["symbol", "ts", "value"]);
    assert_eq!(rows.len(), 1);
}

#[test]
fn cli_prql_to_query_hex() {
    let exe = env!("CARGO_BIN_EXE_pxc");
    let prql = r#"from ticks
| filter symbol == "AAPL"
| select {value}"#;
    let output = Command::new(exe)
        .args(["--prql", prql, "--req-id", "9", "--hex"])
        .output()
        .expect("run pxc");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let bytes = hex_to_bytes(stdout.trim());
    let frame = decode_frame(bytes);
    assert_eq!(frame.req_id, 9);
    assert_eq!(frame.op, Op::QueryCol);
    let query = decode_query_col_payload(&frame.payload).expect("payload");
    assert_eq!(query.table, "ticks");
}

#[test]
fn cli_sql_to_delete_hex() {
    let exe = env!("CARGO_BIN_EXE_pxc");
    let output = Command::new(exe)
        .args([
            "--sql",
            "DELETE FROM ticks WHERE symbol = 'AAPL'",
            "--req-id",
            "11",
            "--hex",
        ])
        .output()
        .expect("run pxc");
    assert!(output.status.success(), "{output:?}");
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    let bytes = hex_to_bytes(stdout.trim());
    let frame = decode_frame(bytes);
    assert_eq!(frame.req_id, 11);
    assert_eq!(frame.op, Op::Delete);
    let (table, filter) = decode_delete_payload(&frame.payload).expect("payload");
    assert_eq!(table, "ticks");
    assert!(filter.is_some());
}
