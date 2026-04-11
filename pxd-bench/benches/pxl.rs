use criterion::{black_box, Criterion};
use pxd::pxl::{
    encode_frame, encode_query_col_payload, read_frame, ColumnProjectExpr, ColumnProjectItem,
    ColumnQuery, Frame, Op,
};
use pxd::types::Value;
use std::io::Cursor;

fn build_frame() -> Frame {
    let query = ColumnQuery {
        table: "ticks".to_string(),
        columns: vec!["symbol".to_string()],
        filter: vec![],
        project: vec![
            ColumnProjectItem {
                name: "symbol".to_string(),
                expr: ColumnProjectExpr::Column(0),
            },
            ColumnProjectItem {
                name: "one".to_string(),
                expr: ColumnProjectExpr::Literal(Value::I64(1)),
            },
        ],
    };
    let payload = encode_query_col_payload(&query).expect("payload");
    Frame {
        flags: 0,
        req_id: 7,
        op: Op::QueryCol,
        payload,
    }
}

fn bench_pxl_encode(c: &mut Criterion) {
    let frame = build_frame();
    c.bench_function("pxl_encode_frame", |b| {
        b.iter(|| {
            let bytes = encode_frame(black_box(&frame));
            black_box(bytes);
        });
    });
}

fn bench_pxl_decode(c: &mut Criterion) {
    let frame = build_frame();
    let bytes = encode_frame(&frame);
    c.bench_function("pxl_decode_frame", |b| {
        b.iter(|| {
            let mut cursor = Cursor::new(black_box(&bytes));
            let decoded = read_frame(&mut cursor).expect("read").expect("frame");
            black_box(decoded);
        });
    });
}

criterion::criterion_group!(pxl_benches, bench_pxl_encode, bench_pxl_decode);
criterion::criterion_main!(pxl_benches);
