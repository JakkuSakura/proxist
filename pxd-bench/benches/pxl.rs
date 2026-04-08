use criterion::{black_box, Criterion};
use pxd::pxl::{encode_frame, read_frame, encode_query_payload, Frame, Op};
use pxd::query::{QueryPlan, SelectExpr, SelectItem};
use pxd::types::Value;
use std::io::Cursor;

fn build_frame() -> Frame {
    let plan = QueryPlan {
        table: "ticks".to_string(),
        join: None,
        filter: None,
        group_by: Vec::new(),
        select: vec![
            SelectItem {
                expr: SelectExpr::Column("symbol".to_string()),
                alias: None,
            },
            SelectItem {
                expr: SelectExpr::Literal(Value::I64(1)),
                alias: None,
            },
        ],
    };
    let payload = encode_query_payload(&plan).expect("payload");
    Frame {
        flags: 0,
        req_id: 7,
        op: Op::Query,
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
