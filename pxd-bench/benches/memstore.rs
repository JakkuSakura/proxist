use criterion::{black_box, BatchSize, Criterion};
use pxd::expr::BinaryOp;
use pxd::memstore::MemStore;
use pxd::pxl::{ColumnInstr, ColumnProjectExpr, ColumnProjectItem, ColumnQuery};
use pxd::types::Value;

fn build_rows(count: usize) -> Vec<Vec<Value>> {
    let mut rows = Vec::with_capacity(count);
    for idx in 0..count {
        rows.push(vec![
            Value::String(format!("SYM{}", idx % 50)),
            Value::I64(idx as i64),
            Value::F64((idx % 100) as f64),
        ]);
    }
    rows
}

fn bench_memstore_insert(c: &mut Criterion) {
    let columns = vec!["symbol".to_string(), "ts".to_string(), "price".to_string()];
    let rows = build_rows(1_000);
    c.bench_function("memstore_insert_1k", |b| {
        b.iter_batched(
            MemStore::new,
            |mut mem| {
                mem.insert("ticks", &columns, &rows).expect("insert");
                black_box(mem);
            },
            BatchSize::SmallInput,
        );
    });
}

fn bench_memstore_filter_query(c: &mut Criterion) {
    let columns = vec!["symbol".to_string(), "ts".to_string(), "price".to_string()];
    let rows = build_rows(10_000);
    let mut mem = MemStore::new();
    mem.insert("ticks", &columns, &rows).expect("insert");
    let plan = ColumnQuery {
        table: "ticks".to_string(),
        columns: vec!["symbol".to_string(), "ts".to_string(), "price".to_string()],
        filter: vec![
            ColumnInstr::PushCol(2),
            ColumnInstr::PushLit(Value::F64(50.0)),
            ColumnInstr::Cmp(BinaryOp::Gt),
        ],
        project: vec![ColumnProjectItem {
            name: "price".to_string(),
            expr: ColumnProjectExpr::Column(2),
        }],
    };
    c.bench_function("memstore_filter_query", |b| {
        b.iter(|| {
            let result = mem.query_col(black_box(&plan)).expect("query");
            black_box(result);
        });
    });
}

criterion::criterion_group!(
    memstore_benches,
    bench_memstore_insert,
    bench_memstore_filter_query
);
criterion::criterion_main!(memstore_benches);
