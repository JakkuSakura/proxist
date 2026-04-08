use criterion::{black_box, BatchSize, Criterion};
use pxd::expr::{BinaryOp, Expr};
use pxd::memstore::MemStore;
use pxd::query::{AggFunc, AggregateExpr, QueryPlan, SelectExpr, SelectItem, WindowBound, WindowExpr, WindowFrameUnit, WindowSpec};
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
    let filter = Expr::Binary {
        op: BinaryOp::Gt,
        left: Box::new(Expr::Column("price".to_string())),
        right: Box::new(Expr::Literal(Value::F64(50.0))),
    };
    let plan = QueryPlan {
        table: "ticks".to_string(),
        join: None,
        filter: Some(filter),
        group_by: Vec::new(),
        select: vec![SelectItem {
            expr: SelectExpr::Column("price".to_string()),
            alias: None,
        }],
    };
    c.bench_function("memstore_filter_query", |b| {
        b.iter(|| {
            let result = mem.query(black_box(&plan)).expect("query");
            black_box(result);
        });
    });
}

fn bench_memstore_group_by(c: &mut Criterion) {
    let columns = vec!["symbol".to_string(), "ts".to_string(), "price".to_string()];
    let rows = build_rows(10_000);
    let mut mem = MemStore::new();
    mem.insert("ticks", &columns, &rows).expect("insert");
    let plan = QueryPlan {
        table: "ticks".to_string(),
        join: None,
        filter: None,
        group_by: vec!["symbol".to_string()],
        select: vec![
            SelectItem {
                expr: SelectExpr::Column("symbol".to_string()),
                alias: None,
            },
            SelectItem {
                expr: SelectExpr::Aggregate(AggregateExpr {
                    func: AggFunc::Avg,
                    arg: Expr::Column("price".to_string()),
                }),
                alias: None,
            },
        ],
    };
    c.bench_function("memstore_group_by_avg", |b| {
        b.iter(|| {
            let result = mem.query(black_box(&plan)).expect("query");
            black_box(result);
        });
    });
}

fn bench_memstore_window(c: &mut Criterion) {
    let columns = vec!["symbol".to_string(), "ts".to_string(), "price".to_string()];
    let rows = build_rows(10_000);
    let mut mem = MemStore::new();
    mem.insert("ticks", &columns, &rows).expect("insert");
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
                expr: SelectExpr::Window(WindowExpr {
                    func: AggFunc::Avg,
                    arg: Expr::Column("price".to_string()),
                    spec: WindowSpec {
                        partition_by: vec!["symbol".to_string()],
                        order_by: "ts".to_string(),
                        unit: WindowFrameUnit::Rows,
                        start: WindowBound::Preceding,
                        start_value: Some(10),
                    },
                }),
                alias: None,
            },
        ],
    };
    c.bench_function("memstore_window_avg", |b| {
        b.iter(|| {
            let result = mem.query(black_box(&plan)).expect("query");
            black_box(result);
        });
    });
}

criterion::criterion_group!(
    memstore_benches,
    bench_memstore_insert,
    bench_memstore_filter_query,
    bench_memstore_group_by,
    bench_memstore_window
);
criterion::criterion_main!(memstore_benches);
