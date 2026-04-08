use criterion::{black_box, Criterion};
use pxd::memstore::MemStore;
use pxd::types::{ColumnSpec, ColumnType, Schema, Value};
use pxd::wal::Wal;
use std::path::PathBuf;

fn bench_wal_append_insert(c: &mut Criterion) {
    let mut mem = MemStore::new();
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

    let mut path = std::env::temp_dir();
    path.push(format!("pxd_bench_wal_{}.wal", std::process::id()));
    let mut wal = Wal::open(&path, &mut mem, false).expect("wal open");
    wal.append_create("ticks", &schema).expect("create");
    let columns = vec!["symbol".to_string(), "ts".to_string(), "price".to_string()];
    let row = vec![Value::String("AAPL".to_string()), Value::I64(1), Value::F64(10.0)];

    c.bench_function("wal_append_insert", |b| {
        b.iter(|| {
            let lsn = wal.append_insert("ticks", &columns, &[row.clone()]).expect("insert");
            black_box(lsn);
        });
    });

    let _ = std::fs::remove_file(PathBuf::from(path));
}

criterion::criterion_group!(wal_benches, bench_wal_append_insert);
criterion::criterion_main!(wal_benches);
