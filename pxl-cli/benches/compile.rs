use criterion::{black_box, Criterion};
use pxl_cli::compiler::{compile_to_frame, InputKind};

fn bench_compile_sql(c: &mut Criterion) {
    let sql = "SELECT value FROM ticks WHERE symbol = 'AAPL'";
    c.bench_function("compile_sql_select", |b| {
        b.iter(|| {
            let bytes = compile_to_frame(black_box(sql), InputKind::Sql, 1).expect("compile");
            black_box(bytes);
        });
    });
}

fn bench_compile_prql(c: &mut Criterion) {
    let prql = r#"
from ticks
| filter symbol == "AAPL"
| select {value}
"#;
    c.bench_function("compile_prql_select", |b| {
        b.iter(|| {
            let bytes = compile_to_frame(black_box(prql), InputKind::Prql, 1).expect("compile");
            black_box(bytes);
        });
    });
}

criterion::criterion_group!(compile_benches, bench_compile_sql, bench_compile_prql);
criterion::criterion_main!(compile_benches);
