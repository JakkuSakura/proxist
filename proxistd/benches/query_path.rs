use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use std::cell::RefCell;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use proxist_mem::{HotColumnStore, HotSymbolSummary, InMemoryHotColumnStore, MemConfig};
use proxistd::scheduler::{ExecutorConfig, ProxistScheduler, SqlExecutor, TableConfig};

fn micros_to_system_time(micros: i64) -> SystemTime {
    if micros >= 0 {
        UNIX_EPOCH + Duration::from_micros(micros as u64)
    } else {
        UNIX_EPOCH - Duration::from_micros((-micros) as u64)
    }
}

struct Fixture {
    rt: tokio::runtime::Runtime,
    scheduler: ProxistScheduler,
    sql: String,
}

fn build_fixture(cutoff_micros: i64) -> Fixture {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let hot_store = Arc::new(InMemoryHotColumnStore::new(MemConfig::default()));
    let symbols = vec!["SYM1".to_string(), "SYM2".to_string(), "SYM3".to_string()];
    let tenant = "alpha".to_string();
    let rows_per_symbol = 20_000i64;
    let start_micros = 1_000_000i64;
    let end_micros = start_micros + rows_per_symbol - 1;

    rt.block_on(async {
        for symbol in &symbols {
            for offset in 0..rows_per_symbol {
                let ts = micros_to_system_time(start_micros + offset);
                let payload = b"payload";
                hot_store
                    .append_row(&tenant, symbol, ts, payload)
                    .await
                    .expect("append row");
            }
        }
    });

    let scheduler = rt
        .block_on(ProxistScheduler::new(
            ExecutorConfig {
                sqlite_path: None,
                pg_url: None,
            },
            None,
            Some(hot_store),
        ))
        .expect("scheduler");

    scheduler.register_table(
        "ticks",
        TableConfig {
            order_col: "ts_micros".to_string(),
            payload_col: "payload_base64".to_string(),
            filter_cols: vec!["tenant".to_string(), "symbol".to_string()],
            seq_col: Some("seq".to_string()),
            columns: vec![
                "tenant".to_string(),
                "symbol".to_string(),
                "ts_micros".to_string(),
                "payload_base64".to_string(),
                "seq".to_string(),
            ],
        },
    );
    scheduler.set_persisted_cutoff(Some(micros_to_system_time(cutoff_micros)));

    let stats: Vec<_> = symbols
        .iter()
        .map(|symbol| HotSymbolSummary {
            tenant: tenant.clone(),
            symbol: symbol.clone(),
            rows: rows_per_symbol as u64,
            first_timestamp: Some(micros_to_system_time(start_micros)),
            last_timestamp: Some(micros_to_system_time(end_micros)),
        })
        .collect();
    rt.block_on(scheduler.update_hot_stats(&stats));

    let sql = format!(
        "SELECT symbol, ts_micros, payload_base64 \
         FROM ticks \
         WHERE tenant = '{tenant}' \
           AND symbol IN ('SYM1','SYM2','SYM3') \
           AND ts_micros BETWEEN {start} AND {end} \
         ORDER BY ts_micros ASC",
        tenant = tenant,
        start = start_micros,
        end = end_micros
    );

    Fixture { rt, scheduler, sql }
}

fn run_query(fixture: Fixture) {
    fixture
        .rt
        .block_on(fixture.scheduler.execute(&fixture.sql))
        .expect("query");
}

fn bench_query_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("scheduler_query");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    group.bench_function("hot_only", |b| {
        let fixture = RefCell::new(build_fixture(999_999));
        b.iter_batched(
            || (),
            |_| {
                let fixture = fixture.borrow();
                fixture
                    .rt
                    .block_on(fixture.scheduler.execute(&fixture.sql))
                    .expect("query");
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("mixed_skip_cold", |b| {
        let fixture = RefCell::new(build_fixture(1_100_000));
        b.iter_batched(
            || (),
            |_| {
                let fixture = fixture.borrow();
                fixture
                    .rt
                    .block_on(fixture.scheduler.execute(&fixture.sql))
                    .expect("query");
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_query_paths);
criterion_main!(benches);
