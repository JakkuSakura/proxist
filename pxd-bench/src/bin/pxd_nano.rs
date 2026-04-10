use std::alloc::{alloc, dealloc, Layout};
use std::env;
use std::hint::black_box;
use std::slice;
use std::time::Instant;

use pxd::memstore::MemStore;
use pxd::types::Value;
use pxd_bench::{lcg_index, LCG_SEED};

#[derive(Debug, Clone)]
struct Config {
    rows: usize,
    random_reads: usize,
    cache_rows: usize,
    batch_size: usize,
}

impl Config {
    fn from_env() -> Self {
        let mut rows = 10_000_000usize;
        let mut random_reads: Option<usize> = None;
        let mut cache_rows: Option<usize> = None;
        let mut batch_size = 100_000usize;

        let mut args = env::args().skip(1).peekable();
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--rows" => {
                    if let Some(value) = args.next() {
                        rows = value.parse().unwrap_or(rows);
                    }
                }
                "--random-reads" => {
                    if let Some(value) = args.next() {
                        random_reads = value.parse().ok();
                    }
                }
                "--cache-rows" => {
                    if let Some(value) = args.next() {
                        cache_rows = value.parse().ok();
                    }
                }
                "--batch" => {
                    if let Some(value) = args.next() {
                        batch_size = value.parse().unwrap_or(batch_size);
                    }
                }
                _ => {}
            }
        }

        let random_reads = random_reads.unwrap_or_else(|| rows / 10).max(1);
        let cache_rows = cache_rows.unwrap_or_else(|| rows.min(1_000_000));

        Self {
            rows,
            random_reads,
            cache_rows,
            batch_size: batch_size.max(1),
        }
    }
}

fn emit(test: &str, rows: usize, ops: usize, bytes: usize, elapsed_ms: f64) {
    let secs = elapsed_ms / 1000.0;
    let ops_per_sec = if secs > 0.0 { ops as f64 / secs } else { 0.0 };
    let mb_per_sec = if secs > 0.0 {
        bytes as f64 / (1024.0 * 1024.0) / secs
    } else {
        0.0
    };
    println!(
        "{test},{rows},{ops},{bytes},{elapsed_ms:.3},{ops_per_sec:.3},{mb_per_sec:.3}",
    );
}

struct RawF64Buf {
    ptr: *mut f64,
    len: usize,
    layout: Layout,
}

impl RawF64Buf {
    fn new(len: usize) -> Self {
        let layout = Layout::array::<f64>(len.max(1)).expect("layout");
        let ptr = unsafe { alloc(layout) as *mut f64 };
        if ptr.is_null() {
            panic!("alloc failed");
        }
        Self { ptr, len, layout }
    }

    fn as_mut_slice(&mut self) -> &mut [f64] {
        unsafe { slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl Drop for RawF64Buf {
    fn drop(&mut self) {
        unsafe {
            dealloc(self.ptr as *mut u8, self.layout);
        }
    }
}

fn main() {
    let config = Config::from_env();
    let columns = vec!["symbol".to_string(), "ts".to_string(), "value".to_string()];
    let row_bytes = 8usize + 8usize + 8usize;

    println!("test,rows,ops,bytes,elapsed_ms,ops_per_sec,mb_per_sec");

    let mut mem = MemStore::new();
    let mut inserted = 0usize;
    let start = Instant::now();
    while inserted < config.rows {
        let batch_count = (config.rows - inserted).min(config.batch_size);
        let mut batch = Vec::with_capacity(batch_count);
        for offset in 0..batch_count {
            let idx = inserted + offset;
            let symbol = format!("S{}", idx);
            batch.push(vec![
                Value::String(symbol),
                Value::I64(idx as i64),
                Value::F64((idx % 10_000) as f64),
            ]);
        }
        mem.insert("ticks", &columns, &batch).expect("insert");
        inserted += batch_count;
    }
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    emit("write", config.rows, config.rows, config.rows * row_bytes, elapsed);

    let values = mem
        .table_column("ticks", "value")
        .expect("value column");

    let start = Instant::now();
    let mut sum = 0.0f64;
    for value in values {
        if let Value::F64(v) = value {
            sum += *v;
        }
    }
    black_box(sum);
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    emit("reread", config.rows, config.rows, config.rows * row_bytes, elapsed);

    let start = Instant::now();
    let mut seed = LCG_SEED;
    let mut rand_sum = 0.0f64;
    let row_len = values.len();
    for _ in 0..config.random_reads {
        let idx = lcg_index(&mut seed, row_len);
        if let Value::F64(v) = values[idx] {
            rand_sum += v;
        }
    }
    black_box(rand_sum);
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    emit(
        "randomread",
        config.rows,
        config.random_reads,
        config.random_reads * row_bytes,
        elapsed,
    );

    let start = Instant::now();
    let mut buf = RawF64Buf::new(config.rows);
    let vec = buf.as_mut_slice();
    for (idx, slot) in vec.iter_mut().enumerate() {
        *slot = idx as f64 * 0.1;
    }
    let mut acc = 0.0f64;
    for v in vec.iter_mut() {
        *v = *v * 1.0001 + 0.1;
    }
    for v in vec.iter() {
        acc += *v;
    }
    black_box(acc);
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    let cpu_bytes = config.rows * 16;
    emit("cpu", config.rows, config.rows, cpu_bytes, elapsed);

    let cache_len = config.cache_rows.max(1);
    let start = Instant::now();
    let mut cache_buf = RawF64Buf::new(cache_len);
    let cache_vec = cache_buf.as_mut_slice();
    for (idx, slot) in cache_vec.iter_mut().enumerate() {
        *slot = idx as f64 * 0.2;
    }
    let mut cache_acc = 0.0f64;
    for _ in 0..5 {
        for v in cache_vec.iter_mut() {
            *v = *v * 1.0001 + 0.2;
        }
        for v in cache_vec.iter() {
            cache_acc += *v;
        }
    }
    black_box(cache_acc);
    let elapsed = start.elapsed().as_secs_f64() * 1000.0;
    let cache_ops = cache_len * 5;
    let cache_bytes = cache_ops * 16;
    emit("cpucache", cache_len, cache_ops, cache_bytes, elapsed);
}
