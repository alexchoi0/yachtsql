#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
struct NumericDatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl NumericDatasetConfig {
    fn new(rows: usize, _is_int: bool) -> Self {
        let total_bytes = rows as u64 * (8 + 8 + 8);
        Self { rows, total_bytes }
    }
}

#[derive(Debug, Clone, Copy)]
struct StringDatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl StringDatasetConfig {
    fn new(rows: usize, is_long: bool) -> Self {
        let mut total_bytes = 0u64;

        for i in 0..rows {
            total_bytes += 8;
            if is_long {
                total_bytes += format!("This is a much longer string for testing performance with string operations and it contains more data to process - entry {}", i).len() as u64;
            } else {
                total_bytes += format!("short_{}", i).len() as u64;
            }
        }

        Self { rows, total_bytes }
    }
}

fn bench_numeric_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("numeric_types");
    configure_standard(&mut group);

    for &size in &[1000, 10000, 50000] {
        let config = NumericDatasetConfig::new(size, true);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("int64_operations", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE ints (id INT64, val1 INT64, val2 INT64)")
                    .unwrap();

                for i in 0..config.rows {
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO ints VALUES ({}, {}, {})",
                            i,
                            i * 10,
                            i * 20
                        ))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(
                        executor.execute_sql(
                            "SELECT id, val1 + val2, val1 * val2, val1 - val2 FROM ints",
                        ),
                    )
                    .unwrap();
                });
            },
        );

        let config_float = NumericDatasetConfig::new(size, false);
        group.throughput(Throughput::Bytes(config_float.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("float64_operations", size),
            &config_float,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE floats (id INT64, val1 FLOAT64, val2 FLOAT64)")
                    .unwrap();

                for i in 0..config.rows {
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO floats VALUES ({}, {}, {})",
                            i,
                            i as f64 * 10.5,
                            i as f64 * 20.3
                        ))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT id, val1 + val2, val1 * val2, val1 - val2 FROM floats",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_string_lengths(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_lengths");
    configure_standard(&mut group);

    for &size in &[100, 1000, 5000] {
        let config = StringDatasetConfig::new(size, false);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("short_strings", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE strings (id INT64, text STRING)")
                    .unwrap();

                for i in 0..config.rows {
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO strings VALUES ({}, 'short_{}')",
                            i, i
                        ))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(
                        executor.execute_sql("SELECT id, UPPER(text), LENGTH(text) FROM strings"),
                    )
                    .unwrap();
                });
            },
        );

        let config_long = StringDatasetConfig::new(size, true);
        group.throughput(Throughput::Bytes(config_long.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("long_strings", size),
            &config_long,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE strings (id INT64, text STRING)")
                    .unwrap();

                for i in 0..config.rows {
                    let long_str = format!("This is a much longer string for testing performance with string operations and it contains more data to process - entry {}", i);
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO strings VALUES ({}, '{}')",
                            i, long_str
                        ))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(
                        executor.execute_sql("SELECT id, UPPER(text), LENGTH(text) FROM strings")
                    )
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_null_overhead(c: &mut Criterion) {
    let mut group = c.benchmark_group("null_overhead");
    configure_standard(&mut group);

    for &size in &[1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("no_nulls", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql("CREATE TABLE data (id INT64, value INT64, name STRING)")
                .unwrap();

            for i in 0..size {
                executor
                    .execute_sql(&format!(
                        "INSERT INTO data VALUES ({}, {}, 'name_{}')",
                        i,
                        i * 10,
                        i
                    ))
                    .unwrap();
            }

            b.iter(|| {
                black_box(
                    executor.execute_sql("SELECT id, value, name FROM data WHERE value > 5000"),
                )
                .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("with_nulls", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql("CREATE TABLE data (id INT64, value INT64, name STRING)")
                .unwrap();

            for i in 0..size {
                let value = if i % 3 == 0 {
                    "NULL".to_string()
                } else {
                    (i * 10).to_string()
                };
                let name = if i % 5 == 0 {
                    "NULL".to_string()
                } else {
                    format!("'name_{}'", i)
                };
                executor
                    .execute_sql(&format!(
                        "INSERT INTO data VALUES ({}, {}, {})",
                        i, value, name
                    ))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT id, value, name FROM data WHERE value > 5000 OR value IS NULL",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_mixed_types(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_types");
    configure_standard(&mut group);

    for &size in &[500, 1000, 5000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("all_types", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql(
                    "CREATE TABLE mixed (
                        id INT64,
                        int_val INT64,
                        float_val FLOAT64,
                        text STRING,
                        flag BOOL,
                        created DATE
                    )",
                )
                .unwrap();

            for i in 0..size {
                executor
                    .execute_sql(&format!(
                        "INSERT INTO mixed VALUES ({}, {}, {}, 'text_{}', {}, DATE '2024-01-{}')",
                        i,
                        i * 100,
                        i as f64 * std::f64::consts::PI,
                        i,
                        if i % 2 == 0 { "TRUE" } else { "FALSE" },
                        1 + (i % 28)
                    ))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT id, int_val, float_val, UPPER(text), flag
                         FROM mixed
                         WHERE flag = TRUE AND float_val > 100.0",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_date_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("date_operations");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("date_comparison", size),
            &size,
            |b, &size| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE events (id INT64, event_date DATE)")
                    .unwrap();

                for i in 0..size {
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO events VALUES ({}, DATE '2024-01-{}')",
                            i,
                            1 + (i % 28)
                        ))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(
                        executor.execute_sql(
                            "SELECT * FROM events WHERE event_date >= DATE '2024-01-15'",
                        ),
                    )
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_numeric_types,
    bench_string_lengths,
    bench_null_overhead,
    bench_mixed_types,
    bench_date_operations
);
criterion_main!(benches);
