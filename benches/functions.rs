#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
struct StringDatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl StringDatasetConfig {
    fn new(rows: usize) -> Self {
        let total_bytes = Self::calculate_bytes(rows);
        Self { rows, total_bytes }
    }

    fn calculate_bytes(rows: usize) -> u64 {
        let mut total_bytes = 0u64;

        for i in 0..rows {
            total_bytes += 8;
            total_bytes += format!("  Some Text With Spaces {}  ", i).len() as u64;
            total_bytes += format!("User Name {}", i).len() as u64;
        }

        total_bytes
    }
}

#[derive(Debug, Clone, Copy)]
struct NumericDatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl NumericDatasetConfig {
    fn new(rows: usize) -> Self {
        let total_bytes = Self::calculate_bytes(rows);
        Self { rows, total_bytes }
    }

    fn calculate_bytes(rows: usize) -> u64 {
        let mut total_bytes = 0u64;

        for _ in 0..rows {
            total_bytes += 8;
            total_bytes += 8;
            total_bytes += 8;
        }

        total_bytes
    }
}

fn setup_string_data(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE strings (id INT64, text STRING, name STRING)")
        .unwrap();

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO strings VALUES ({}, '  Some Text With Spaces {}  ', 'User Name {}')",
                i, i, i
            ))
            .unwrap();
    }
}

fn setup_numeric_data(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE numbers (id INT64, int_val INT64, float_val FLOAT64)")
        .unwrap();

    for i in 0..rows {
        let sign = if i % 2 == 0 { 1 } else { -1 };
        executor
            .execute_sql(&format!(
                "INSERT INTO numbers VALUES ({}, {}, {})",
                i,
                sign * (i as i64) * 100,
                sign as f64 * (i as f64 + 0.7654)
            ))
            .unwrap();
    }
}

fn bench_string_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_functions");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        let config = StringDatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(BenchmarkId::new("upper", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT UPPER(text) FROM strings")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("lower", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT LOWER(text) FROM strings")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("trim", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT TRIM(text) FROM strings")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("ltrim", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT LTRIM(text) FROM strings")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("rtrim", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT RTRIM(text) FROM strings")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("concat", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT CONCAT(text, ' - ', name) FROM strings"))
                    .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("replace", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(
                    executor.execute_sql("SELECT REPLACE(text, 'Text', 'Value') FROM strings"),
                )
                .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("length", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT LENGTH(text) FROM strings")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("substr", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_string_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT SUBSTR(text, 1, 10) FROM strings")).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_math_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("math_functions");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        let config = NumericDatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(BenchmarkId::new("abs", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_numeric_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT ABS(int_val) FROM numbers")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("round", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_numeric_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT ROUND(float_val) FROM numbers")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("ceil", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_numeric_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT CEIL(float_val) FROM numbers")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("floor", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_numeric_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT FLOOR(float_val) FROM numbers")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("sign", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_numeric_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT SIGN(int_val) FROM numbers")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("mod", size), &config, |b, &config| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_numeric_data(&mut executor, config.rows);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT MOD(int_val, 10) FROM numbers")).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_regex_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("regex_functions");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        let config = StringDatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("regexp_contains", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_string_data(&mut executor, config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT id FROM strings WHERE REGEXP_CONTAINS(text, 'Text.*[0-9]+')",
                    ))
                    .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regexp_replace", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_string_data(&mut executor, config.rows);

                b.iter(|| {
                    black_box(
                        executor.execute_sql(
                            "SELECT REGEXP_REPLACE(text, '[0-9]+', 'NUM') FROM strings",
                        ),
                    )
                    .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("regexp_extract", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_string_data(&mut executor, config.rows);

                b.iter(|| {
                    black_box(
                        executor
                            .execute_sql("SELECT REGEXP_EXTRACT(text, '([0-9]+)') FROM strings"),
                    )
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_null_handling_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("null_handling");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("coalesce", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql(
                    "CREATE TABLE nullable (id INT64, val1 STRING, val2 STRING, val3 STRING)",
                )
                .unwrap();

            for i in 0..size {
                let val1 = if i % 3 == 0 {
                    "NULL"
                } else {
                    &format!("'val1_{}'", i)
                };
                let val2 = if i % 2 == 0 {
                    "NULL"
                } else {
                    &format!("'val2_{}'", i)
                };
                executor
                    .execute_sql(&format!(
                        "INSERT INTO nullable VALUES ({}, {}, {}, 'val3_{}')",
                        i, val1, val2, i
                    ))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql("SELECT COALESCE(val1, val2, val3) FROM nullable"))
                    .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("ifnull", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
                .unwrap();

            for i in 0..size {
                let val = if i % 2 == 0 {
                    "NULL"
                } else {
                    &format!("'value_{}'", i)
                };
                executor
                    .execute_sql(&format!("INSERT INTO nullable VALUES ({}, {})", i, val))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql("SELECT IFNULL(value, 'default') FROM nullable"))
                    .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("nullif", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql("CREATE TABLE test (id INT64, val1 STRING, val2 STRING)")
                .unwrap();

            for i in 0..size {
                let val1 = format!("'value_{}'", i % 10);
                let val2 = format!("'value_{}'", i % 20);
                executor
                    .execute_sql(&format!(
                        "INSERT INTO test VALUES ({}, {}, {})",
                        i, val1, val2
                    ))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql("SELECT NULLIF(val1, val2) FROM test")).unwrap();
            });
        });
    }

    group.finish();
}

fn bench_date_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("date_functions");
    configure_standard(&mut group);

    for &size in &[100, 1000, 5000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("current_date", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql("CREATE TABLE dates (id INT64)")
                .unwrap();

            for i in 0..size {
                executor
                    .execute_sql(&format!("INSERT INTO dates VALUES ({})", i))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql("SELECT CURRENT_DATE() FROM dates")).unwrap();
            });
        });

        group.bench_with_input(
            BenchmarkId::new("current_timestamp", size),
            &size,
            |b, &size| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE dates (id INT64)")
                    .unwrap();

                for i in 0..size {
                    executor
                        .execute_sql(&format!("INSERT INTO dates VALUES ({})", i))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT CURRENT_TIMESTAMP() FROM dates"))
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_conditional_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("conditional_functions");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        let config = NumericDatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("case_when", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_numeric_data(&mut executor, config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT
                            CASE
                                WHEN int_val < 0 THEN 'negative'
                                WHEN int_val = 0 THEN 'zero'
                                ELSE 'positive'
                            END as category
                         FROM numbers",
                    ))
                    .unwrap();
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("greatest", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql(
                    "CREATE TABLE multi_val (id INT64, val1 INT64, val2 INT64, val3 INT64)",
                )
                .unwrap();

            for i in 0..size {
                executor
                    .execute_sql(&format!(
                        "INSERT INTO multi_val VALUES ({}, {}, {}, {})",
                        i,
                        i * 10,
                        i * 20,
                        i * 15
                    ))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql("SELECT GREATEST(val1, val2, val3) FROM multi_val"))
                    .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("least", size), &size, |b, &size| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            executor
                .execute_sql(
                    "CREATE TABLE multi_val (id INT64, val1 INT64, val2 INT64, val3 INT64)",
                )
                .unwrap();

            for i in 0..size {
                executor
                    .execute_sql(&format!(
                        "INSERT INTO multi_val VALUES ({}, {}, {}, {})",
                        i,
                        i * 10,
                        i * 20,
                        i * 15
                    ))
                    .unwrap();
            }

            b.iter(|| {
                black_box(executor.execute_sql("SELECT LEAST(val1, val2, val3) FROM multi_val"))
                    .unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_string_functions,
    bench_math_functions,
    bench_regex_functions,
    bench_null_handling_functions,
    bench_date_functions,
    bench_conditional_functions
);
criterion_main!(benches);
