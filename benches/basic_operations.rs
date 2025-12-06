#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
struct DatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl DatasetConfig {
    fn new(rows: usize) -> Self {
        let mut total_bytes = 0u64;
        for i in 0..rows {
            total_bytes += calculate_basic_row_bytes(i);
        }
        Self { rows, total_bytes }
    }
}

fn setup_empty_table(executor: &mut QueryExecutor, table_name: &str) {
    create_basic_table(executor, table_name);
}

fn bench_table_creation(c: &mut Criterion) {
    let mut group = c.benchmark_group("table_creation");
    configure_standard(&mut group);

    group.bench_function("create_simple_table", |b| {
        b.iter(|| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            black_box(executor.execute_sql("CREATE TABLE test_table (id INT64, name STRING)"))
                .unwrap();
        });
    });

    group.bench_function("create_complex_table", |b| {
        b.iter(|| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            black_box(executor.execute_sql(
                "CREATE TABLE test_table (
                        id INT64,
                        name STRING,
                        email STRING,
                        age INT64,
                        salary FLOAT64,
                        active BOOL,
                        created_at DATE,
                        metadata STRING
                    )",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_insert_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_operations");
    configure_standard(&mut group);

    group.bench_function("single_insert", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_empty_table(&mut executor, "test_table");
                executor
            },
            |mut executor| {
                black_box(
                    executor.execute_sql("INSERT INTO test_table VALUES (1, 'Alice', 100, 85.5)"),
                )
                .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    for &size in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("bulk_insert", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    setup_empty_table(&mut executor, "test_table");
                    executor
                },
                |mut executor| {
                    for i in 0..size {
                        black_box(executor.execute_sql(&format!(
                            "INSERT INTO test_table VALUES ({}, 'name_{}', {}, {})",
                            i,
                            i,
                            i * 10,
                            i as f64 * 1.5
                        )))
                        .unwrap();
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_select_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_all");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        let config = DatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("select_star", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT * FROM test_table")).unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_select_with_where(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_with_where");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        let config = DatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("equality_filter", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT * FROM test_table WHERE id = 500"))
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("range_filter", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT * FROM test_table WHERE value > 5000"))
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("complex_filter", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT * FROM test_table WHERE value > 1000 AND score < 5000.0",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_select_with_order_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_with_order_by");
    configure_standard(&mut group);

    for &size in &[100, 1000, 5000] {
        let config = DatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("order_by_int_asc", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT * FROM test_table ORDER BY id ASC"))
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("order_by_int_desc", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT * FROM test_table ORDER BY id DESC"))
                        .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("order_by_float", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT * FROM test_table ORDER BY score DESC"))
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_select_with_limit(c: &mut Criterion) {
    let mut group = c.benchmark_group("select_with_limit");
    configure_standard(&mut group);

    let data_size = 10000;
    let config = DatasetConfig::new(data_size);

    for &limit in &[10, 100, 1000] {
        group.throughput(Throughput::Bytes(config.total_bytes));
        group.bench_with_input(
            BenchmarkId::new("limit_only", limit),
            &limit,
            |b, &limit| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", data_size);

                b.iter(|| {
                    black_box(
                        executor.execute_sql(&format!("SELECT * FROM test_table LIMIT {}", limit)),
                    )
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        let config = DatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("select_single_column", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT id FROM test_table")).unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("select_two_columns", config.rows),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_basic_table(&mut executor, "test_table", config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT id, name FROM test_table")).unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_table_creation,
    bench_insert_operations,
    bench_select_all,
    bench_select_with_where,
    bench_select_with_order_by,
    bench_select_with_limit,
    bench_projection
);
criterion_main!(benches);
