#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
struct DatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl DatasetConfig {
    fn new(rows: usize) -> Self {
        let total_bytes = calculate_large_table_bytes(rows);
        Self { rows, total_bytes }
    }
}

fn calculate_large_table_bytes(rows: usize) -> u64 {
    let mut total_bytes = 0u64;

    for i in 0..rows {
        total_bytes += 8 + 8;

        total_bytes += 8;

        let category_id = i % 20;
        total_bytes += format!("cat_{}", category_id).len() as u64;

        let region_id = i % 10;
        total_bytes += format!("region_{}", region_id).len() as u64;
    }

    total_bytes
}

fn setup_large_dataset(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql(
            "CREATE TABLE large_table (
                id INT64,
                category STRING,
                value INT64,
                score FLOAT64,
                region STRING
            )",
        )
        .unwrap();

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO large_table VALUES ({}, 'cat_{}', {}, {}, 'region_{}')",
                i,
                i % 20,
                i * 100,
                i as f64 * 1.5,
                i % 10
            ))
            .unwrap();
    }
}

fn bench_predicate_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("predicate_pushdown");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("with_early_filter", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT * FROM (
                        SELECT id, category, value
                        FROM large_table
                        WHERE value > 500000
                     ) sub
                     WHERE category = 'cat_5'",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_projection_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection_pushdown");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("select_specific_columns", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT id, value FROM (
                        SELECT *
                        FROM large_table
                        WHERE value > 100000
                     ) sub",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_join_optimization(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_optimization");
    configure_standard(&mut group);

    let small_rows = 100;
    let large_rows = 10000;
    let small_bytes: u64 = (0..small_rows)
        .map(|i| 8 + format!("name_{}", i).len() as u64)
        .sum();
    let large_bytes: u64 = (0..large_rows)
        .map(|i| 8 + format!("data_{}", i).len() as u64 + 8)
        .sum();
    group.throughput(Throughput::Bytes(small_bytes + large_bytes));

    group.bench_function("small_table_left", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor
            .execute_sql("CREATE TABLE small (id INT64, name STRING)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE large (id INT64, data STRING, value INT64)")
            .unwrap();

        for i in 0..small_rows {
            executor
                .execute_sql(&format!("INSERT INTO small VALUES ({}, 'name_{}')", i, i))
                .unwrap();
        }

        for i in 0..large_rows {
            executor
                .execute_sql(&format!(
                    "INSERT INTO large VALUES ({}, 'data_{}', {})",
                    i % 100,
                    i,
                    i * 10
                ))
                .unwrap();
        }

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT small.name, large.value
                     FROM small
                     INNER JOIN large ON small.id = large.id",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_aggregation_optimization(c: &mut Criterion) {
    let mut group = c.benchmark_group("aggregation_optimization");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("agg_after_filter", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT category, COUNT(*), SUM(value)
                     FROM large_table
                     WHERE value > 100000
                     GROUP BY category",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_filter_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_merge");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("multiple_conditions", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT * FROM large_table
                     WHERE value > 100000
                     AND category = 'cat_5'
                     AND region = 'region_3'",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_constant_folding(c: &mut Criterion) {
    let mut group = c.benchmark_group("constant_folding");
    configure_standard(&mut group);

    let config = DatasetConfig::new(5000);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("computed_constants", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT * FROM large_table
                     WHERE value > 100 * 1000 + 50000",
            ))
            .unwrap();
        });
    });

    group.finish();
}

fn bench_limit_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("limit_pushdown");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("early_limit", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT * FROM (
                        SELECT * FROM large_table ORDER BY value DESC
                     ) sub
                     LIMIT 100",
            ))
            .unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_predicate_pushdown,
    bench_projection_pushdown,
    bench_join_optimization,
    bench_aggregation_optimization,
    bench_filter_merge,
    bench_constant_folding,
    bench_limit_pushdown
);
criterion_main!(benches);
