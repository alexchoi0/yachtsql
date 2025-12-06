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
    fn new(rows: usize, bytes_per_row: u64) -> Self {
        Self {
            rows,
            total_bytes: rows as u64 * bytes_per_row,
        }
    }
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

fn setup_join_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE small_table (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE large_table (id INT64, data STRING, value INT64)")
        .unwrap();

    for i in 0..100 {
        executor
            .execute_sql(&format!(
                "INSERT INTO small_table VALUES ({}, 'name_{}')",
                i, i
            ))
            .unwrap();
    }

    for i in 0..10000 {
        executor
            .execute_sql(&format!(
                "INSERT INTO large_table VALUES ({}, 'data_{}', {})",
                i % 100,
                i,
                i * 10
            ))
            .unwrap();
    }
}

fn bench_predicate_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/predicate_pushdown");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000, 100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT * FROM (
                    SELECT id, category, value
                    FROM large_table
                    WHERE value > 500000
                 ) sub
                 WHERE category = 'cat_5'";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_projection_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/projection_pushdown");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000, 100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT id, value FROM (
                    SELECT *
                    FROM large_table
                    WHERE value > 100000
                 ) sub";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_join_optimization(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/join_optimization");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000, 80);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT small_table.name, large_table.value
                 FROM small_table
                 INNER JOIN large_table ON small_table.id = large_table.id";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_join_tables(&mut executor);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_join_tables(&mut executor);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_filter_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/filter_merge");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000, 100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT * FROM large_table
                 WHERE value > 100000
                 AND category = 'cat_5'
                 AND region = 'region_3'";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_constant_folding(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/constant_folding");
    configure_standard(&mut group);

    let config = DatasetConfig::new(5000, 100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT * FROM large_table
                 WHERE value > 100 * 1000 + 50000";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_limit_pushdown(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/limit_pushdown");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000, 100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT * FROM (
                    SELECT * FROM large_table ORDER BY value DESC
                 ) sub
                 LIMIT 100";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_aggregation_with_filter(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/aggregation_with_filter");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000, 100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT category, COUNT(*), SUM(value)
                 FROM large_table
                 WHERE value > 100000
                 GROUP BY category";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_complex_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("optimizer_comparison/complex_query");
    configure_standard(&mut group);

    let config = DatasetConfig::new(10000, 100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    let query = "SELECT id, category, value FROM (
                    SELECT *
                    FROM large_table
                    WHERE value > 50 * 1000 AND region = 'region_3'
                 ) sub
                 WHERE category IN ('cat_1', 'cat_5', 'cat_10')
                 ORDER BY value DESC
                 LIMIT 50";

    group.bench_function("with_optimizer", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.bench_function("without_optimizer", |b| {
        let mut executor = QueryExecutor::new_without_optimizer();
        setup_large_dataset(&mut executor, config.rows);

        b.iter(|| {
            black_box(executor.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_predicate_pushdown,
    bench_projection_pushdown,
    bench_join_optimization,
    bench_filter_merge,
    bench_constant_folding,
    bench_limit_pushdown,
    bench_aggregation_with_filter,
    bench_complex_query
);
criterion_main!(benches);
