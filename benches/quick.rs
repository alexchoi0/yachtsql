#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
struct QuickDatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl QuickDatasetConfig {
    fn new(rows: usize) -> Self {
        let total_bytes = Self::calculate_bytes(rows);
        Self { rows, total_bytes }
    }

    fn calculate_bytes(rows: usize) -> u64 {
        let mut total_bytes = 0u64;

        for i in 0..rows {
            total_bytes += 8;
            total_bytes += format!("name_{}", i).len() as u64;
            total_bytes += 8;
            total_bytes += 8;
        }

        total_bytes
    }
}

fn setup_test_data(executor: &mut QueryExecutor, table_name: &str, rows: usize) {
    executor
        .execute_sql(&format!(
            "CREATE TABLE {} (id INT64, name STRING, value INT64, score FLOAT64)",
            table_name
        ))
        .unwrap();

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO {} VALUES ({}, 'name_{}', {}, {})",
                table_name,
                i,
                i,
                i * 10,
                i as f64 * 1.5
            ))
            .unwrap();
    }
}

fn quick_basic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick_basic");
    configure_quick(&mut group);

    group.bench_function("create_table", |b| {
        b.iter(|| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            black_box(executor.execute_sql("CREATE TABLE test (id INT64, name STRING)")).unwrap();
        });
    });

    group.bench_function("insert_10_rows", |b| {
        b.iter(|| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_test_data(&mut executor, "test", 0);
            for i in 0..10 {
                black_box(executor.execute_sql(&format!(
                    "INSERT INTO test VALUES ({}, 'name_{}', {}, {})",
                    i,
                    i,
                    i * 10,
                    i as f64 * 1.5
                )))
                .unwrap();
            }
        });
    });

    let config = QuickDatasetConfig::new(100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("select_100_rows", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_test_data(&mut executor, "test", config.rows);

        b.iter(|| {
            black_box(executor.execute_sql("SELECT * FROM test")).unwrap();
        });
    });

    group.bench_function("where_filter_100_rows", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_test_data(&mut executor, "test", config.rows);

        b.iter(|| {
            black_box(executor.execute_sql("SELECT * FROM test WHERE value > 500")).unwrap();
        });
    });

    group.finish();
}

fn quick_joins(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick_joins");
    configure_quick(&mut group);

    let rows = 50;
    let mut total_bytes = 0u64;
    for i in 0..rows {
        total_bytes += 8;
        total_bytes += format!("left_{}", i).len() as u64;
        total_bytes += 8;
        total_bytes += format!("right_{}", i).len() as u64;
    }
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("inner_join_50x50", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor
            .execute_sql("CREATE TABLE left_t (id INT64, value STRING)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE right_t (id INT64, data STRING)")
            .unwrap();

        for i in 0..50 {
            executor
                .execute_sql(&format!("INSERT INTO left_t VALUES ({}, 'left_{}')", i, i))
                .unwrap();
            executor
                .execute_sql(&format!(
                    "INSERT INTO right_t VALUES ({}, 'right_{}')",
                    i, i
                ))
                .unwrap();
        }

        b.iter(|| {
            black_box(
                executor.execute_sql(
                    "SELECT left_t.id, left_t.value, right_t.data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id",
                ),
            )
            .unwrap();
        });
    });

    let rows_100 = 100;
    let mut total_bytes_100 = 0u64;
    for i in 0..rows_100 {
        total_bytes_100 += 8;
        total_bytes_100 += format!("left_{}", i).len() as u64;
        total_bytes_100 += 8;
        total_bytes_100 += format!("right_{}", i).len() as u64;
    }
    group.throughput(Throughput::Bytes(total_bytes_100));

    group.bench_function("inner_join_100x100", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor
            .execute_sql("CREATE TABLE left_t (id INT64, value STRING)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE right_t (id INT64, data STRING)")
            .unwrap();

        for i in 0..100 {
            executor
                .execute_sql(&format!("INSERT INTO left_t VALUES ({}, 'left_{}')", i, i))
                .unwrap();
            executor
                .execute_sql(&format!(
                    "INSERT INTO right_t VALUES ({}, 'right_{}')",
                    i, i
                ))
                .unwrap();
        }

        b.iter(|| {
            black_box(
                executor.execute_sql(
                    "SELECT left_t.id, left_t.value, right_t.data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id",
                ),
            )
            .unwrap();
        });
    });

    let rows_200 = 200;
    let mut total_bytes_200 = 0u64;
    for i in 0..rows_200 {
        total_bytes_200 += 8;
        total_bytes_200 += format!("left_{}", i).len() as u64;
        total_bytes_200 += 8;
        total_bytes_200 += format!("right_{}", i).len() as u64;
    }
    group.throughput(Throughput::Bytes(total_bytes_200));

    group.bench_function("inner_join_200x200", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor
            .execute_sql("CREATE TABLE left_t (id INT64, value STRING)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE right_t (id INT64, data STRING)")
            .unwrap();

        for i in 0..200 {
            executor
                .execute_sql(&format!("INSERT INTO left_t VALUES ({}, 'left_{}')", i, i))
                .unwrap();
            executor
                .execute_sql(&format!(
                    "INSERT INTO right_t VALUES ({}, 'right_{}')",
                    i, i
                ))
                .unwrap();
        }

        b.iter(|| {
            black_box(
                executor.execute_sql(
                    "SELECT left_t.id, left_t.value, right_t.data FROM left_t INNER JOIN right_t ON left_t.id = right_t.id",
                ),
            )
            .unwrap();
        });
    });

    group.finish();
}

fn quick_aggregations(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick_aggregations");
    configure_quick(&mut group);

    let config = QuickDatasetConfig::new(100);
    group.throughput(Throughput::Bytes(config.total_bytes));

    group.bench_function("count_100_rows", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        setup_test_data(&mut executor, "test", config.rows);

        b.iter(|| {
            black_box(executor.execute_sql("SELECT COUNT(*) FROM test")).unwrap();
        });
    });

    let rows = 100;
    let mut total_bytes = 0u64;
    for i in 0..rows {
        total_bytes += 8;
        total_bytes += format!("cat_{}", i % 10).len() as u64;
        total_bytes += 8;
    }
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_function("group_by_10_groups", |b| {
        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
        executor
            .execute_sql("CREATE TABLE sales (id INT64, category STRING, amount INT64)")
            .unwrap();
        for i in 0..100 {
            executor
                .execute_sql(&format!(
                    "INSERT INTO sales VALUES ({}, 'cat_{}', {})",
                    i,
                    i % 10,
                    i * 100
                ))
                .unwrap();
        }

        b.iter(|| {
            black_box(executor.execute_sql(
                "SELECT category, COUNT(*), SUM(amount) FROM sales GROUP BY category",
            ))
            .unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    quick_basic_operations,
    quick_joins,
    quick_aggregations
);
criterion_main!(benches);
