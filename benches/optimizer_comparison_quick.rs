#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

fn setup_test_data(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql(
            "CREATE TABLE test_table (
                id INT64,
                category STRING,
                value INT64,
                score FLOAT64
            )",
        )
        .unwrap();

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO test_table VALUES ({}, 'cat_{}', {}, {})",
                i,
                i % 5,
                i * 100,
                i as f64 * 1.5
            ))
            .unwrap();
    }
}

fn bench_predicate_pushdown_quick(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick/predicate_pushdown");
    configure_standard(&mut group);

    let rows = 1000;
    group.throughput(Throughput::Elements(rows as u64));

    let query = "SELECT * FROM (
                    SELECT id, category, value
                    FROM test_table
                    WHERE value > 50000
                 ) sub
                 WHERE category = 'cat_2'";

    let mut executor_with = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    setup_test_data(&mut executor_with, rows);

    group.bench_function("with_optimizer", |b| {
        b.iter(|| {
            black_box(executor_with.execute_sql(query)).unwrap();
        });
    });

    let mut executor_without = QueryExecutor::new_without_optimizer();
    setup_test_data(&mut executor_without, rows);

    group.bench_function("without_optimizer", |b| {
        b.iter(|| {
            black_box(executor_without.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_projection_pushdown_quick(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick/projection_pushdown");
    configure_standard(&mut group);

    let rows = 1000;
    group.throughput(Throughput::Elements(rows as u64));

    let query = "SELECT id, value FROM (
                    SELECT *
                    FROM test_table
                    WHERE value > 10000
                 ) sub";

    let mut executor_with = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    setup_test_data(&mut executor_with, rows);

    group.bench_function("with_optimizer", |b| {
        b.iter(|| {
            black_box(executor_with.execute_sql(query)).unwrap();
        });
    });

    let mut executor_without = QueryExecutor::new_without_optimizer();
    setup_test_data(&mut executor_without, rows);

    group.bench_function("without_optimizer", |b| {
        b.iter(|| {
            black_box(executor_without.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

fn bench_constant_folding_quick(c: &mut Criterion) {
    let mut group = c.benchmark_group("quick/constant_folding");
    configure_standard(&mut group);

    let rows = 1000;
    group.throughput(Throughput::Elements(rows as u64));

    let query = "SELECT * FROM test_table
                 WHERE value > 100 * 100 + 10000";

    let mut executor_with = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    setup_test_data(&mut executor_with, rows);

    group.bench_function("with_optimizer", |b| {
        b.iter(|| {
            black_box(executor_with.execute_sql(query)).unwrap();
        });
    });

    let mut executor_without = QueryExecutor::new_without_optimizer();
    setup_test_data(&mut executor_without, rows);

    group.bench_function("without_optimizer", |b| {
        b.iter(|| {
            black_box(executor_without.execute_sql(query)).unwrap();
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_predicate_pushdown_quick,
    bench_projection_pushdown_quick,
    bench_constant_folding_quick
);
criterion_main!(benches);
