#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
struct MergeConfig {
    target_rows: usize,
    source_rows: usize,
    #[allow(dead_code)]
    match_ratio: f64,
}

impl MergeConfig {
    fn new(target_rows: usize, source_rows: usize, match_ratio: f64) -> Self {
        Self {
            target_rows,
            source_rows,
            match_ratio,
        }
    }
}

fn setup_merge_tables(executor: &mut QueryExecutor, config: MergeConfig) {
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64, status STRING)")
        .unwrap();

    for i in 0..config.target_rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO target VALUES ({}, {}, 'active')",
                i,
                i * 10
            ))
            .unwrap();
    }

    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64, status STRING)")
        .unwrap();

    for i in 0..config.source_rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO source VALUES ({}, {}, 'updated')",
                i,
                i * 20
            ))
            .unwrap();
    }
}

fn setup_merge_with_cascade(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql("CREATE TABLE orders (order_id INT64 PRIMARY KEY, total FLOAT64)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE order_items (
                item_id INT64 PRIMARY KEY,
                order_id INT64,
                product STRING,
                FOREIGN KEY (order_id) REFERENCES orders(order_id) ON DELETE CASCADE
            )",
        )
        .unwrap();

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO orders VALUES ({}, {})",
                i,
                i as f64 * 100.0
            ))
            .unwrap();
    }

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO order_items VALUES ({}, {}, 'Product A')",
                i * 2,
                i
            ))
            .unwrap();
        executor
            .execute_sql(&format!(
                "INSERT INTO order_items VALUES ({}, {}, 'Product B')",
                i * 2 + 1,
                i
            ))
            .unwrap();
    }

    executor
        .execute_sql("CREATE TABLE cancellations (order_id INT64)")
        .unwrap();

    for i in 0..rows / 2 {
        executor
            .execute_sql(&format!("INSERT INTO cancellations VALUES ({})", i))
            .unwrap();
    }
}

fn bench_merge_hash_join_optimization(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_hash_join");
    configure_standard(&mut group);

    for size in [100, 500, 1000, 2000, 5000].iter() {
        let config = MergeConfig::new(*size, *size, 0.5);

        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, _| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    setup_merge_tables(&mut executor, config);
                    executor
                },
                |mut executor| {
                    black_box(
                        executor
                            .execute_sql(
                                "MERGE INTO target AS t
                                 USING source AS s
                                 ON t.id = s.id
                                 WHEN MATCHED THEN
                                   UPDATE SET value = s.value, status = s.status
                                 WHEN NOT MATCHED THEN
                                   INSERT (id, value, status) VALUES (s.id, s.value, s.status)",
                            )
                            .unwrap(),
                    )
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_merge_vs_traditional(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_vs_traditional");
    configure_standard(&mut group);

    let config = MergeConfig::new(1000, 1000, 0.5);

    group.bench_function("merge_single_statement", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN
                               UPDATE SET value = s.value, status = s.status
                             WHEN NOT MATCHED THEN
                               INSERT (id, value, status) VALUES (s.id, s.value, s.status)",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("traditional_update_insert", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {

                black_box(
                    executor
                        .execute_sql(
                            "UPDATE target SET value = (SELECT value FROM source WHERE source.id = target.id),
                                             status = (SELECT status FROM source WHERE source.id = target.id)
                             WHERE id IN (SELECT id FROM source)",
                        )
                        .unwrap(),
                );

                black_box(
                    executor
                        .execute_sql(
                            "INSERT INTO target SELECT * FROM source WHERE id NOT IN (SELECT id FROM target)",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_merge_operation_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_patterns");
    configure_standard(&mut group);

    let size = 1000;

    group.bench_function("insert_heavy_10pct_match", |b| {
        let config = MergeConfig::new(size, size, 0.1);
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN
                               UPDATE SET value = s.value
                             WHEN NOT MATCHED THEN
                               INSERT (id, value, status) VALUES (s.id, s.value, s.status)",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("update_heavy_90pct_match", |b| {
        let config = MergeConfig::new(size, size, 0.9);
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN
                               UPDATE SET value = s.value
                             WHEN NOT MATCHED THEN
                               INSERT (id, value, status) VALUES (s.id, s.value, s.status)",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("balanced_50pct_match", |b| {
        let config = MergeConfig::new(size, size, 0.5);
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN
                               UPDATE SET value = s.value
                             WHEN NOT MATCHED THEN
                               INSERT (id, value, status) VALUES (s.id, s.value, s.status)",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("delete_only", |b| {
        let config = MergeConfig::new(size, size / 2, 1.0);
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN DELETE",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_merge_cascade_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_cascade");
    configure_standard(&mut group);

    for size in [100, 500, 1000].iter() {
        group.throughput(Throughput::Elements(*size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    setup_merge_with_cascade(&mut executor, size);
                    executor
                },
                |mut executor| {
                    black_box(
                        executor
                            .execute_sql(
                                "MERGE INTO orders AS t
                                 USING cancellations AS s
                                 ON t.order_id = s.order_id
                                 WHEN MATCHED THEN DELETE",
                            )
                            .unwrap(),
                    )
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_merge_complex_conditions(c: &mut Criterion) {
    let mut group = c.benchmark_group("merge_complex");
    configure_standard(&mut group);

    let config = MergeConfig::new(1000, 1000, 0.5);

    group.bench_function("simple_equijoin", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id
                             WHEN MATCHED THEN UPDATE SET value = s.value",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("compound_equijoin", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id AND t.status = s.status
                             WHEN MATCHED THEN UPDATE SET value = s.value",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("complex_condition_with_or", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_merge_tables(&mut executor, config);
                executor
            },
            |mut executor| {
                black_box(
                    executor
                        .execute_sql(
                            "MERGE INTO target AS t
                             USING source AS s
                             ON t.id = s.id OR t.value = s.value
                             WHEN MATCHED THEN UPDATE SET status = s.status",
                        )
                        .unwrap(),
                )
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_merge_hash_join_optimization,
    bench_merge_vs_traditional,
    bench_merge_operation_patterns,
    bench_merge_cascade_delete,
    bench_merge_complex_conditions,
);
criterion_main!(benches);
