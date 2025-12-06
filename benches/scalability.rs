#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

fn bench_row_count_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("row_count_scaling");
    configure_standard(&mut group);

    for &size in &[1000, 5000, 10000, 50000, 100000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("full_scan", size), &size, |b, &size| {
            b.iter_batched(
                || {
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

                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql("SELECT * FROM data")).unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(
            BenchmarkId::new("filtered_scan", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
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

                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(&format!(
                            "SELECT * FROM data WHERE value > {}",
                            size * 5
                        )))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(BenchmarkId::new("aggregation", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    executor
                        .execute_sql("CREATE TABLE data (id INT64, category STRING, value INT64)")
                        .unwrap();

                    for i in 0..size {
                        executor
                            .execute_sql(&format!(
                                "INSERT INTO data VALUES ({}, 'cat_{}', {})",
                                i,
                                i % 100,
                                i * 10
                            ))
                            .unwrap();
                    }

                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "SELECT category, COUNT(*), SUM(value)
                                 FROM data
                                 GROUP BY category",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_column_count_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("column_count_scaling");
    configure_standard(&mut group);

    let row_count = 1000;

    for &col_count in &[5, 10, 20, 50] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("wide_table", col_count),
            &col_count,
            |b, &col_count| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

                        let mut columns = vec!["id INT64".to_string()];
                        for i in 1..col_count {
                            columns.push(format!("col{} INT64", i));
                        }
                        let create_sql =
                            format!("CREATE TABLE wide_table ({})", columns.join(", "));
                        executor.execute_sql(&create_sql).unwrap();

                        for i in 0..row_count {
                            let mut values = vec![i.to_string()];
                            for j in 1..col_count {
                                values.push((i * j).to_string());
                            }
                            let insert_sql =
                                format!("INSERT INTO wide_table VALUES ({})", values.join(", "));
                            executor.execute_sql(&insert_sql).unwrap();
                        }

                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql("SELECT * FROM wide_table")).unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_join_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_scaling");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000, 5000] {
        group.throughput(Throughput::Elements((size * 2) as u64));
        group.bench_with_input(
            BenchmarkId::new("two_table_join", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        executor
                            .execute_sql("CREATE TABLE t1 (id INT64, value STRING)")
                            .unwrap();
                        executor
                            .execute_sql("CREATE TABLE t2 (id INT64, data STRING)")
                            .unwrap();

                        for i in 0..size {
                            executor
                                .execute_sql(&format!("INSERT INTO t1 VALUES ({}, 'val_{}')", i, i))
                                .unwrap();
                            executor
                                .execute_sql(&format!(
                                    "INSERT INTO t2 VALUES ({}, 'data_{}')",
                                    i, i
                                ))
                                .unwrap();
                        }

                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql("SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id"),
                        )
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_query_complexity_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_complexity");
    configure_standard(&mut group);

    let row_count = 1000;

    group.bench_function("simple_query", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE data (id INT64, value INT64)")
                    .unwrap();
                for i in 0..row_count {
                    executor
                        .execute_sql(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10))
                        .unwrap();
                }
                executor
            },
            |mut executor| {
                black_box(executor.execute_sql("SELECT * FROM data WHERE value > 5000")).unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("medium_complexity", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE data (id INT64, category STRING, value INT64)")
                    .unwrap();
                for i in 0..row_count {
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO data VALUES ({}, 'cat_{}', {})",
                            i,
                            i % 10,
                            i * 10
                        ))
                        .unwrap();
                }
                executor
            },
            |mut executor| {
                black_box(executor.execute_sql(
                    "SELECT category, COUNT(*), AVG(value)
                         FROM data
                         WHERE value > 5000
                         GROUP BY category
                         HAVING COUNT(*) > 10",
                ))
                .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.bench_function("high_complexity", |b| {
        b.iter_batched(
            || {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE data (id INT64, category STRING, value INT64)")
                    .unwrap();
                for i in 0..row_count {
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO data VALUES ({}, 'cat_{}', {})",
                            i,
                            i % 10,
                            i * 10
                        ))
                        .unwrap();
                }
                executor
            },
            |mut executor| {
                black_box(executor.execute_sql(
                    "WITH category_stats AS (
                            SELECT category, AVG(value) as avg_val
                            FROM data
                            GROUP BY category
                         )
                         SELECT d.category, d.value, cs.avg_val
                         FROM data d
                         INNER JOIN category_stats cs ON d.category = cs.category
                         WHERE d.value > cs.avg_val
                         ORDER BY d.value DESC
                         LIMIT 100",
                ))
                .unwrap();
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

fn bench_insert_scaling(c: &mut Criterion) {
    let mut group = c.benchmark_group("insert_scaling");
    configure_standard(&mut group);

    for &batch_size in &[10, 100, 1000, 10000] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("sequential_inserts", batch_size),
            &batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    executor
                        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
                        .unwrap();

                    for i in 0..batch_size {
                        black_box(executor.execute_sql(&format!(
                            "INSERT INTO data VALUES ({}, {})",
                            i,
                            i * 10
                        )))
                        .unwrap();
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_row_count_scaling,
    bench_column_count_scaling,
    bench_join_scaling,
    bench_query_complexity_scaling,
    bench_insert_scaling
);
criterion_main!(benches);
