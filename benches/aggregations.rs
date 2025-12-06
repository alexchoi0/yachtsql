#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
enum DatasetSize {
    Small,
    Medium,
    Large,
}

impl DatasetSize {
    fn rows(&self) -> usize {
        match self {
            DatasetSize::Small => 128,
            DatasetSize::Medium => 256,
            DatasetSize::Large => 512,
        }
    }

    fn all() -> &'static [DatasetSize] {
        &[DatasetSize::Small, DatasetSize::Medium, DatasetSize::Large]
    }
}

fn bench_count_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("count_operations");
    configure_standard(&mut group);

    for &dataset_size in DatasetSize::all() {
        let rows = dataset_size.rows();
        let num_products = 100;
        let total_bytes = calculate_sales_bytes(rows, num_products);

        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(BenchmarkId::new("count_star", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT COUNT(*) FROM sales")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("count_column", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT COUNT(product) FROM sales")).unwrap();
            });
        });

        group.bench_with_input(
            BenchmarkId::new("count_distinct", rows),
            &rows,
            |b, &rows| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_sales_table(&mut executor, rows, num_products);

                b.iter(|| {
                    black_box(executor.execute_sql("SELECT COUNT(DISTINCT product) FROM sales"))
                        .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_basic_aggregates(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_aggregates");
    configure_standard(&mut group);

    for &dataset_size in &[DatasetSize::Medium, DatasetSize::Large] {
        let rows = dataset_size.rows();
        let num_products = 100;
        let total_bytes = calculate_sales_bytes(rows, num_products);

        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(BenchmarkId::new("sum", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT SUM(amount) FROM sales")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("avg", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT AVG(amount) FROM sales")).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("min_max", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT MIN(amount), MAX(amount) FROM sales"))
                    .unwrap();
            });
        });

        group.bench_with_input(
            BenchmarkId::new("all_aggregates", rows),
            &rows,
            |b, &rows| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_sales_table(&mut executor, rows, num_products);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount)
                         FROM sales",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_group_by_cardinality(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_cardinality");
    configure_heavy(&mut group);

    let rows = 10000;

    for &cardinality in &[10, 100, 1000, 5000] {
        let total_bytes = calculate_sales_bytes(rows, cardinality);
        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(
            BenchmarkId::new("group_by_count", cardinality),
            &cardinality,
            |b, &cardinality| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_sales_table(&mut executor, rows, cardinality);

                b.iter(|| {
                    black_box(
                        executor
                            .execute_sql("SELECT product, COUNT(*) FROM sales GROUP BY product"),
                    )
                    .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("group_by_sum", cardinality),
            &cardinality,
            |b, &cardinality| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_sales_table(&mut executor, rows, cardinality);

                b.iter(|| {
                    black_box(
                        executor
                            .execute_sql("SELECT product, SUM(amount) FROM sales GROUP BY product"),
                    )
                    .unwrap();
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("group_by_multi_agg", cardinality),
            &cardinality,
            |b, &cardinality| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_sales_table(&mut executor, rows, cardinality);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT product, COUNT(*), SUM(amount), AVG(amount)
                         FROM sales GROUP BY product",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_group_by_multiple_columns(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_by_multiple_columns");
    configure_standard(&mut group);

    for &dataset_size in &[DatasetSize::Medium, DatasetSize::Large] {
        let rows = dataset_size.rows();
        let num_products = 100;
        let total_bytes = calculate_sales_bytes(rows, num_products);

        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(BenchmarkId::new("two_columns", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT product, region, COUNT(*)
                         FROM sales GROUP BY product, region",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_having_clause(c: &mut Criterion) {
    let mut group = c.benchmark_group("having_clause");
    configure_standard(&mut group);

    for &dataset_size in &[DatasetSize::Medium, DatasetSize::Large] {
        let rows = dataset_size.rows();
        let num_products = 100;
        let total_bytes = calculate_sales_bytes(rows, num_products);

        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(BenchmarkId::new("having_count", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT product, COUNT(*) as cnt
                         FROM sales GROUP BY product
                         HAVING COUNT(*) > 5",
                ))
                .unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("having_sum", rows), &rows, |b, &rows| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_sales_table(&mut executor, rows, num_products);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT product, SUM(amount) as total
                         FROM sales GROUP BY product
                         HAVING SUM(amount) > 1000",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_string_agg(c: &mut Criterion) {
    let mut group = c.benchmark_group("string_agg");
    configure_heavy(&mut group);

    for &size in &[100, 500, 1000] {
        let num_products = 10;
        let total_bytes = calculate_sales_bytes(size, num_products);

        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(
            BenchmarkId::new("string_agg_basic", size),
            &size,
            |b, &size| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_sales_table(&mut executor, size, num_products);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT product, STRING_AGG(CAST(id AS STRING), ',')
                         FROM sales GROUP BY product",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_array_agg(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_agg");
    configure_heavy(&mut group);

    for &size in &[100, 500, 1000] {
        let num_products = 10;
        let total_bytes = calculate_sales_bytes(size, num_products);

        group.throughput(Throughput::Bytes(total_bytes));

        group.bench_with_input(
            BenchmarkId::new("array_agg_basic", size),
            &size,
            |b, &size| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_sales_table(&mut executor, size, num_products);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT product, ARRAY_AGG(amount)
                         FROM sales GROUP BY product",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_count_operations,
    bench_basic_aggregates,
    bench_group_by_cardinality,
    bench_group_by_multiple_columns,
    bench_having_clause,
    bench_string_agg,
    bench_array_agg
);

criterion_main!(benches);
