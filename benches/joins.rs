#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

#[derive(Debug, Clone, Copy)]
struct TwoTableDatasetConfig {
    left_rows: usize,
    right_rows: usize,
    total_bytes: u64,
}

impl TwoTableDatasetConfig {
    fn new(left_rows: usize, right_rows: usize) -> Self {
        let total_bytes = Self::calculate_bytes(left_rows, right_rows);
        Self {
            left_rows,
            right_rows,
            total_bytes,
        }
    }

    fn calculate_bytes(left_rows: usize, right_rows: usize) -> u64 {
        let mut total_bytes = 0u64;

        for i in 0..left_rows {
            total_bytes += 8;
            total_bytes += format!("left_{}", i).len() as u64;
            total_bytes += 8;
        }

        for i in 0..right_rows {
            total_bytes += 8;
            total_bytes += format!("right_{}", i).len() as u64;
            total_bytes += 8;
        }

        total_bytes
    }
}

#[derive(Debug, Clone, Copy)]
struct CustomersOrdersDatasetConfig {
    num_customers: usize,
    num_orders: usize,
    total_bytes: u64,
}

impl CustomersOrdersDatasetConfig {
    fn new(num_customers: usize, num_orders: usize) -> Self {
        let total_bytes = Self::calculate_bytes(num_customers, num_orders);
        Self {
            num_customers,
            num_orders,
            total_bytes,
        }
    }

    fn calculate_bytes(num_customers: usize, num_orders: usize) -> u64 {
        let mut total_bytes = 0u64;

        for i in 0..num_customers {
            total_bytes += 8;
            total_bytes += format!("customer_{}", i).len() as u64;
            total_bytes += format!("region_{}", i % 10).len() as u64;
        }

        for i in 0..num_orders {
            total_bytes += 8;
            total_bytes += 8;
            total_bytes += 8;
            total_bytes += format!("product_{}", i % 50).len() as u64;
        }

        total_bytes
    }
}

#[derive(Debug, Clone, Copy)]
struct ThreeTableDatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl ThreeTableDatasetConfig {
    fn new(rows: usize) -> Self {
        let total_bytes = Self::calculate_bytes(rows);
        Self { rows, total_bytes }
    }

    fn calculate_bytes(rows: usize) -> u64 {
        let mut total_bytes = 0u64;

        for i in 0..rows {
            total_bytes += 8;
            total_bytes += format!("a_{}", i).len() as u64;

            total_bytes += 8;
            total_bytes += 8;
            total_bytes += format!("b_{}", i).len() as u64;

            total_bytes += 8;
            total_bytes += 8;
            total_bytes += 8;
        }

        total_bytes
    }
}

#[derive(Debug, Clone, Copy)]
struct EmployeesDatasetConfig {
    rows: usize,
    total_bytes: u64,
}

impl EmployeesDatasetConfig {
    fn new(rows: usize) -> Self {
        let total_bytes = Self::calculate_bytes(rows);
        Self { rows, total_bytes }
    }

    fn calculate_bytes(rows: usize) -> u64 {
        let mut total_bytes = 0u64;

        for i in 0..rows {
            total_bytes += 8;
            total_bytes += format!("emp_{}", i).len() as u64;
            total_bytes += 8;
        }

        total_bytes
    }
}

fn setup_customers_orders(executor: &mut QueryExecutor, num_customers: usize, num_orders: usize) {
    executor
        .execute_sql("CREATE TABLE customers (id INT64, name STRING, region STRING)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE orders (order_id INT64, customer_id INT64, amount INT64, product STRING)",
        )
        .unwrap();

    for i in 0..num_customers {
        executor
            .execute_sql(&format!(
                "INSERT INTO customers VALUES ({}, 'customer_{}', 'region_{}')",
                i,
                i,
                i % 10
            ))
            .unwrap();
    }

    for i in 0..num_orders {
        let customer_id = i % num_customers;
        executor
            .execute_sql(&format!(
                "INSERT INTO orders VALUES ({}, {}, {}, 'product_{}')",
                i,
                customer_id,
                (i * 100) % 10000,
                i % 50
            ))
            .unwrap();
    }
}

fn bench_inner_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("inner_join");
    configure_standard(&mut group);

    let scenarios = vec![
        ("small_x_small", 100, 100),
        ("small_x_medium", 100, 1000),
        ("medium_x_medium", 1000, 1000),
        ("medium_x_large", 1000, 10000),
    ];

    for (name, left_size, right_size) in scenarios {
        let config = TwoTableDatasetConfig::new(left_size, right_size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_function(name, |b| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_join_tables(&mut executor, left_size, right_size, 0.8);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT * FROM left_table
                         INNER JOIN right_table ON left_table.id = right_table.id",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_left_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("left_join");
    configure_standard(&mut group);

    let scenarios = vec![
        ("small_x_small", 100, 100),
        ("medium_x_medium", 1000, 1000),
        ("large_x_small", 10000, 100),
    ];

    for (name, left_size, right_size) in scenarios {
        let config = TwoTableDatasetConfig::new(left_size, right_size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_function(name, |b| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_join_tables(&mut executor, left_size, right_size, 0.5);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT * FROM left_table
                         LEFT JOIN right_table ON left_table.id = right_table.id",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_right_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("right_join");
    configure_standard(&mut group);

    let scenarios = vec![("small_x_small", 100, 100), ("medium_x_medium", 1000, 1000)];

    for (name, left_size, right_size) in scenarios {
        let config = TwoTableDatasetConfig::new(left_size, right_size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_function(name, |b| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_join_tables(&mut executor, left_size, right_size, 0.5);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT * FROM left_table
                         RIGHT JOIN right_table ON left_table.id = right_table.id",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_full_outer_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("full_outer_join");
    configure_standard(&mut group);

    let scenarios = vec![("small_x_small", 100, 100), ("medium_x_medium", 1000, 1000)];

    for (name, left_size, right_size) in scenarios {
        let config = TwoTableDatasetConfig::new(left_size, right_size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_function(name, |b| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_join_tables(&mut executor, left_size, right_size, 0.5);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT * FROM left_table
                         FULL OUTER JOIN right_table ON left_table.id = right_table.id",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_cross_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("cross_join");
    configure_standard(&mut group);

    let scenarios = vec![
        ("tiny_x_tiny", 10, 10),
        ("small_x_small", 50, 50),
        ("small_x_medium", 50, 100),
    ];

    for (name, left_size, right_size) in scenarios {
        let config = TwoTableDatasetConfig::new(left_size, right_size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_function(name, |b| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_join_tables(&mut executor, left_size, right_size, 0.0);

            b.iter(|| {
                black_box(executor.execute_sql("SELECT * FROM left_table CROSS JOIN right_table"))
                    .unwrap();
            });
        });
    }

    group.finish();
}

fn bench_join_with_where(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_with_where");
    configure_standard(&mut group);

    for &size in &[100, 1000, 5000] {
        let config = TwoTableDatasetConfig::new(size, size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("inner_join_filter", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_join_tables(&mut executor, config.left_rows, config.right_rows, 0.8);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT * FROM left_table
                         INNER JOIN right_table ON left_table.id = right_table.id
                         WHERE left_table.amount > 1000",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_join_with_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_with_aggregation");
    configure_standard(&mut group);

    for &num_customers in &[100, 500, 1000] {
        let num_orders = num_customers * 10;
        let config = CustomersOrdersDatasetConfig::new(num_customers, num_orders);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("join_group_by", num_customers),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_customers_orders(&mut executor, config.num_customers, config.num_orders);

                b.iter(|| {
                    black_box(
                        executor.execute_sql(
                            "SELECT customers.name, COUNT(*) as order_count, SUM(orders.amount) as total
                             FROM customers
                             INNER JOIN orders ON customers.id = orders.customer_id
                             GROUP BY customers.name"
                        )
                    )
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_multi_table_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_table_join");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        let config = ThreeTableDatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("three_table_join", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

                executor
                    .execute_sql("CREATE TABLE table_a (id INT64, value STRING)")
                    .unwrap();
                executor
                    .execute_sql("CREATE TABLE table_b (id INT64, a_id INT64, data STRING)")
                    .unwrap();
                executor
                    .execute_sql("CREATE TABLE table_c (id INT64, b_id INT64, score INT64)")
                    .unwrap();

                for i in 0..config.rows {
                    executor
                        .execute_sql(&format!("INSERT INTO table_a VALUES ({}, 'a_{}')", i, i))
                        .unwrap();
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO table_b VALUES ({}, {}, 'b_{}')",
                            i,
                            i % (config.rows / 2),
                            i
                        ))
                        .unwrap();
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO table_c VALUES ({}, {}, {})",
                            i,
                            i % (config.rows / 2),
                            i * 10
                        ))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT table_a.value, table_b.data, table_c.score
                             FROM table_a
                             INNER JOIN table_b ON table_a.id = table_b.a_id
                             INNER JOIN table_c ON table_b.id = table_c.b_id",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_self_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("self_join");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        let config = EmployeesDatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("self_join", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                executor
                    .execute_sql("CREATE TABLE employees (id INT64, name STRING, manager_id INT64)")
                    .unwrap();

                for i in 0..config.rows {
                    let manager_id = if i > 0 { i / 2 } else { 0 };
                    executor
                        .execute_sql(&format!(
                            "INSERT INTO employees VALUES ({}, 'emp_{}', {})",
                            i, i, manager_id
                        ))
                        .unwrap();
                }

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT e1.name as employee, e2.name as manager
                         FROM employees e1
                         LEFT JOIN employees e2 ON e1.manager_id = e2.id",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_join_selectivity(c: &mut Criterion) {
    let mut group = c.benchmark_group("join_selectivity");
    configure_standard(&mut group);

    let size = 1000;

    for &match_ratio in &[0.1, 0.5, 0.9] {
        let config = TwoTableDatasetConfig::new(size, size);
        group.throughput(Throughput::Bytes(config.total_bytes));
        let label = format!("match_ratio_{}", (match_ratio * 100.0) as u32);

        group.bench_function(&label, |b| {
            let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
            setup_join_tables(&mut executor, size, size, match_ratio);

            b.iter(|| {
                black_box(executor.execute_sql(
                    "SELECT * FROM left_table
                         INNER JOIN right_table ON left_table.id = right_table.id",
                ))
                .unwrap();
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_inner_join,
    bench_left_join,
    bench_right_join,
    bench_full_outer_join,
    bench_cross_join,
    bench_join_with_where,
    bench_join_with_aggregation,
    bench_multi_table_join,
    bench_self_join,
    bench_join_selectivity
);
criterion_main!(benches);
