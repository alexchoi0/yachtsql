#[allow(clippy::duplicate_mod)]
#[path = "helpers.rs"]
mod helpers;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use helpers::*;
use yachtsql::{DialectType, QueryExecutor};

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
            let dept_id = i % 5;
            total_bytes += 8;
            total_bytes += format!("emp_{}", i).len() as u64;
            total_bytes += format!("dept_{}", dept_id).len() as u64;
            total_bytes += 8;
            total_bytes += 8;
        }

        total_bytes
    }
}

fn setup_employees_data(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql(
            "CREATE TABLE employees (
                id INT64,
                name STRING,
                department STRING,
                salary INT64,
                manager_id INT64
            )",
        )
        .unwrap();

    for i in 0..rows {
        let dept_id = i % 5;
        let manager_id = if i > 0 { i / 10 } else { 0 };
        executor
            .execute_sql(&format!(
                "INSERT INTO employees VALUES ({}, 'emp_{}', 'dept_{}', {}, {})",
                i,
                i,
                dept_id,
                30000 + (i * 1000) % 70000,
                manager_id
            ))
            .unwrap();
    }
}

fn setup_sales_data(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql(
            "CREATE TABLE sales (
                id INT64,
                product STRING,
                region STRING,
                amount INT64,
                sale_date DATE
            )",
        )
        .unwrap();

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO sales VALUES ({}, 'product_{}', 'region_{}', {}, DATE '2024-01-{}');",
                i,
                i % 50,
                i % 10,
                1000 + (i * 50) % 5000,
                1 + (i % 28)
            ))
            .unwrap();
    }
}

fn bench_scalar_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("scalar_subquery");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        let config = EmployeesDatasetConfig::new(size);
        group.throughput(Throughput::Bytes(config.total_bytes));

        group.bench_with_input(
            BenchmarkId::new("avg_comparison", size),
            &config,
            |b, &config| {
                let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                setup_employees_data(&mut executor, config.rows);

                b.iter(|| {
                    black_box(executor.execute_sql(
                        "SELECT name, salary FROM employees
                             WHERE salary > (SELECT AVG(salary) FROM employees)",
                    ))
                    .unwrap();
                });
            },
        );
    }

    group.finish();
}

fn bench_correlated_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("correlated_subquery");
    configure_standard(&mut group);

    for &size in &[50, 100, 200] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("dept_avg_comparison", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_employees_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "SELECT e1.name, e1.salary
                                 FROM employees e1
                                 WHERE e1.salary > (
                                    SELECT AVG(e2.salary)
                                    FROM employees e2
                                    WHERE e2.department = e1.department
                                 )",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_subquery_in_from(c: &mut Criterion) {
    let mut group = c.benchmark_group("subquery_in_from");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("derived_table", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_sales_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "SELECT * FROM (
                                    SELECT product, SUM(amount) as total
                                    FROM sales
                                    GROUP BY product
                                 ) as product_totals
                                 WHERE total > 50000",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_cte_simple(c: &mut Criterion) {
    let mut group = c.benchmark_group("cte_simple");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("single_cte", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    setup_sales_data(&mut executor, size);
                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "WITH product_totals AS (
                                SELECT product, SUM(amount) as total
                                FROM sales
                                GROUP BY product
                             )
                             SELECT * FROM product_totals WHERE total > 50000",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_cte_multiple(c: &mut Criterion) {
    let mut group = c.benchmark_group("cte_multiple");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("multiple_ctes", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_employees_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "WITH
                                 high_earners AS (
                                    SELECT * FROM employees WHERE salary > 60000
                                 ),
                                 dept_counts AS (
                                    SELECT department, COUNT(*) as cnt
                                    FROM high_earners
                                    GROUP BY department
                                 )
                                 SELECT * FROM dept_counts WHERE cnt > 5",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_window_functions(c: &mut Criterion) {
    let mut group = c.benchmark_group("window_functions");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(BenchmarkId::new("row_number", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    setup_sales_data(&mut executor, size);
                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "SELECT product, amount,
                                        ROW_NUMBER() OVER (ORDER BY amount DESC) as rank
                                 FROM sales",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(
            BenchmarkId::new("partition_by", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_sales_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql(
                                "SELECT product, region, amount,
                                        ROW_NUMBER() OVER (PARTITION BY region ORDER BY amount DESC) as region_rank
                                 FROM sales"
                            )
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

fn bench_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("distinct");
    configure_standard(&mut group);

    for &size in &[100, 1000, 10000] {
        group.throughput(Throughput::Elements(size as u64));

        group.bench_with_input(
            BenchmarkId::new("distinct_single_col", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_sales_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql("SELECT DISTINCT product FROM sales"))
                            .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("distinct_multi_col", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_sales_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql("SELECT DISTINCT product, region FROM sales"),
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

fn bench_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("union");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements((size * 2) as u64));

        group.bench_with_input(BenchmarkId::new("union", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    executor
                        .execute_sql("CREATE TABLE set1 (id INT64, value STRING)")
                        .unwrap();
                    executor
                        .execute_sql("CREATE TABLE set2 (id INT64, value STRING)")
                        .unwrap();

                    for i in 0..size {
                        executor
                            .execute_sql(&format!("INSERT INTO set1 VALUES ({}, 'val_{}')", i, i))
                            .unwrap();
                        executor
                            .execute_sql(&format!(
                                "INSERT INTO set2 VALUES ({}, 'val_{}')",
                                i + size / 2,
                                i + size / 2
                            ))
                            .unwrap();
                    }

                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "SELECT id, value FROM set1
                             UNION
                             SELECT id, value FROM set2",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("union_all", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    executor
                        .execute_sql("CREATE TABLE set1 (id INT64, value STRING)")
                        .unwrap();
                    executor
                        .execute_sql("CREATE TABLE set2 (id INT64, value STRING)")
                        .unwrap();

                    for i in 0..size {
                        executor
                            .execute_sql(&format!("INSERT INTO set1 VALUES ({}, 'val_{}')", i, i))
                            .unwrap();
                        executor
                            .execute_sql(&format!(
                                "INSERT INTO set2 VALUES ({}, 'val_{}')",
                                i + size / 2,
                                i + size / 2
                            ))
                            .unwrap();
                    }

                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "SELECT id, value FROM set1
                             UNION ALL
                             SELECT id, value FROM set2",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_intersect_except(c: &mut Criterion) {
    let mut group = c.benchmark_group("intersect_except");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements((size * 2) as u64));

        group.bench_with_input(BenchmarkId::new("intersect", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    executor
                        .execute_sql("CREATE TABLE set1 (value INT64)")
                        .unwrap();
                    executor
                        .execute_sql("CREATE TABLE set2 (value INT64)")
                        .unwrap();

                    for i in 0..size {
                        executor
                            .execute_sql(&format!("INSERT INTO set1 VALUES ({})", i))
                            .unwrap();
                        executor
                            .execute_sql(&format!("INSERT INTO set2 VALUES ({})", i + size / 2))
                            .unwrap();
                    }

                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "SELECT value FROM set1
                             INTERSECT
                             SELECT value FROM set2",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });

        group.bench_with_input(BenchmarkId::new("except", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    executor
                        .execute_sql("CREATE TABLE set1 (value INT64)")
                        .unwrap();
                    executor
                        .execute_sql("CREATE TABLE set2 (value INT64)")
                        .unwrap();

                    for i in 0..size {
                        executor
                            .execute_sql(&format!("INSERT INTO set1 VALUES ({})", i))
                            .unwrap();
                        executor
                            .execute_sql(&format!("INSERT INTO set2 VALUES ({})", i + size / 2))
                            .unwrap();
                    }

                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "SELECT value FROM set1
                             EXCEPT
                             SELECT value FROM set2",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_exists_subquery(c: &mut Criterion) {
    let mut group = c.benchmark_group("exists_subquery");
    configure_standard(&mut group);

    for &size in &[100, 500, 1000] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::new("exists", size), &size, |b, &size| {
            b.iter_batched(
                || {
                    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                    executor
                        .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
                        .unwrap();
                    executor
                        .execute_sql("CREATE TABLE orders (customer_id INT64, amount INT64)")
                        .unwrap();

                    for i in 0..size {
                        executor
                            .execute_sql(&format!(
                                "INSERT INTO customers VALUES ({}, 'customer_{}')",
                                i, i
                            ))
                            .unwrap();

                        if i % 3 == 0 {
                            executor
                                .execute_sql(&format!(
                                    "INSERT INTO orders VALUES ({}, {})",
                                    i,
                                    i * 100
                                ))
                                .unwrap();
                        }
                    }

                    executor
                },
                |mut executor| {
                    black_box(executor.execute_sql(
                        "SELECT name FROM customers c
                             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)",
                    ))
                    .unwrap();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

fn bench_nested_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("nested_queries");
    configure_standard(&mut group);

    for &size in &[50, 100, 200] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("three_level_nesting", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_employees_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "SELECT name, salary FROM employees
                                 WHERE department IN (
                                    SELECT department FROM (
                                        SELECT department, AVG(salary) as avg_sal
                                        FROM employees
                                        GROUP BY department
                                    ) sub
                                    WHERE avg_sal > 50000
                                 )",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn setup_multi_table_data(executor: &mut QueryExecutor, rows: usize) {
    executor
        .execute_sql(
            "CREATE TABLE customers (
                customer_id INT64,
                customer_name STRING,
                region STRING,
                tier STRING
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE orders (
                order_id INT64,
                customer_id INT64,
                product_id INT64,
                order_date DATE,
                quantity INT64,
                amount INT64
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE products (
                product_id INT64,
                product_name STRING,
                category STRING,
                price FLOAT64
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE suppliers (
                supplier_id INT64,
                product_id INT64,
                supplier_name STRING,
                country STRING
            )",
        )
        .unwrap();

    for i in 0..rows {
        executor
            .execute_sql(&format!(
                "INSERT INTO customers VALUES ({}, 'customer_{}', 'region_{}', 'tier_{}')",
                i,
                i,
                i % 10,
                i % 3
            ))
            .unwrap();

        executor
            .execute_sql(&format!(
                "INSERT INTO orders VALUES ({}, {}, {}, DATE '2024-01-{}', {}, {})",
                i,
                i % (rows / 2),
                i % (rows / 3),
                1 + (i % 28),
                (i % 10) + 1,
                100 + (i * 50) % 5000
            ))
            .unwrap();

        if i < rows / 3 {
            executor
                .execute_sql(&format!(
                    "INSERT INTO products VALUES ({}, 'product_{}', 'category_{}', {})",
                    i,
                    i,
                    i % 5,
                    10.0 + (i as f64 * 2.5)
                ))
                .unwrap();
        }

        if i < rows / 2 {
            executor
                .execute_sql(&format!(
                    "INSERT INTO suppliers VALUES ({}, {}, 'supplier_{}', 'country_{}')",
                    i,
                    i % (rows / 3),
                    i,
                    i % 20
                ))
                .unwrap();
        }
    }
}

fn bench_triple_join_with_cte(c: &mut Criterion) {
    let mut group = c.benchmark_group("triple_join_with_cte");
    configure_standard(&mut group);

    for &size in &[100, 300, 500] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("cte_triple_join", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_multi_table_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "WITH high_value_customers AS (
                                    SELECT customer_id, SUM(amount) as total_spent
                                    FROM orders
                                    GROUP BY customer_id
                                    HAVING SUM(amount) > 5000
                                 ),
                                 top_products AS (
                                    SELECT product_id, COUNT(*) as order_count
                                    FROM orders
                                    GROUP BY product_id
                                    HAVING COUNT(*) > 2
                                 )
                                 SELECT
                                    c.customer_name,
                                    p.product_name,
                                    p.category,
                                    o.amount,
                                    hvc.total_spent
                                 FROM high_value_customers hvc
                                 INNER JOIN orders o ON hvc.customer_id = o.customer_id
                                 INNER JOIN top_products tp ON o.product_id = tp.product_id
                                 INNER JOIN products p ON o.product_id = p.product_id
                                 INNER JOIN customers c ON o.customer_id = c.customer_id
                                 WHERE o.amount > 1000
                                 ORDER BY hvc.total_spent DESC",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("nested_cte_with_joins", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_multi_table_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "WITH customer_stats AS (
                                    SELECT
                                        c.customer_id,
                                        c.customer_name,
                                        c.region,
                                        COUNT(o.order_id) as order_count,
                                        SUM(o.amount) as total_amount
                                    FROM customers c
                                    LEFT JOIN orders o ON c.customer_id = o.customer_id
                                    GROUP BY c.customer_id, c.customer_name, c.region
                                 ),
                                 product_performance AS (
                                    SELECT
                                        p.product_id,
                                        p.product_name,
                                        p.category,
                                        COUNT(o.order_id) as times_ordered,
                                        SUM(o.quantity) as total_quantity
                                    FROM products p
                                    LEFT JOIN orders o ON p.product_id = o.product_id
                                    GROUP BY p.product_id, p.product_name, p.category
                                 ),
                                 regional_summary AS (
                                    SELECT
                                        cs.region,
                                        AVG(cs.total_amount) as avg_customer_spend,
                                        COUNT(DISTINCT cs.customer_id) as customer_count
                                    FROM customer_stats cs
                                    GROUP BY cs.region
                                 )
                                 SELECT
                                    cs.customer_name,
                                    cs.region,
                                    cs.order_count,
                                    cs.total_amount,
                                    rs.avg_customer_spend,
                                    pp.product_name,
                                    pp.times_ordered
                                 FROM customer_stats cs
                                 INNER JOIN regional_summary rs ON cs.region = rs.region
                                 INNER JOIN orders o ON cs.customer_id = o.customer_id
                                 INNER JOIN product_performance pp ON o.product_id = pp.product_id
                                 WHERE cs.total_amount > rs.avg_customer_spend
                                 AND pp.times_ordered > 5",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_quadruple_join(c: &mut Criterion) {
    let mut group = c.benchmark_group("quadruple_join");
    configure_standard(&mut group);

    for &size in &[100, 300, 500] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("four_table_join", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_multi_table_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "SELECT
                                    c.customer_name,
                                    p.product_name,
                                    s.supplier_name,
                                    s.country,
                                    o.amount,
                                    o.quantity
                                 FROM customers c
                                 INNER JOIN orders o ON c.customer_id = o.customer_id
                                 INNER JOIN products p ON o.product_id = p.product_id
                                 INNER JOIN suppliers s ON p.product_id = s.product_id
                                 WHERE c.region = 'region_5'
                                 AND s.country = 'country_10'",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_complex_analytics_query(c: &mut Criterion) {
    let mut group = c.benchmark_group("complex_analytics");
    configure_standard(&mut group);

    for &size in &[100, 300, 500] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("full_analytics_pipeline", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_multi_table_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql(
                                "WITH monthly_sales AS (
                                    SELECT
                                        o.customer_id,
                                        o.product_id,
                                        SUM(o.amount) as monthly_total,
                                        COUNT(*) as order_count
                                    FROM orders o
                                    GROUP BY o.customer_id, o.product_id
                                 ),
                                 customer_ranks AS (
                                    SELECT
                                        customer_id,
                                        product_id,
                                        monthly_total,
                                        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_total DESC) as product_rank
                                    FROM monthly_sales
                                 ),
                                 top_customer_products AS (
                                    SELECT customer_id, product_id, monthly_total
                                    FROM customer_ranks
                                    WHERE product_rank <= 3
                                 )
                                 SELECT
                                    c.customer_name,
                                    c.tier,
                                    p.product_name,
                                    p.category,
                                    tcp.monthly_total,
                                    COUNT(DISTINCT s.supplier_id) as supplier_count
                                 FROM top_customer_products tcp
                                 INNER JOIN customers c ON tcp.customer_id = c.customer_id
                                 INNER JOIN products p ON tcp.product_id = p.product_id
                                 LEFT JOIN suppliers s ON p.product_id = s.product_id
                                 GROUP BY c.customer_name, c.tier, p.product_name, p.category, tcp.monthly_total
                                 HAVING COUNT(DISTINCT s.supplier_id) > 0
                                 ORDER BY tcp.monthly_total DESC"
                            )
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

fn bench_recursive_cte_pattern(c: &mut Criterion) {
    let mut group = c.benchmark_group("recursive_cte_pattern");
    configure_standard(&mut group);

    for &size in &[50, 100, 200] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("hierarchical_query", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_employees_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(executor.execute_sql(
                            "WITH manager_hierarchy AS (
                                    SELECT
                                        e1.id,
                                        e1.name as employee_name,
                                        e2.name as manager_name,
                                        e1.salary,
                                        e1.department
                                    FROM employees e1
                                    LEFT JOIN employees e2 ON e1.manager_id = e2.id
                                 ),
                                 dept_summary AS (
                                    SELECT
                                        department,
                                        COUNT(*) as emp_count,
                                        AVG(salary) as avg_salary,
                                        MAX(salary) as max_salary
                                    FROM employees
                                    GROUP BY department
                                 )
                                 SELECT
                                    mh.employee_name,
                                    mh.manager_name,
                                    mh.department,
                                    mh.salary,
                                    ds.avg_salary,
                                    ds.emp_count
                                 FROM manager_hierarchy mh
                                 INNER JOIN dept_summary ds ON mh.department = ds.department
                                 WHERE mh.salary > ds.avg_salary
                                 ORDER BY mh.salary DESC",
                        ))
                        .unwrap();
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn bench_multi_level_aggregation(c: &mut Criterion) {
    let mut group = c.benchmark_group("multi_level_aggregation");
    configure_standard(&mut group);

    for &size in &[100, 300, 500] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("nested_aggregations", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
                        setup_multi_table_data(&mut executor, size);
                        executor
                    },
                    |mut executor| {
                        black_box(
                            executor.execute_sql(
                                "WITH category_totals AS (
                                    SELECT
                                        p.category,
                                        SUM(o.amount) as category_revenue,
                                        COUNT(DISTINCT o.customer_id) as unique_customers
                                    FROM products p
                                    INNER JOIN orders o ON p.product_id = o.product_id
                                    GROUP BY p.category
                                 ),
                                 overall_metrics AS (
                                    SELECT
                                        AVG(category_revenue) as avg_category_revenue,
                                        SUM(category_revenue) as total_revenue
                                    FROM category_totals
                                 )
                                 SELECT
                                    ct.category,
                                    ct.category_revenue,
                                    ct.unique_customers,
                                    om.avg_category_revenue,
                                    (ct.category_revenue * 100.0 / om.total_revenue) as revenue_percentage
                                 FROM category_totals ct
                                 CROSS JOIN overall_metrics om
                                 WHERE ct.category_revenue > om.avg_category_revenue
                                 ORDER BY ct.category_revenue DESC"
                            )
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

criterion_group!(
    benches,
    bench_scalar_subquery,
    bench_correlated_subquery,
    bench_subquery_in_from,
    bench_cte_simple,
    bench_cte_multiple,
    bench_window_functions,
    bench_distinct,
    bench_union,
    bench_intersect_except,
    bench_exists_subquery,
    bench_nested_queries,
    bench_triple_join_with_cte,
    bench_quadruple_join,
    bench_complex_analytics_query,
    bench_recursive_cte_pattern,
    bench_multi_level_aggregation
);
criterion_main!(benches);
