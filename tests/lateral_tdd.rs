mod common;

use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

mod basic_lateral {
    use super::*;

    #[test]
    fn test_cross_join_lateral_basic() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS departments")
            .unwrap();
        executor
            .execute_sql("DROP TABLE IF EXISTS employees")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE departments (dept_id INT64, dept_name STRING)")
            .unwrap();
        executor
            .execute_sql(
                "CREATE TABLE employees (emp_id INT64, dept_id INT64, name STRING, salary INT64)",
            )
            .unwrap();

        executor
            .execute_sql("INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales')")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO employees VALUES
                (1, 1, 'Alice', 100000),
                (2, 1, 'Bob', 90000),
                (3, 1, 'Charlie', 95000),
                (4, 2, 'David', 80000),
                (5, 2, 'Eve', 85000)",
            )
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT d.dept_name, e.name, e.salary
                FROM departments d
                CROSS JOIN LATERAL (
                    SELECT name, salary
                    FROM employees
                    WHERE dept_id = d.dept_id
                    ORDER BY salary DESC
                    LIMIT 2
                ) e
                ORDER BY d.dept_name, e.salary DESC",
            )
            .expect("CROSS JOIN LATERAL should succeed");

        assert_eq!(
            result.num_rows(),
            4,
            "Should return 4 rows (2 per department)"
        );
    }

    #[test]
    fn test_left_join_lateral_with_nulls() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS depts").unwrap();
        executor.execute_sql("DROP TABLE IF EXISTS emps").unwrap();
        executor
            .execute_sql("CREATE TABLE depts (dept_id INT64, dept_name STRING)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE emps (emp_id INT64, dept_id INT64, name STRING)")
            .unwrap();

        executor
            .execute_sql("INSERT INTO depts VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR')")
            .unwrap();
        executor
            .execute_sql("INSERT INTO emps VALUES (1, 1, 'Alice'), (2, 1, 'Bob')")
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT d.dept_name, e.name
                FROM depts d
                LEFT JOIN LATERAL (
                    SELECT name
                    FROM emps
                    WHERE dept_id = d.dept_id
                    LIMIT 1
                ) e ON true
                ORDER BY d.dept_name",
            )
            .expect("LEFT JOIN LATERAL should succeed");

        assert_eq!(
            result.num_rows(),
            3,
            "Should return 3 rows (one per department)"
        );
    }

    #[test]
    fn test_lateral_without_correlation() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS t1").unwrap();
        executor.execute_sql("DROP TABLE IF EXISTS t2").unwrap();
        executor.execute_sql("CREATE TABLE t1 (a INT64)").unwrap();
        executor.execute_sql("CREATE TABLE t2 (b INT64)").unwrap();

        executor
            .execute_sql("INSERT INTO t1 VALUES (1), (2)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO t2 VALUES (10), (20)")
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT t1.a, x.b
                FROM t1
                CROSS JOIN LATERAL (
                    SELECT b FROM t2
                ) x
                ORDER BY t1.a, x.b",
            )
            .expect("LATERAL without correlation should succeed");

        assert_eq!(
            result.num_rows(),
            4,
            "Should return 4 rows (2 x 2 cartesian product)"
        );
    }
}

mod apply_syntax {
    use super::*;

    #[test]
    fn test_cross_apply() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
        executor
            .execute_sql("DROP TABLE IF EXISTS order_items")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE orders (order_id INT64, customer_id INT64)")
            .unwrap();
        executor
            .execute_sql(
                "CREATE TABLE order_items (order_id INT64, item_name STRING, price FLOAT64)",
            )
            .unwrap();

        executor
            .execute_sql("INSERT INTO orders VALUES (1, 100), (2, 100), (3, 101)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO order_items VALUES
                (1, 'Widget', 10.0),
                (1, 'Gadget', 20.0),
                (2, 'Gizmo', 15.0)",
            )
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT o.order_id, items.item_name, items.price
                FROM orders o
                CROSS APPLY (
                    SELECT item_name, price
                    FROM order_items
                    WHERE order_id = o.order_id
                ) items
                ORDER BY o.order_id, items.item_name",
            )
            .expect("CROSS APPLY should succeed");

        assert_eq!(
            result.num_rows(),
            3,
            "Should return 3 rows (order 3 excluded)"
        );
    }

    #[test]
    fn test_outer_apply() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS orders_all")
            .unwrap();
        executor
            .execute_sql("DROP TABLE IF EXISTS items_some")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE orders_all (order_id INT64)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE items_some (order_id INT64, item_name STRING)")
            .unwrap();

        executor
            .execute_sql("INSERT INTO orders_all VALUES (1), (2), (3)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO items_some VALUES (1, 'Widget'), (2, 'Gadget')")
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT o.order_id, items.item_name
                FROM orders_all o
                OUTER APPLY (
                    SELECT item_name
                    FROM items_some
                    WHERE order_id = o.order_id
                ) items
                ORDER BY o.order_id",
            )
            .expect("OUTER APPLY should succeed");

        assert_eq!(
            result.num_rows(),
            3,
            "Should return 3 rows (order 3 has NULL item)"
        );
    }
}

mod lateral_aggregation {
    use super::*;

    #[test]
    fn test_lateral_with_aggregate() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS dept_list")
            .unwrap();
        executor
            .execute_sql("DROP TABLE IF EXISTS emp_list")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE dept_list (dept_id INT64, dept_name STRING)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE emp_list (dept_id INT64, salary INT64)")
            .unwrap();

        executor
            .execute_sql("INSERT INTO dept_list VALUES (1, 'Engineering'), (2, 'Sales')")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO emp_list VALUES
                (1, 100000), (1, 90000), (1, 95000),
                (2, 80000), (2, 85000)",
            )
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT d.dept_name, stats.avg_salary, stats.total_salary
                FROM dept_list d
                CROSS JOIN LATERAL (
                    SELECT AVG(salary) as avg_salary, SUM(salary) as total_salary
                    FROM emp_list
                    WHERE dept_id = d.dept_id
                ) stats
                ORDER BY d.dept_name",
            )
            .expect("LATERAL with aggregation should succeed");

        assert_eq!(
            result.num_rows(),
            2,
            "Should return 2 rows (one per department)"
        );
    }
}

mod multiple_lateral {
    use super::*;

    #[test]
    fn test_chained_lateral_joins() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS t1").unwrap();
        executor.execute_sql("DROP TABLE IF EXISTS t2").unwrap();
        executor.execute_sql("DROP TABLE IF EXISTS t3").unwrap();
        executor.execute_sql("CREATE TABLE t1 (a INT64)").unwrap();
        executor
            .execute_sql("CREATE TABLE t2 (a INT64, b INT64)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE t3 (a INT64, b INT64, c INT64)")
            .unwrap();

        executor
            .execute_sql("INSERT INTO t1 VALUES (1), (2)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO t2 VALUES (1, 10), (2, 20)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO t3 VALUES (1, 10, 100), (2, 20, 200)")
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT t1.a, x.b, y.c
                FROM t1
                CROSS JOIN LATERAL (
                    SELECT b FROM t2 WHERE t2.a = t1.a
                ) x
                CROSS JOIN LATERAL (
                    SELECT c FROM t3 WHERE t3.a = t1.a AND t3.b = x.b
                ) y
                ORDER BY t1.a",
            )
            .expect("Chained LATERAL joins should succeed");

        assert_eq!(result.num_rows(), 2, "Should return 2 rows");
    }
}

mod null_handling {
    use super::*;

    #[test]
    fn test_lateral_null_correlation() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS outer_table")
            .unwrap();
        executor
            .execute_sql("DROP TABLE IF EXISTS inner_table")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE outer_table (id INT64, ref_id INT64)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE inner_table (ref_id INT64, data STRING)")
            .unwrap();

        executor
            .execute_sql("INSERT INTO outer_table VALUES (1, 100), (2, NULL), (3, 200)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO inner_table VALUES (100, 'A'), (200, 'B')")
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT o.id, i.data
                FROM outer_table o
                CROSS JOIN LATERAL (
                    SELECT data FROM inner_table WHERE ref_id = o.ref_id
                ) i
                ORDER BY o.id",
            )
            .expect("LATERAL with NULL correlation should succeed");

        assert_eq!(
            result.num_rows(),
            2,
            "Should return 2 rows (NULL doesn't match)"
        );
    }
}

mod integration {
    use super::*;

    #[test]
    fn test_lateral_with_subquery_from() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS order_data")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE order_data (customer_id INT64, amount FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO order_data VALUES (1, 100), (1, 200), (2, 150)")
            .unwrap();

        let result = executor
            .execute_sql(
                "SELECT ct.customer_id, ct.total, recent.amount
                FROM (
                    SELECT customer_id, SUM(amount) as total
                    FROM order_data
                    GROUP BY customer_id
                ) ct
                CROSS JOIN LATERAL (
                    SELECT amount
                    FROM order_data
                    WHERE customer_id = ct.customer_id
                    ORDER BY amount DESC
                    LIMIT 1
                ) recent
                ORDER BY ct.customer_id",
            )
            .expect("LATERAL with derived table should succeed");

        assert_eq!(
            result.num_rows(),
            2,
            "Should return 2 rows (one per customer)"
        );
    }
}

mod parser_tests {
    use super::*;

    #[test]
    fn test_lateral_keyword_parsing() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_t1")
            .unwrap();
        executor
            .execute_sql("DROP TABLE IF EXISTS test_t2")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_t1 (id INT64)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_t2 (id INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_t1 VALUES (1)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_t2 VALUES (1)")
            .unwrap();

        let queries = vec![
            "SELECT * FROM test_t1 CROSS JOIN LATERAL (SELECT * FROM test_t2) x",
            "SELECT * FROM test_t1 LEFT JOIN LATERAL (SELECT * FROM test_t2) x ON true",
            "SELECT * FROM test_t1 CROSS APPLY (SELECT * FROM test_t2) x",
            "SELECT * FROM test_t1 OUTER APPLY (SELECT * FROM test_t2) x",
        ];

        for query in queries {
            let result = executor.execute_sql(query);
            assert!(result.is_ok(), "Query should parse and execute: {}", query);
        }
    }
}
