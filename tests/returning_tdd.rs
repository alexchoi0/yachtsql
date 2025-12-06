mod common;

use common::{get_f64, get_i64, get_string, is_null};
use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

mod insert_returning {
    use super::*;

    #[test]
    fn test_insert_returning_star() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING, value FLOAT64)")
            .unwrap();

        let result = executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'Alice', 100.5) RETURNING *")
            .expect("INSERT RETURNING * should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 3, "Should have 3 columns");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_f64(&result, 2, 0), 100.5);
    }

    #[test]
    fn test_insert_returning_specific_columns() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING, value FLOAT64)")
            .unwrap();

        let result = executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'Alice', 100.5) RETURNING id, name")
            .expect("INSERT RETURNING id, name should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 2, "Should have 2 columns");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
    }

    #[test]
    fn test_insert_returning_with_alias() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING)")
            .unwrap();

        let result = executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'Alice') RETURNING id AS user_id, name AS user_name")
            .expect("INSERT RETURNING with aliases should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 2, "Should have 2 columns");

        let schema = result.schema();
        assert_eq!(schema.fields()[0].name, "user_id");
        assert_eq!(schema.fields()[1].name, "user_name");
    }

    #[test]
    fn test_insert_returning_expression() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, value FLOAT64)")
            .unwrap();

        let result = executor
            .execute_sql(
                "INSERT INTO test_ret VALUES (1, 100.0) RETURNING id, value * 2 AS doubled",
            )
            .expect("INSERT RETURNING with expression should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 2, "Should have 2 columns");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 200.0);
    }

    #[test]
    fn test_insert_returning_expression_no_alias() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, value FLOAT64)")
            .unwrap();

        let result = executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 100.0) RETURNING value + 50")
            .expect("INSERT RETURNING with expression (no alias) should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(get_f64(&result, 0, 0), 150.0);
    }

    #[test]
    fn test_insert_multiple_rows_returning() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING)")
            .unwrap();

        let result = executor
            .execute_sql(
                "INSERT INTO test_ret VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie') RETURNING *",
            )
            .expect("INSERT multiple rows RETURNING should succeed");

        assert_eq!(result.num_rows(), 3, "Should return 3 rows");
    }
}

mod update_returning {
    use super::*;

    #[test]
    fn test_update_returning_star() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'Alice', 100.0)")
            .unwrap();

        let result = executor
            .execute_sql("UPDATE test_ret SET value = 200.0 WHERE id = 1 RETURNING *")
            .expect("UPDATE RETURNING * should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 3, "Should have 3 columns");
        assert_eq!(get_f64(&result, 2, 0), 200.0);
    }

    #[test]
    fn test_update_returning_specific_columns() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'Alice', 100.0)")
            .unwrap();

        let result = executor
            .execute_sql("UPDATE test_ret SET value = 200.0 WHERE id = 1 RETURNING id, value")
            .expect("UPDATE RETURNING specific columns should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 2, "Should have 2 columns");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 200.0);
    }

    #[test]
    fn test_update_returning_expression() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 100.0)")
            .unwrap();

        let result = executor
            .execute_sql(
                "UPDATE test_ret SET value = 200.0 WHERE id = 1 RETURNING id, value * 2 AS doubled",
            )
            .expect("UPDATE RETURNING with expression should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 400.0);
    }

    #[test]
    fn test_update_multiple_rows_returning() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, category STRING, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql(
                "INSERT INTO test_ret VALUES (1, 'A', 100.0), (2, 'A', 200.0), (3, 'B', 300.0)",
            )
            .unwrap();

        let result = executor
            .execute_sql("UPDATE test_ret SET value = value + 10 WHERE category = 'A' RETURNING *")
            .expect("UPDATE multiple rows RETURNING should succeed");

        assert_eq!(result.num_rows(), 2, "Should return 2 rows");
    }

    #[test]
    fn test_update_no_matching_rows_returning() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 100.0)")
            .unwrap();

        let result = executor
            .execute_sql("UPDATE test_ret SET value = 200.0 WHERE id = 999 RETURNING *")
            .expect("UPDATE with no matching rows RETURNING should succeed");

        assert_eq!(result.num_rows(), 0, "Should return 0 rows when no matches");
    }
}

mod delete_returning {
    use super::*;

    #[test]
    fn test_delete_returning_star() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'Alice', 100.0)")
            .unwrap();

        let result = executor
            .execute_sql("DELETE FROM test_ret WHERE id = 1 RETURNING *")
            .expect("DELETE RETURNING * should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 3, "Should have 3 columns");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
        assert_eq!(get_f64(&result, 2, 0), 100.0);
    }

    #[test]
    fn test_delete_returning_specific_columns() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'Alice', 100.0)")
            .unwrap();

        let result = executor
            .execute_sql("DELETE FROM test_ret WHERE id = 1 RETURNING id, name")
            .expect("DELETE RETURNING specific columns should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(result.num_columns(), 2, "Should have 2 columns");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Alice");
    }

    #[test]
    fn test_delete_returning_expression() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 100.0)")
            .unwrap();

        let result = executor
            .execute_sql("DELETE FROM test_ret WHERE id = 1 RETURNING id, value * 2 AS doubled")
            .expect("DELETE RETURNING with expression should succeed");

        assert_eq!(result.num_rows(), 1, "Should return 1 row");
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_f64(&result, 1, 0), 200.0);
    }

    #[test]
    fn test_delete_multiple_rows_returning() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, category STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'A'), (2, 'A'), (3, 'B')")
            .unwrap();

        let result = executor
            .execute_sql("DELETE FROM test_ret WHERE category = 'A' RETURNING *")
            .expect("DELETE multiple rows RETURNING should succeed");

        assert_eq!(result.num_rows(), 2, "Should return 2 rows");
    }

    #[test]
    fn test_delete_no_matching_rows_returning() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, value FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 100.0)")
            .unwrap();

        let result = executor
            .execute_sql("DELETE FROM test_ret WHERE id = 999 RETURNING *")
            .expect("DELETE with no matching rows RETURNING should succeed");

        assert_eq!(result.num_rows(), 0, "Should return 0 rows when no matches");
    }

    #[test]
    fn test_delete_using_returning() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();
        executor
            .execute_sql("DROP TABLE IF EXISTS customers")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE customers (id INT64, name STRING)")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64, amount FLOAT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')")
            .unwrap();
        executor
            .execute_sql("INSERT INTO orders VALUES (1, 1, 100.0), (2, 1, 200.0), (3, 2, 300.0)")
            .unwrap();

        let result = executor
            .execute_sql(
                "DELETE FROM orders USING customers
                 WHERE orders.customer_id = customers.id AND customers.name = 'Alice'
                 RETURNING *",
            )
            .expect("DELETE USING with RETURNING should succeed");

        assert_eq!(result.num_rows(), 2, "Should return 2 deleted orders");

        let remaining = executor
            .execute_sql("SELECT * FROM orders")
            .expect("SELECT should succeed");
        assert_eq!(remaining.num_rows(), 1, "Only Bob's order should remain");
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn test_returning_null_values() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING)")
            .unwrap();

        let result = executor
            .execute_sql("INSERT INTO test_ret VALUES (1, NULL) RETURNING *")
            .expect("INSERT with NULL RETURNING should succeed");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert!(is_null(&result, 1, 0));
    }

    #[test]
    fn test_returning_with_function_expression() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, name STRING)")
            .unwrap();

        let result = executor
            .execute_sql(
                "INSERT INTO test_ret VALUES (1, 'hello') RETURNING id, UPPER(name) AS upper_name",
            )
            .expect("INSERT with function expression RETURNING should succeed");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "HELLO");
    }

    #[test]
    fn test_returning_with_concat_expression() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_ret")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_ret (id INT64, first_name STRING, last_name STRING)")
            .unwrap();

        let result = executor
            .execute_sql("INSERT INTO test_ret VALUES (1, 'John', 'Doe') RETURNING id, first_name || ' ' || last_name AS full_name")
            .expect("INSERT with concat expression RETURNING should succeed");

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "John Doe");
    }
}
