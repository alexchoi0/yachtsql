#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::approx_constant)]

mod common;
use common::*;

#[test]
fn test_basic_insert_string() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS test").unwrap();
    executor
        .execute_sql("CREATE TABLE test (value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('hello')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM test").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 0, 0), "hello");
}

#[test]
fn test_basic_insert_float() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS test").unwrap();
    executor
        .execute_sql("CREATE TABLE test (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES (3.14)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM test").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_float_eq(get_f64(&result, 0, 0), 3.14, 0.001);
}

#[test]
fn test_basic_insert_int() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS test").unwrap();
    executor
        .execute_sql("CREATE TABLE test (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES (42)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM test").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 42);
}

#[test]
fn test_insert_multiple_rows() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS users").unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM users ORDER BY id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_string(&result, 1, 0), "Alice");
    assert_eq!(get_i64(&result, 0, 1), 2);
    assert_eq!(get_string(&result, 1, 1), "Bob");
}

#[test]
fn test_insert_null_values() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
    executor
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (1, 'Widget')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES (NULL, 'Gadget')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY name")
        .unwrap();
    assert_eq!(result.num_rows(), 2);

    assert!(is_null(&result, 0, 0));
    assert_eq!(get_string(&result, 1, 0), "Gadget");

    assert_eq!(get_i64(&result, 0, 1), 1);
    assert_eq!(get_string(&result, 1, 1), "Widget");
}

#[test]
fn test_insert_multi_value_syntax() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data ORDER BY value")
        .unwrap();
    assert_eq!(result.num_rows(), 5);
    for i in 0..5 {
        assert_eq!(get_i64(&result, 0, i), (i + 1) as i64);
    }
}

#[test]
fn test_basic_update() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS users").unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 25)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 30)")
        .unwrap();

    executor
        .execute_sql("UPDATE users SET age = 26 WHERE name = 'Alice'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, age FROM users WHERE name = 'Alice'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 0, 0), "Alice");
    assert_eq!(get_i64(&result, 1, 0), 26);
}

#[test]
fn test_update_all_rows() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS users").unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 25)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 30)")
        .unwrap();

    executor.execute_sql("UPDATE users SET age = 40").unwrap();

    let result = executor.execute_sql("SELECT age FROM users").unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 0, 0), 40);
    assert_eq!(get_i64(&result, 0, 1), 40);
}

#[test]
fn test_update_set_literal() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20)")
        .unwrap();

    executor
        .execute_sql("UPDATE data SET value = 100 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM data ORDER BY id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 1, 0), 100);
    assert_eq!(get_i64(&result, 1, 1), 20);
}

#[test]
fn test_update_multiple_columns() {
    let mut executor = setup_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS products")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, price FLOAT64, stock INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Gadget', 10.00, 100)")
        .unwrap();

    executor
        .execute_sql("UPDATE products SET price = 14.99, stock = 75 WHERE name = 'Gadget'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT price, stock FROM products WHERE name = 'Gadget'")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_float_eq(get_f64(&result, 0, 0), 14.99, 0.001);
    assert_eq!(get_i64(&result, 1, 0), 75);
}

#[test]
fn test_update_with_null() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS users").unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, email STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'bob@example.com')")
        .unwrap();

    executor
        .execute_sql("UPDATE users SET email = 'default@example.com' WHERE email IS NULL")
        .unwrap();

    let result = executor
        .execute_sql("SELECT email FROM users WHERE id = 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 0, 0), "default@example.com");
}

#[test]
fn test_basic_delete() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS users").unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob')")
        .unwrap();

    executor
        .execute_sql("DELETE FROM users WHERE name = 'Bob'")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 1, 0), "Alice");
}

#[test]
fn test_delete_all_rows() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS users").unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob')")
        .unwrap();

    executor.execute_sql("DELETE FROM users").unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_delete_with_like() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS logs").unwrap();
    executor
        .execute_sql("CREATE TABLE logs (id INT64, message STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO logs VALUES (1, 'DEBUG: started')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO logs VALUES (2, 'INFO: running')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO logs VALUES (3, 'DEBUG: finished')")
        .unwrap();

    executor
        .execute_sql("DELETE FROM logs WHERE message LIKE 'DEBUG:%'")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM logs").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 1, 0), "INFO: running");
}

#[test]
fn test_delete_no_matching_rows() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 100), (2, 200)")
        .unwrap();

    executor
        .execute_sql("DELETE FROM data WHERE id > 1000")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM data").unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_insert_select() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS source").unwrap();
    executor.execute_sql("DROP TABLE IF EXISTS target").unwrap();

    executor
        .execute_sql("CREATE TABLE source (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target SELECT * FROM source")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM target ORDER BY id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_string(&result, 1, 0), "Alice");
}

#[test]
fn test_insert_boundary_int64() {
    let mut executor = setup_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS big_numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE big_numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO big_numbers VALUES (9223372036854775807)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO big_numbers VALUES (-9223372036854775808)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM big_numbers ORDER BY value")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 0, 0), i64::MIN);
    assert_eq!(get_i64(&result, 0, 1), i64::MAX);
}

#[test]
fn test_insert_boolean_values() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS flags").unwrap();
    executor
        .execute_sql("CREATE TABLE flags (id INT64, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (1, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO flags VALUES (2, FALSE)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM flags ORDER BY id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert!(get_bool(&result, 1, 0));
    assert!(!get_bool(&result, 1, 1));
}

#[test]
fn test_select_with_in_predicate() {
    let mut executor = setup_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM numbers WHERE value IN (2, 2, 2) ORDER BY value")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 2);
}

#[test]
fn test_select_with_between_null() {
    let mut executor = setup_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (5), (10), (15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM numbers WHERE value BETWEEN NULL AND 20")
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_select_with_like_null() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('hello'), ('world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT word FROM words WHERE word LIKE NULL")
        .unwrap();
    assert_eq!(result.num_rows(), 0);
}

#[test]
fn test_select_with_like_pattern() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('hi'), ('cat'), ('hello'), ('dog')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT word FROM words WHERE word LIKE '___' ORDER BY word")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_string(&result, 0, 0), "cat");
    assert_eq!(get_string(&result, 0, 1), "dog");
}

#[test]
fn test_case_simple() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS test").unwrap();
    executor
        .execute_sql("CREATE TABLE test (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES (5), (-3), (0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val, CASE WHEN val > 0 THEN 'pos' ELSE 'neg' END as sign FROM test ORDER BY val")
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    assert_eq!(get_string(&result, 1, 0), "neg");

    assert_eq!(get_string(&result, 1, 1), "neg");

    assert_eq!(get_string(&result, 1, 2), "pos");
}

#[test]
fn test_case_all_null_result() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS test").unwrap();
    executor
        .execute_sql("CREATE TABLE test (val INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO test VALUES (5)").unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE WHEN val > 20 THEN NULL WHEN val > 10 THEN NULL ELSE NULL END as result FROM test",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert!(is_null(&result, 0, 0));
}

#[test]
fn test_count_after_insert() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
    executor
        .execute_sql("CREATE TABLE items (id INT64)")
        .unwrap();

    for i in 1..=5 {
        executor
            .execute_sql(&format!("INSERT INTO items VALUES ({})", i))
            .unwrap();
    }

    let result = executor.execute_sql("SELECT COUNT(*) FROM items").unwrap();
    assert_eq!(get_i64(&result, 0, 0), 5);
}

#[test]
fn test_sum_after_insert() {
    let mut executor = setup_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS amounts")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE amounts (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO amounts VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(value) FROM amounts")
        .unwrap();

    let sum = result
        .column(0)
        .unwrap()
        .get(0)
        .unwrap()
        .as_numeric()
        .map(|n| n.try_into().unwrap())
        .unwrap_or_else(|| result.column(0).unwrap().get(0).unwrap().as_i64().unwrap());
    assert_eq!(sum, 60i64);
}

#[test]
fn test_select_order_by() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (3, 'Charlie'), (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data ORDER BY name")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_string(&result, 1, 0), "Alice");
    assert_eq!(get_string(&result, 1, 1), "Bob");
    assert_eq!(get_string(&result, 1, 2), "Charlie");
}

#[test]
fn test_select_limit() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM data ORDER BY value LIMIT 3")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_i64(&result, 0, 1), 2);
    assert_eq!(get_i64(&result, 0, 2), 3);
}

#[test]
fn test_select_min_max() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (30), (10), (20)")
        .unwrap();

    let result = executor.execute_sql("SELECT MIN(value) FROM data").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 10);

    let result = executor.execute_sql("SELECT MAX(value) FROM data").unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_i64(&result, 0, 0), 30);
}

#[test]
fn test_insert_and_select_nulls() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, NULL), (2, 20), (3, NULL), (4, 40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM data WHERE value IS NULL")
        .unwrap();
    assert_eq!(get_i64(&result, 0, 0), 2);

    let result = executor
        .execute_sql("SELECT COUNT(value) FROM data")
        .unwrap();
    assert_eq!(get_i64(&result, 0, 0), 2);
}

#[test]
fn test_insert_with_expressions() {
    let mut executor = setup_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS computed")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE computed (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO computed VALUES (1 + 2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO computed VALUES (10 * 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM computed ORDER BY value")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_i64(&result, 0, 0), 3);
    assert_eq!(get_i64(&result, 0, 1), 50);
}

#[test]
fn test_select_group_by_after_insert() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS sales").unwrap();
    executor
        .execute_sql("CREATE TABLE sales (category STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sales VALUES ('A', 100), ('A', 150), ('B', 200), ('B', 120), ('C', 300)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(amount) as total FROM sales GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_string(&result, 0, 0), "A");
    assert_eq!(get_string(&result, 0, 1), "B");
    assert_eq!(get_string(&result, 0, 2), "C");
}

#[test]
fn test_delete_with_specific_value() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS orders").unwrap();

    executor
        .execute_sql("CREATE TABLE orders (id INT64, customer_id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO orders VALUES (100, 1), (101, 2), (102, 1)")
        .unwrap();

    executor
        .execute_sql("DELETE FROM orders WHERE customer_id = 2")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM orders").unwrap();
    assert_eq!(result.num_rows(), 2);

    assert_eq!(get_i64(&result, 1, 0), 1);
    assert_eq!(get_i64(&result, 1, 1), 1);
}

#[test]
fn test_select_distinct_after_insert() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
    executor
        .execute_sql("CREATE TABLE data (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1), (2), (1), (3), (2), (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT value FROM data ORDER BY value")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
    assert_eq!(get_i64(&result, 0, 0), 1);
    assert_eq!(get_i64(&result, 0, 1), 2);
    assert_eq!(get_i64(&result, 0, 2), 3);
}

#[test]
fn test_join_after_insert() {
    let mut executor = setup_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS user_orders")
        .unwrap();
    executor
        .execute_sql("DROP TABLE IF EXISTS order_users")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE order_users (user_id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE user_orders (order_id INT64, user_id INT64, product STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO order_users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO user_orders VALUES (100, 1, 'Widget'), (101, 2, 'Gadget'), (102, 1, 'Gizmo')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT order_users.name, user_orders.product FROM order_users JOIN user_orders ON order_users.user_id = user_orders.user_id ORDER BY user_orders.product",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 3);

    assert_eq!(get_string(&result, 0, 0), "Bob");
    assert_eq!(get_string(&result, 1, 0), "Gadget");
}

#[test]
fn test_insert_unicode() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('hello世界'), ('world'), ('你好world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT word FROM words ORDER BY word")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_select_like_unicode() {
    let mut executor = setup_executor();
    executor.execute_sql("DROP TABLE IF EXISTS words").unwrap();
    executor
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO words VALUES ('hello世界'), ('world'), ('你好world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT word FROM words WHERE word LIKE '%世界%' ORDER BY word")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, 0, 0), "hello世界");
}
