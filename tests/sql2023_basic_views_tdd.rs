#![allow(dead_code)]
#![allow(unused_variables)]

mod common;
use common::{
    assert_row_count, exec_ok, get_f64_by_name, get_i64_by_name, get_string_by_name, new_executor,
    query,
};
use yachtsql::QueryExecutor;

fn setup_users_table() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS users");
    exec_ok(
        &mut executor,
        "CREATE TABLE users (
            id INT64,
            name STRING,
            age INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO users VALUES (1, 'Alice', 30), (2, 'Bob', 25), (3, 'Charlie', 35)",
    );

    executor
}

fn setup_equipment_table() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS equipment");
    exec_ok(
        &mut executor,
        "CREATE TABLE equipment (
            id INT64,
            name STRING,
            price FLOAT64,
            quantity INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO equipment VALUES
            (1, 'Widget', 10.50, 100),
            (2, 'Gadget', 25.00, 50)",
    );

    executor
}

fn setup_fleet_crew_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS fleets");
    exec_ok(
        &mut executor,
        "CREATE TABLE fleets (
            id INT64,
            name STRING
        )",
    );

    exec_ok(&mut executor, "DROP TABLE IF EXISTS crew_members");
    exec_ok(
        &mut executor,
        "CREATE TABLE crew_members (
            id INT64,
            name STRING,
            dept_id INT64,
            salary INT64
        )",
    );

    exec_ok(
        &mut executor,
        "INSERT INTO fleets VALUES (1, 'Engineering'), (2, 'Sales')",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO crew_members VALUES
            (1, 'Alice', 1, 80000),
            (2, 'Bob', 2, 60000),
            (3, 'Charlie', 1, 90000)",
    );

    executor
}

fn setup_sales_table() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS sales");
    exec_ok(
        &mut executor,
        "CREATE TABLE sales (
            id INT64,
            product STRING,
            amount FLOAT64,
            region STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO sales VALUES
            (1, 'Widget', 100, 'North'),
            (2, 'Widget', 150, 'South'),
            (3, 'Gadget', 200, 'North')",
    );

    executor
}

fn setup_orders_table() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS orders");
    exec_ok(
        &mut executor,
        "CREATE TABLE orders (
            id INT64,
            owner_id INT64,
            total FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO orders VALUES
            (1, 1, 100),
            (2, 1, 200),
            (3, 2, 150),
            (4, 2, 175)",
    );

    executor
}

#[test]
fn test_create_simple_view() {
    let mut executor = setup_users_table();

    exec_ok(&mut executor, "DROP VIEW IF EXISTS adult_users");
    exec_ok(
        &mut executor,
        "CREATE VIEW adult_users AS SELECT * FROM users WHERE age >= 18",
    );

    let result = query(&mut executor, "SELECT * FROM adult_users ORDER BY id");

    assert_row_count(&result, 3);

    assert_eq!(get_i64_by_name(&result, 0, "id"), 1);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Alice");
    assert_eq!(get_i64_by_name(&result, 0, "age"), 30);

    assert_eq!(get_i64_by_name(&result, 1, "id"), 2);
    assert_eq!(get_string_by_name(&result, 1, "name"), "Bob");
    assert_eq!(get_i64_by_name(&result, 1, "age"), 25);

    assert_eq!(get_i64_by_name(&result, 2, "id"), 3);
    assert_eq!(get_string_by_name(&result, 2, "name"), "Charlie");
    assert_eq!(get_i64_by_name(&result, 2, "age"), 35);
}

#[test]
fn test_view_with_computed_columns() {
    let mut executor = setup_equipment_table();

    exec_ok(&mut executor, "DROP VIEW IF EXISTS product_inventory");
    exec_ok(
        &mut executor,
        "CREATE VIEW product_inventory AS
        SELECT
            id,
            name,
            price,
            quantity,
            price * quantity AS total_value
        FROM equipment",
    );

    let result = query(&mut executor, "SELECT * FROM product_inventory ORDER BY id");

    assert_row_count(&result, 2);

    assert_eq!(get_i64_by_name(&result, 0, "id"), 1);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Widget");
    assert!((get_f64_by_name(&result, 0, "price") - 10.50).abs() < 0.01);
    assert_eq!(get_i64_by_name(&result, 0, "quantity"), 100);
    assert!((get_f64_by_name(&result, 0, "total_value") - 1050.0).abs() < 0.01);

    assert_eq!(get_i64_by_name(&result, 1, "id"), 2);
    assert_eq!(get_string_by_name(&result, 1, "name"), "Gadget");
    assert!((get_f64_by_name(&result, 1, "price") - 25.00).abs() < 0.01);
    assert_eq!(get_i64_by_name(&result, 1, "quantity"), 50);
    assert!((get_f64_by_name(&result, 1, "total_value") - 1250.0).abs() < 0.01);
}

#[test]
fn test_view_with_join() {
    let mut executor = setup_fleet_crew_tables();

    exec_ok(&mut executor, "DROP VIEW IF EXISTS crew_member_details");
    exec_ok(
        &mut executor,
        "CREATE VIEW crew_member_details AS
        SELECT
            e.id AS crew_id,
            e.name AS crew_name,
            d.name AS fleet_name,
            e.salary AS crew_salary
        FROM crew_members e
        JOIN fleets d ON e.dept_id = d.id",
    );

    let result = query(
        &mut executor,
        "SELECT * FROM crew_member_details ORDER BY crew_id",
    );

    assert_row_count(&result, 3);

    let num_cols = result.schema().fields().len();
    assert_eq!(num_cols, 4, "View should have 4 columns");

    let mut all_crew_names: Vec<String> = Vec::new();
    for col_idx in 0..num_cols {
        let col = result.column(col_idx).unwrap();
        for row_idx in 0..result.num_rows() {
            let val = col.get(row_idx).unwrap();
            if let Some(s) = val.as_str()
                && matches!(s, "Alice" | "Bob" | "Charlie")
            {
                all_crew_names.push(s.to_string());
            }
        }
    }

    assert!(
        all_crew_names.contains(&"Alice".to_string()),
        "Should have Alice"
    );
    assert!(
        all_crew_names.contains(&"Bob".to_string()),
        "Should have Bob"
    );
    assert!(
        all_crew_names.contains(&"Charlie".to_string()),
        "Should have Charlie"
    );

    let mut all_salaries: Vec<i64> = Vec::new();
    for col_idx in 0..num_cols {
        let col = result.column(col_idx).unwrap();
        for row_idx in 0..result.num_rows() {
            let val = col.get(row_idx).unwrap();
            if let Some(v) = val.as_i64()
                && matches!(v, 80000 | 60000 | 90000)
            {
                all_salaries.push(v);
            }
        }
    }
    assert!(all_salaries.contains(&80000), "Should have salary 80000");
    assert!(all_salaries.contains(&60000), "Should have salary 60000");
    assert!(all_salaries.contains(&90000), "Should have salary 90000");
}

#[test]
fn test_view_with_aggregation() {
    let mut executor = setup_sales_table();

    exec_ok(&mut executor, "DROP VIEW IF EXISTS sales_by_region");
    exec_ok(
        &mut executor,
        "CREATE VIEW sales_by_region AS
        SELECT
            region,
            SUM(amount) AS total_sales,
            COUNT(*) AS sale_count
        FROM sales
        GROUP BY region",
    );

    let result = query(
        &mut executor,
        "SELECT * FROM sales_by_region ORDER BY region",
    );

    assert_row_count(&result, 2);

    assert_eq!(get_string_by_name(&result, 0, "region"), "North");
    assert!((get_f64_by_name(&result, 0, "total_sales") - 300.0).abs() < 0.01);
    assert_eq!(get_i64_by_name(&result, 0, "sale_count"), 2);

    assert_eq!(get_string_by_name(&result, 1, "region"), "South");
    assert!((get_f64_by_name(&result, 1, "total_sales") - 150.0).abs() < 0.01);
    assert_eq!(get_i64_by_name(&result, 1, "sale_count"), 1);
}

#[test]
fn test_view_with_aggregation_subquery() {
    let mut executor = setup_orders_table();

    exec_ok(&mut executor, "DROP VIEW IF EXISTS customer_order_stats");
    exec_ok(
        &mut executor,
        "CREATE VIEW customer_order_stats AS
        SELECT
            owner_id,
            COUNT(*) AS order_count,
            SUM(total) AS total_spent
        FROM orders
        GROUP BY owner_id",
    );

    let result = query(
        &mut executor,
        "SELECT * FROM customer_order_stats ORDER BY owner_id",
    );

    assert_row_count(&result, 2);

    assert_eq!(get_i64_by_name(&result, 0, "owner_id"), 1);
    assert_eq!(get_i64_by_name(&result, 0, "order_count"), 2);
    assert!((get_f64_by_name(&result, 0, "total_spent") - 300.0).abs() < 0.01);

    assert_eq!(get_i64_by_name(&result, 1, "owner_id"), 2);
    assert_eq!(get_i64_by_name(&result, 1, "order_count"), 2);
    assert!((get_f64_by_name(&result, 1, "total_spent") - 325.0).abs() < 0.01);
}

#[test]
fn test_view_with_where_clause() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS items");
    exec_ok(
        &mut executor,
        "CREATE TABLE items (
            id INT64,
            name STRING,
            category STRING,
            price FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO items VALUES
            (1, 'Item A', 'Electronics', 100.0),
            (2, 'Item B', 'Books', 20.0),
            (3, 'Item C', 'Electronics', 150.0)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS electronics");
    exec_ok(
        &mut executor,
        "CREATE VIEW electronics AS SELECT * FROM items WHERE category = 'Electronics'",
    );

    let result = query(&mut executor, "SELECT * FROM electronics ORDER BY id");

    assert_row_count(&result, 2);

    assert_eq!(get_i64_by_name(&result, 0, "id"), 1);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Item A");
    assert_eq!(get_string_by_name(&result, 0, "category"), "Electronics");

    assert_eq!(get_i64_by_name(&result, 1, "id"), 3);
    assert_eq!(get_string_by_name(&result, 1, "name"), "Item C");
    assert_eq!(get_string_by_name(&result, 1, "category"), "Electronics");
}

#[test]
fn test_view_with_order_by_in_query() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS scores");
    exec_ok(
        &mut executor,
        "CREATE TABLE scores (
            id INT64,
            name STRING,
            score INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO scores VALUES (1, 'Alice', 85), (2, 'Bob', 92), (3, 'Charlie', 78)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS top_scores");
    exec_ok(
        &mut executor,
        "CREATE VIEW top_scores AS SELECT * FROM scores WHERE score >= 80",
    );

    let result = query(
        &mut executor,
        "SELECT * FROM top_scores ORDER BY score DESC",
    );

    assert_row_count(&result, 2);

    assert_eq!(get_i64_by_name(&result, 0, "id"), 2);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Bob");
    assert_eq!(get_i64_by_name(&result, 0, "score"), 92);

    assert_eq!(get_i64_by_name(&result, 1, "id"), 1);
    assert_eq!(get_string_by_name(&result, 1, "name"), "Alice");
    assert_eq!(get_i64_by_name(&result, 1, "score"), 85);
}

#[test]
fn test_create_or_replace_view() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS data");
    exec_ok(
        &mut executor,
        "CREATE TABLE data (
            id INT64,
            value INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS data_view");
    exec_ok(
        &mut executor,
        "CREATE VIEW data_view AS SELECT * FROM data WHERE value > 10",
    );

    let result = query(&mut executor, "SELECT COUNT(*) AS cnt FROM data_view");
    assert_eq!(get_i64_by_name(&result, 0, "cnt"), 2);

    exec_ok(
        &mut executor,
        "CREATE OR REPLACE VIEW data_view AS SELECT * FROM data WHERE value > 20",
    );

    let result = query(&mut executor, "SELECT COUNT(*) AS cnt FROM data_view");
    assert_eq!(get_i64_by_name(&result, 0, "cnt"), 1);
}

#[test]
fn test_create_or_replace_view_change_columns() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS items_replace");
    exec_ok(
        &mut executor,
        "CREATE TABLE items_replace (
            id INT64,
            name STRING,
            price FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO items_replace VALUES (1, 'Item1', 10.0), (2, 'Item2', 20.0)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS item_view");
    exec_ok(
        &mut executor,
        "CREATE VIEW item_view AS SELECT id, name FROM items_replace",
    );

    let result = query(&mut executor, "SELECT * FROM item_view ORDER BY id");
    assert_row_count(&result, 2);
    assert_eq!(get_i64_by_name(&result, 0, "id"), 1);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Item1");

    exec_ok(
        &mut executor,
        "CREATE OR REPLACE VIEW item_view AS SELECT id, price FROM items_replace",
    );

    let result = query(&mut executor, "SELECT * FROM item_view ORDER BY id");
    assert_row_count(&result, 2);
    assert_eq!(get_i64_by_name(&result, 0, "id"), 1);
    assert!((get_f64_by_name(&result, 0, "price") - 10.0).abs() < 0.01);
}

#[test]
fn test_view_on_view() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS base_data");
    exec_ok(
        &mut executor,
        "CREATE TABLE base_data (
            id INT64,
            category STRING,
            value INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO base_data VALUES
            (1, 'A', 100),
            (2, 'A', 150),
            (3, 'B', 200)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS category_a");
    exec_ok(
        &mut executor,
        "CREATE VIEW category_a AS SELECT * FROM base_data WHERE category = 'A'",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS high_value_a");
    exec_ok(
        &mut executor,
        "CREATE VIEW high_value_a AS SELECT * FROM category_a WHERE value > 120",
    );

    let result = query(&mut executor, "SELECT * FROM high_value_a");

    assert_row_count(&result, 1);
    assert_eq!(get_i64_by_name(&result, 0, "id"), 2);
    assert_eq!(get_string_by_name(&result, 0, "category"), "A");
    assert_eq!(get_i64_by_name(&result, 0, "value"), 150);
}

#[test]
fn test_drop_view() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS temp_table");
    exec_ok(&mut executor, "CREATE TABLE temp_table (id INT64)");
    exec_ok(&mut executor, "DROP VIEW IF EXISTS temp_view");
    exec_ok(
        &mut executor,
        "CREATE VIEW temp_view AS SELECT * FROM temp_table",
    );

    let result = query(&mut executor, "SELECT * FROM temp_view");
    assert_row_count(&result, 0);

    exec_ok(&mut executor, "DROP VIEW temp_view");

    let result = executor.execute_sql("SELECT * FROM temp_view");
    assert!(result.is_err());
}

#[test]
fn test_drop_view_if_exists() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP VIEW IF EXISTS nonexistent_view");

    exec_ok(&mut executor, "DROP TABLE IF EXISTS test_table");
    exec_ok(&mut executor, "CREATE TABLE test_table (id INT64)");
    exec_ok(&mut executor, "DROP VIEW IF EXISTS test_view");
    exec_ok(
        &mut executor,
        "CREATE VIEW test_view AS SELECT * FROM test_table",
    );
    exec_ok(&mut executor, "DROP VIEW IF EXISTS test_view");

    let result = executor.execute_sql("SELECT * FROM test_view");
    assert!(result.is_err());
}

#[test]
fn test_view_with_column_aliases() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS source_data");
    exec_ok(
        &mut executor,
        "CREATE TABLE source_data (
            col1 INT64,
            col2 STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO source_data VALUES (1, 'A'), (2, 'B')",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS aliased_view");
    exec_ok(
        &mut executor,
        "CREATE VIEW aliased_view AS SELECT col1 AS id, col2 AS label FROM source_data",
    );

    let result = query(
        &mut executor,
        "SELECT id, label FROM aliased_view ORDER BY id",
    );

    assert_row_count(&result, 2);
    assert_eq!(get_i64_by_name(&result, 0, "id"), 1);
    assert_eq!(get_string_by_name(&result, 0, "label"), "A");
    assert_eq!(get_i64_by_name(&result, 1, "id"), 2);
    assert_eq!(get_string_by_name(&result, 1, "label"), "B");
}

#[test]
fn test_view_with_distinct() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS duplicates");
    exec_ok(
        &mut executor,
        "CREATE TABLE duplicates (
            category STRING,
            value INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO duplicates VALUES
            ('A', 1),
            ('A', 1),
            ('B', 2),
            ('B', 2)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS unique_combinations");
    exec_ok(
        &mut executor,
        "CREATE VIEW unique_combinations AS SELECT DISTINCT category, value FROM duplicates",
    );

    let result = query(
        &mut executor,
        "SELECT * FROM unique_combinations ORDER BY category",
    );

    assert_row_count(&result, 2);
    assert_eq!(get_string_by_name(&result, 0, "category"), "A");
    assert_eq!(get_i64_by_name(&result, 0, "value"), 1);
    assert_eq!(get_string_by_name(&result, 1, "category"), "B");
    assert_eq!(get_i64_by_name(&result, 1, "value"), 2);
}

#[test]
fn test_complex_view_with_join_and_having() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS yacht_owners");
    exec_ok(
        &mut executor,
        "CREATE TABLE yacht_owners (
            owner_id INT64,
            owner_name STRING
        )",
    );

    exec_ok(&mut executor, "DROP TABLE IF EXISTS orders_complex");
    exec_ok(
        &mut executor,
        "CREATE TABLE orders_complex (
            order_id INT64,
            owner_id INT64,
            amount INT64
        )",
    );

    exec_ok(
        &mut executor,
        "INSERT INTO yacht_owners VALUES (1, 'Alice'), (2, 'Bob')",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO orders_complex VALUES
            (1, 1, 100),
            (2, 1, 200),
            (3, 2, 150)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS owner_orders");
    exec_ok(
        &mut executor,
        "CREATE VIEW owner_orders AS
        SELECT
            c.owner_id,
            c.owner_name,
            o.order_id,
            o.amount
        FROM yacht_owners c
        JOIN orders_complex o ON c.owner_id = o.owner_id",
    );

    let result = query(
        &mut executor,
        "SELECT * FROM owner_orders ORDER BY order_id",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_i64_by_name(&result, 0, "owner_id"), 1);
    assert_eq!(get_string_by_name(&result, 0, "owner_name"), "Alice");
    assert_eq!(get_i64_by_name(&result, 0, "order_id"), 1);
    assert_eq!(get_i64_by_name(&result, 0, "amount"), 100);

    assert_eq!(get_i64_by_name(&result, 1, "owner_id"), 1);
    assert_eq!(get_string_by_name(&result, 1, "owner_name"), "Alice");
    assert_eq!(get_i64_by_name(&result, 1, "order_id"), 2);
    assert_eq!(get_i64_by_name(&result, 1, "amount"), 200);

    assert_eq!(get_i64_by_name(&result, 2, "owner_id"), 2);
    assert_eq!(get_string_by_name(&result, 2, "owner_name"), "Bob");
    assert_eq!(get_i64_by_name(&result, 2, "order_id"), 3);
    assert_eq!(get_i64_by_name(&result, 2, "amount"), 150);
}

#[test]
fn test_view_with_cte() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS transactions");
    exec_ok(
        &mut executor,
        "CREATE TABLE transactions (
            id INT64,
            amount INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO transactions VALUES (1, 100), (2, 200), (3, 300)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS transaction_summary");
    exec_ok(
        &mut executor,
        "CREATE VIEW transaction_summary AS
        WITH high_value AS (
            SELECT * FROM transactions WHERE amount > 150
        )
        SELECT COUNT(*) AS count, SUM(amount) AS total
        FROM high_value",
    );

    let result = query(&mut executor, "SELECT * FROM transaction_summary");

    assert_row_count(&result, 1);

    assert_eq!(get_i64_by_name(&result, 0, "count"), 2);
    assert_eq!(get_i64_by_name(&result, 0, "total"), 500);
}

#[test]
fn test_view_with_null_handling() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS nullable_data");
    exec_ok(
        &mut executor,
        "CREATE TABLE nullable_data (
            id INT64,
            value STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO nullable_data VALUES (1, 'A'), (2, NULL), (3, 'C')",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS non_null_values");
    exec_ok(
        &mut executor,
        "CREATE VIEW non_null_values AS SELECT * FROM nullable_data WHERE value IS NOT NULL",
    );

    let result = query(&mut executor, "SELECT COUNT(*) AS cnt FROM non_null_values");

    assert_eq!(get_i64_by_name(&result, 0, "cnt"), 2);
}

#[test]
fn test_view_reflects_base_table_changes() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS dynamic_table");
    exec_ok(
        &mut executor,
        "CREATE TABLE dynamic_table (
            id INT64,
            value INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO dynamic_table VALUES (1, 10), (2, 20)",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS dynamic_view");
    exec_ok(
        &mut executor,
        "CREATE VIEW dynamic_view AS SELECT * FROM dynamic_table WHERE value > 10",
    );

    let result = query(&mut executor, "SELECT COUNT(*) AS cnt FROM dynamic_view");
    assert_eq!(get_i64_by_name(&result, 0, "cnt"), 1);

    exec_ok(&mut executor, "INSERT INTO dynamic_table VALUES (3, 30)");

    let result = query(&mut executor, "SELECT COUNT(*) AS cnt FROM dynamic_view");
    assert_eq!(get_i64_by_name(&result, 0, "cnt"), 2);
}

#[test]
fn test_view_for_security_encapsulation() {
    let mut executor = new_executor();

    exec_ok(&mut executor, "DROP TABLE IF EXISTS user_accounts");
    exec_ok(
        &mut executor,
        "CREATE TABLE user_accounts (
            user_id INT64,
            username STRING,
            password_hash STRING,
            email STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO user_accounts VALUES (1, 'alice', 'hash1', 'alice@example.com')",
    );

    exec_ok(&mut executor, "DROP VIEW IF EXISTS public_users");
    exec_ok(
        &mut executor,
        "CREATE VIEW public_users AS SELECT user_id, username, email FROM user_accounts",
    );

    let result = query(&mut executor, "SELECT * FROM public_users");

    assert_row_count(&result, 1);
    assert_eq!(get_i64_by_name(&result, 0, "user_id"), 1);
    assert_eq!(get_string_by_name(&result, 0, "username"), "alice");
    assert_eq!(get_string_by_name(&result, 0, "email"), "alice@example.com");

    let schema = result.schema();
    let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name.as_str()).collect();
    assert!(!field_names.contains(&"password_hash"));
}
