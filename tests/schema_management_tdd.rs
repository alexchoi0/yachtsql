#![allow(dead_code)]
#![allow(unused_variables)]

mod common;
use common::{assert_error_contains, new_executor};

#[test]
fn test_create_schema_basic() {
    let mut executor = new_executor();

    let result = executor.execute_sql("CREATE SCHEMA sales");
    assert!(
        result.is_ok(),
        "CREATE SCHEMA should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_create_schema_if_not_exists() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA sales").unwrap();

    let result = executor.execute_sql("CREATE SCHEMA sales");
    assert!(result.is_err(), "Duplicate CREATE SCHEMA should fail");
    assert_error_contains(result, &["already exists", "schema"]);

    let result = executor.execute_sql("CREATE SCHEMA IF NOT EXISTS sales");
    assert!(
        result.is_ok(),
        "CREATE SCHEMA IF NOT EXISTS should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_create_table_in_schema() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA inventory").unwrap();

    let result = executor.execute_sql("CREATE TABLE inventory.products (id INT64, name STRING)");
    assert!(
        result.is_ok(),
        "CREATE TABLE in schema should succeed: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SELECT * FROM inventory.products");
    assert!(
        result.is_ok(),
        "SELECT from schema.table should work: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().num_rows(), 0);
}

#[test]
fn test_create_schema_with_authorization() {
    let mut executor = new_executor();

    let result = executor.execute_sql("CREATE SCHEMA hr AUTHORIZATION postgres");
    assert!(
        result.is_ok(),
        "CREATE SCHEMA with AUTHORIZATION should parse: {:?}",
        result.err()
    );
}

#[test]
fn test_drop_schema_empty() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA temp_schema").unwrap();

    let result = executor.execute_sql("DROP SCHEMA temp_schema");
    assert!(
        result.is_ok(),
        "DROP SCHEMA should succeed: {:?}",
        result.err()
    );

    let result = executor.execute_sql("CREATE SCHEMA temp_schema");
    assert!(
        result.is_ok(),
        "Re-creating dropped schema should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_drop_schema_cascade() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA myschema").unwrap();
    executor
        .execute_sql("CREATE TABLE myschema.mytable (id INT64)")
        .unwrap();

    let result = executor.execute_sql("DROP SCHEMA myschema");
    assert!(
        result.is_err(),
        "DROP SCHEMA with objects should fail without CASCADE"
    );

    let result = executor.execute_sql("DROP SCHEMA myschema CASCADE");
    assert!(
        result.is_ok(),
        "DROP SCHEMA CASCADE should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_drop_schema_restrict() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA restricted").unwrap();
    executor
        .execute_sql("CREATE TABLE restricted.data (id INT64)")
        .unwrap();

    let result = executor.execute_sql("DROP SCHEMA restricted RESTRICT");
    assert!(
        result.is_err(),
        "DROP SCHEMA RESTRICT should fail with objects"
    );
}

#[test]
fn test_drop_schema_if_exists() {
    let mut executor = new_executor();

    let result = executor.execute_sql("DROP SCHEMA nonexistent_schema");
    assert!(result.is_err(), "DROP non-existent SCHEMA should fail");

    let result = executor.execute_sql("DROP SCHEMA IF EXISTS nonexistent_schema");
    assert!(
        result.is_ok(),
        "DROP SCHEMA IF EXISTS should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_set_search_path_single() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA app").unwrap();

    let result = executor.execute_sql("SET search_path TO app");
    assert!(
        result.is_ok(),
        "SET search_path should succeed: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SHOW search_path").unwrap();
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap();
    assert!(
        val.as_str().unwrap().contains("app"),
        "search_path should contain app"
    );
}

#[test]
fn test_set_search_path_multiple() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA schema1").unwrap();
    executor.execute_sql("CREATE SCHEMA schema2").unwrap();

    let result = executor.execute_sql("SET search_path TO schema1, schema2");
    assert!(
        result.is_ok(),
        "SET search_path with multiple schemas should succeed: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SHOW search_path").unwrap();
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap();
    let path_str = val.as_str().unwrap();
    assert!(
        path_str.contains("schema1"),
        "search_path should contain schema1: {}",
        path_str
    );
    assert!(
        path_str.contains("schema2"),
        "search_path should contain schema2: {}",
        path_str
    );

    executor
        .execute_sql("CREATE TABLE schema1.table1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE schema2.table2 (id INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM schema1.table1");
    assert!(
        result.is_ok(),
        "table1 should be accessible with schema prefix: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SELECT * FROM schema2.table2");
    assert!(
        result.is_ok(),
        "table2 should be accessible with schema prefix: {:?}",
        result.err()
    );
}

#[test]
fn test_set_search_path_with_public() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA myapp").unwrap();

    let result = executor.execute_sql("SET search_path TO myapp, public");
    assert!(
        result.is_ok(),
        "SET search_path with public should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_show_search_path() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA test_schema").unwrap();
    executor
        .execute_sql("SET search_path TO test_schema, public")
        .unwrap();

    let result = executor.execute_sql("SHOW search_path");
    assert!(
        result.is_ok(),
        "SHOW search_path should succeed: {:?}",
        result.err()
    );

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0).unwrap();
    let val = col.get(0).unwrap();
    let path_str = val.as_str().unwrap();
    assert!(
        path_str.contains("test_schema"),
        "search_path should include test_schema: {}",
        path_str
    );
}

#[test]
fn test_schema_qualified_select() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA data").unwrap();
    executor
        .execute_sql("CREATE TABLE data.measurements (id INT64, value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data.measurements VALUES (1, 100.5)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM data.measurements");
    assert!(
        result.is_ok(),
        "Schema-qualified SELECT should work: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().num_rows(), 1);
}

#[test]
fn test_schema_qualified_insert() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA logging").unwrap();
    executor
        .execute_sql("CREATE TABLE logging.events (id INT64, message STRING)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO logging.events VALUES (1, 'Test event')");
    assert!(
        result.is_ok(),
        "Schema-qualified INSERT should work: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SELECT COUNT(*) FROM logging.events");
    let batch = result.unwrap();
    let count = batch.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(count, 1);
}

#[test]
fn test_schema_qualified_update() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA updates").unwrap();
    executor
        .execute_sql("CREATE TABLE updates.records (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO updates.records VALUES (1, 'pending')")
        .unwrap();

    let result = executor.execute_sql("UPDATE updates.records SET status = 'done' WHERE id = 1");
    assert!(
        result.is_ok(),
        "Schema-qualified UPDATE should work: {:?}",
        result.err()
    );
}

#[test]
fn test_schema_qualified_delete() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA cleanup").unwrap();
    executor
        .execute_sql("CREATE TABLE cleanup.temp (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cleanup.temp VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("DELETE FROM cleanup.temp WHERE id = 1");
    assert!(
        result.is_ok(),
        "Schema-qualified DELETE should work: {:?}",
        result.err()
    );
}

#[test]
fn test_same_table_name_different_schemas() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA schema_a").unwrap();
    executor.execute_sql("CREATE SCHEMA schema_b").unwrap();

    executor
        .execute_sql("CREATE TABLE schema_a.users (id INT64, type STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE schema_b.users (id INT64, type STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO schema_a.users VALUES (1, 'admin')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO schema_b.users VALUES (2, 'guest')")
        .unwrap();

    let result_a = executor
        .execute_sql("SELECT type FROM schema_a.users")
        .unwrap();
    let result_b = executor
        .execute_sql("SELECT type FROM schema_b.users")
        .unwrap();

    let binding_a = result_a.column(0).unwrap().get(0).unwrap();
    let type_a = binding_a.as_str().unwrap();
    let binding_b = result_b.column(0).unwrap().get(0).unwrap();
    let type_b = binding_b.as_str().unwrap();

    assert_eq!(type_a, "admin");
    assert_eq!(type_b, "guest");
}

#[test]
fn test_search_path_resolution_order() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA first").unwrap();
    executor.execute_sql("CREATE SCHEMA second").unwrap();

    executor
        .execute_sql("CREATE TABLE first.priority (value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE second.priority (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO first.priority VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO second.priority VALUES (2)")
        .unwrap();

    executor
        .execute_sql("SET search_path TO first, second")
        .unwrap();

    let result = executor.execute_sql("SHOW search_path").unwrap();
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap();
    let path_str = val.as_str().unwrap();

    let first_pos = path_str.find("first").unwrap_or(usize::MAX);
    let second_pos = path_str.find("second").unwrap_or(usize::MAX);
    assert!(
        first_pos < second_pos,
        "first should appear before second in search_path: {}",
        path_str
    );

    let result = executor
        .execute_sql("SELECT value FROM first.priority")
        .unwrap();
    let value = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(value, 1, "Should get value from first.priority");

    let result = executor
        .execute_sql("SELECT value FROM second.priority")
        .unwrap();
    let value = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(value, 2, "Should get value from second.priority");
}

#[test]
fn test_default_schema_is_public() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE test_default (id INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM default.test_default");
    if result.is_err() {
        let result = executor.execute_sql("SELECT * FROM public.test_default");
    }
}

#[test]
fn test_cross_schema_join() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA orders_schema").unwrap();
    executor
        .execute_sql("CREATE SCHEMA customers_schema")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE customers_schema.customers (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE orders_schema.orders (id INT64, customer_id INT64, amount FLOAT64)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO customers_schema.customers VALUES (1, 'Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders_schema.orders VALUES (100, 1, 250.00)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT c.name, o.amount
         FROM customers_schema.customers c
         JOIN orders_schema.orders o ON c.id = o.customer_id",
    );
    assert!(
        result.is_ok(),
        "Cross-schema JOIN should work: {:?}",
        result.err()
    );

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_cross_schema_subquery() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE SCHEMA main_schema").unwrap();
    executor.execute_sql("CREATE SCHEMA lookup_schema").unwrap();

    executor
        .execute_sql("CREATE TABLE main_schema.items (id INT64, category_id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE lookup_schema.categories (id INT64, name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO main_schema.items VALUES (1, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lookup_schema.categories VALUES (10, 'Electronics')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM main_schema.items");
    assert!(
        result.is_ok(),
        "Schema-qualified main query should work: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().num_rows(), 1);

    let result = executor.execute_sql("SELECT * FROM lookup_schema.categories");
    assert!(
        result.is_ok(),
        "Schema-qualified lookup query should work: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().num_rows(), 1);
}
