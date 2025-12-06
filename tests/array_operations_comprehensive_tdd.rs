#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

fn new_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

fn assert_error_contains(result: yachtsql::Result<yachtsql::RecordBatch>, keywords: &[&str]) {
    assert!(result.is_err(), "Expected error but got Ok");
    let err_msg = result.unwrap_err().to_string();
    let found = keywords
        .iter()
        .any(|kw| err_msg.to_lowercase().contains(&kw.to_lowercase()));
    assert!(
        found,
        "Error message '{}' should contain one of {:?}",
        err_msg, keywords
    );
}

#[test]
fn test_array_length_basic() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (ARRAY[1, 2, 3, 4, 5])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(arr) as len FROM t")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 5);
}

#[test]
fn test_array_length_empty_array() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(ARRAY[]) as len")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 0);
}

#[test]
fn test_array_length_null_array() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (arr ARRAY<INT64>)")
        .unwrap();
    executor.execute_sql("INSERT INTO t VALUES (NULL)").unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(arr) as len FROM t")
        .unwrap();

    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_length_single_element() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(ARRAY[42]) as len")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
}

#[test]
fn test_array_length_string_array() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(ARRAY['a', 'b', 'c']) as len")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 3);
}

#[test]
fn test_array_length_in_where_clause() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (id INT64, arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, ARRAY[1,2,3]), (2, ARRAY[1,2,3,4,5]), (3, ARRAY[1])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM t WHERE ARRAY_LENGTH(arr) > 3 ORDER BY id")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
}

#[test]
fn test_array_append_basic() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_APPEND(ARRAY[1, 2, 3], 4) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 4);
    assert_eq!(elements[3].as_i64().unwrap(), 4);
}

#[test]
fn test_array_append_to_empty() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_APPEND(ARRAY[], 1) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0].as_i64().unwrap(), 1);
}

#[test]
fn test_array_append_null_element() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_APPEND(ARRAY[1, 2], NULL) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert!(elements[2].is_null());
}

#[test]
fn test_array_append_null_array() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (arr ARRAY<INT64>)")
        .unwrap();
    executor.execute_sql("INSERT INTO t VALUES (NULL)").unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_APPEND(arr, 5) as result FROM t")
        .unwrap();

    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_append_string_array() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_APPEND(ARRAY['a', 'b'], 'c') as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[2].as_str().unwrap(), "c");
}

#[test]
fn test_array_append_type_mismatch_should_error() {
    let mut executor = new_executor();

    let result = executor.execute_sql("SELECT ARRAY_APPEND(ARRAY[1, 2], 'string') as arr");

    assert_error_contains(result, &["type", "mismatch", "incompatible"]);
}

#[test]
fn test_array_prepend_basic() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_PREPEND(0, ARRAY[1, 2, 3]) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 4);
    assert_eq!(elements[0].as_i64().unwrap(), 0);
    assert_eq!(elements[1].as_i64().unwrap(), 1);
}

#[test]
fn test_array_prepend_to_empty() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_PREPEND(42, ARRAY[]) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0].as_i64().unwrap(), 42);
}

#[test]
fn test_array_prepend_null_element() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_PREPEND(NULL, ARRAY[1, 2]) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert!(elements[0].is_null());
}

#[test]
fn test_array_cat_basic() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_CAT(ARRAY[1, 2], ARRAY[3, 4]) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 4);
    assert_eq!(elements[0].as_i64().unwrap(), 1);
    assert_eq!(elements[3].as_i64().unwrap(), 4);
}

#[test]
fn test_array_cat_empty_arrays() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_CAT(ARRAY[], ARRAY[]) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 0);
}

#[test]
fn test_array_cat_with_empty() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_CAT(ARRAY[1, 2, 3], ARRAY[]) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
}

#[test]
fn test_array_cat_null_array() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (arr ARRAY<INT64>)")
        .unwrap();
    executor.execute_sql("INSERT INTO t VALUES (NULL)").unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_CAT(arr, ARRAY[1, 2]) as result FROM t")
        .unwrap();

    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_cat_multiple_concatenations() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_CAT(ARRAY_CAT(ARRAY[1], ARRAY[2]), ARRAY[3]) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[2].as_i64().unwrap(), 3);
}

#[test]
fn test_array_position_found() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_POSITION(ARRAY[10, 20, 30, 40], 30) as pos")
        .unwrap();

    let col = result.column(0).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 3);
}

#[test]
fn test_array_position_not_found() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_POSITION(ARRAY[10, 20, 30], 99) as pos")
        .unwrap();

    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_position_first_occurrence() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_POSITION(ARRAY[1, 2, 3, 2, 4], 2) as pos")
        .unwrap();

    let col = result.column(0).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
}

#[test]
fn test_array_position_null_element() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_POSITION(ARRAY[1, NULL, 3], NULL) as pos")
        .unwrap();

    let col = result.column(0).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
}

#[test]
fn test_array_position_string_array() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_POSITION(ARRAY['apple', 'banana', 'cherry'], 'banana') as pos")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
}

#[test]
fn test_array_position_empty_array() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_POSITION(ARRAY[], 1) as pos")
        .unwrap();

    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_remove_single_occurrence() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REMOVE(ARRAY[1, 2, 3, 4], 3) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0].as_i64().unwrap(), 1);
    assert_eq!(elements[1].as_i64().unwrap(), 2);
    assert_eq!(elements[2].as_i64().unwrap(), 4);
}

#[test]
fn test_array_remove_multiple_occurrences() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REMOVE(ARRAY[1, 2, 2, 3, 2, 4], 2) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0].as_i64().unwrap(), 1);
    assert_eq!(elements[1].as_i64().unwrap(), 3);
    assert_eq!(elements[2].as_i64().unwrap(), 4);
}

#[test]
fn test_array_remove_not_found() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REMOVE(ARRAY[1, 2, 3], 99) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
}

#[test]
fn test_array_remove_null_element() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REMOVE(ARRAY[1, NULL, 2, NULL, 3], NULL) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0].as_i64().unwrap(), 1);
    assert_eq!(elements[1].as_i64().unwrap(), 2);
    assert_eq!(elements[2].as_i64().unwrap(), 3);
}

#[test]
fn test_array_remove_all_elements() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REMOVE(ARRAY[5, 5, 5], 5) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 0);
}

#[test]
fn test_array_replace_single_occurrence() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REPLACE(ARRAY[1, 2, 3, 4], 3, 99) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 4);
    assert_eq!(elements[2].as_i64().unwrap(), 99);
}

#[test]
fn test_array_replace_multiple_occurrences() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REPLACE(ARRAY[1, 2, 2, 3, 2], 2, 99) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 5);
    assert_eq!(elements[1].as_i64().unwrap(), 99);
    assert_eq!(elements[2].as_i64().unwrap(), 99);
    assert_eq!(elements[4].as_i64().unwrap(), 99);
}

#[test]
fn test_array_replace_not_found() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REPLACE(ARRAY[1, 2, 3], 99, 0) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0].as_i64().unwrap(), 1);
}

#[test]
fn test_array_replace_with_null() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REPLACE(ARRAY[1, 2, 3], 2, NULL) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert!(elements[1].is_null());
}

#[test]
fn test_array_replace_null_search() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_REPLACE(ARRAY[1, NULL, 3], NULL, 99) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[1].as_i64().unwrap(), 99);
}

#[test]
fn test_array_indexing_basic() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[10, 20, 30, 40][2] as val")
        .unwrap();

    let col = result.column(0).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 20);
}

#[test]
fn test_array_indexing_first_element() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[10, 20, 30][1] as val")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 10);
}

#[test]
fn test_array_indexing_last_element() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[10, 20, 30][3] as val")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 30);
}

#[test]
fn test_array_indexing_out_of_bounds() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[10, 20, 30][10] as val")
        .unwrap();

    let col = result.column(0).unwrap();

    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_indexing_zero() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[10, 20, 30][0] as val")
        .unwrap();

    let col = result.column(0).unwrap();

    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_indexing_negative() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[10, 20, 30][-1] as val")
        .unwrap();

    let col = result.column(0).unwrap();

    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_array_indexing_column() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (id INT64, arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, ARRAY[100, 200, 300])")
        .unwrap();

    let result = executor.execute_sql("SELECT arr[2] as val FROM t").unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 200);
}

#[test]
fn test_array_slice_basic() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[1, 2, 3, 4, 5][2:4] as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0].as_i64().unwrap(), 2);
    assert_eq!(elements[1].as_i64().unwrap(), 3);
    assert_eq!(elements[2].as_i64().unwrap(), 4);
}

#[test]
fn test_array_slice_from_start() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[1, 2, 3, 4, 5][1:3] as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0].as_i64().unwrap(), 1);
}

#[test]
fn test_array_slice_to_end() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[1, 2, 3, 4, 5][3:] as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert_eq!(elements[0].as_i64().unwrap(), 3);
    assert_eq!(elements[2].as_i64().unwrap(), 5);
}

#[test]
fn test_array_slice_out_of_bounds() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY[1, 2, 3][5:10] as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 0);
}

#[test]
fn test_array_operations_with_aggregates() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10), (1, 20), (2, 30), (2, 40)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, ARRAY_LENGTH(ARRAY_AGG(val)) as len FROM t GROUP BY id ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(1).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 2);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 2);
}

#[test]
fn test_nested_array_operations() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_APPEND(ARRAY_REMOVE(ARRAY[1, 2, 3, 2, 4], 2), 99) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 4);
    assert_eq!(elements[3].as_i64().unwrap(), 99);
}

#[test]
fn test_array_in_where_with_position() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (id INT64, arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, ARRAY[10, 20, 30]), (2, ARRAY[15, 25, 35])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM t WHERE ARRAY_POSITION(arr, 20) IS NOT NULL")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 1);
}

#[test]
fn test_array_operations_in_order_by() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (id INT64, arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO t VALUES (1, ARRAY[1, 2, 3]), (2, ARRAY[1]), (3, ARRAY[1, 2, 3, 4, 5])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM t ORDER BY ARRAY_LENGTH(arr) DESC")
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64().unwrap(), 3);
    assert_eq!(col.get(1).unwrap().as_i64().unwrap(), 1);
    assert_eq!(col.get(2).unwrap().as_i64().unwrap(), 2);
}

#[test]
fn test_array_operations_type_preservation() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql("SELECT ARRAY_APPEND(ARRAY[1.5, 2.5], 3.5) as arr")
        .unwrap();

    let col = result.column(0).unwrap();
    let value = col.get(0).unwrap();
    let elements = value.as_array().expect("Expected Array value");
    assert_eq!(elements.len(), 3);
    assert!((elements[2].as_f64().unwrap() - 3.5).abs() < 0.001);
}

#[test]
fn test_array_operations_with_joins() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t1 (id INT64, arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE t2 (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t1 VALUES (1, ARRAY[10, 20, 30])")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t2 VALUES (1, 20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT t1.id
             FROM t1
             JOIN t2 ON t1.id = t2.id
             WHERE ARRAY_POSITION(t1.arr, t2.val) IS NOT NULL",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_operations_with_subqueries() {
    let mut executor = new_executor();
    executor
        .execute_sql("CREATE TABLE t (id INT64, arr ARRAY<INT64>)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, ARRAY[1, 2, 3]), (2, ARRAY[1, 2, 3, 4, 5])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id
             FROM t
             WHERE ARRAY_LENGTH(arr) > (SELECT AVG(ARRAY_LENGTH(arr)) FROM t)",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_array_operations_in_case_expression() {
    let mut executor = new_executor();

    let result = executor
        .execute_sql(
            "SELECT CASE
                WHEN ARRAY_LENGTH(ARRAY[1, 2, 3]) > 2 THEN 'large'
                ELSE 'small'
             END as size",
        )
        .unwrap();

    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_str().unwrap(), "large");
}
