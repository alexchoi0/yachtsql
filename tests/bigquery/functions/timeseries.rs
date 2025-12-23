use crate::assert_table_eq;
use crate::common::{create_executor, d};

#[test]
fn test_date_bucket_2_day() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-05', INTERVAL 2 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 4)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-06', INTERVAL 2 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 6)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-07', INTERVAL 2 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 6)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-08', INTERVAL 2 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 8)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-09', INTERVAL 2 DAY)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 8)]]);
}

#[test]
fn test_date_bucket_with_origin() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-10', INTERVAL 7 DAY, DATE '2024-01-01')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 8)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-15', INTERVAL 7 DAY, DATE '2024-01-01')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-21', INTERVAL 7 DAY, DATE '2024-01-01')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 15)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-22', INTERVAL 7 DAY, DATE '2024-01-01')")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 22)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2023-12-31', INTERVAL 7 DAY, DATE '2024-01-01')")
        .unwrap();
    assert_table_eq!(result, [[d(2023, 12, 25)]]);
}

#[test]
fn test_date_bucket_month_interval() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-01-15', INTERVAL 1 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-02-15', INTERVAL 1 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 2, 1)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-03-15', INTERVAL 3 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 1, 1)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-04-15', INTERVAL 3 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 4, 1)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-06-15', INTERVAL 3 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 4, 1)]]);

    let result = executor
        .execute_sql("SELECT DATE_BUCKET(DATE '2024-07-01', INTERVAL 3 MONTH)")
        .unwrap();
    assert_table_eq!(result, [[d(2024, 7, 1)]]);
}
