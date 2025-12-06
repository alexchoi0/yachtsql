#![allow(clippy::approx_constant)]
#![allow(clippy::manual_range_contains)]

use yachtsql::QueryExecutor;
use yachtsql_parser::DialectType;

fn setup_ch() -> QueryExecutor {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);
    executor
        .execute_sql(
            "CREATE TABLE products (id INT64, name VARCHAR, price FLOAT64, quantity INT64)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Apple', 1.99, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (2, 'Banana', 0.50, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (3, 'Orange', 2.50, 150)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE points (id INT64, x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO points VALUES (1, 1.0, 2.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO points VALUES (2, 2.0, 4.0)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO points VALUES (3, 3.0, 6.0)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE nums (id INT64, a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (1, 10, 20, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nums VALUES (2, 5, 15, 25)")
        .unwrap();

    executor
}

fn get_numeric(val: yachtsql_core::types::Value) -> f64 {
    val.as_f64()
        .or_else(|| val.as_i64().map(|i| i as f64))
        .unwrap_or_else(|| panic!("Expected numeric value, got {:?}", val))
}

#[test]
fn test_ch_abs_sign_mod() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT ABS(-5), SIGN(-3), MOD(10, 3) FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();
    let col2 = result.column(2).unwrap();

    assert!((get_numeric(col0.get(0).unwrap()) - 5.0).abs() < 0.001);
    assert!((get_numeric(col1.get(0).unwrap()) - (-1.0)).abs() < 0.001);
    assert!((get_numeric(col2.get(0).unwrap()) - 1.0).abs() < 0.001);
}

#[test]
fn test_ch_power_sqrt() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT POWER(2, 10), SQRT(16) FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();

    assert!((get_numeric(col0.get(0).unwrap()) - 1024.0).abs() < 0.001);
    assert!((get_numeric(col1.get(0).unwrap()) - 4.0).abs() < 0.001);
}

#[test]
fn test_ch_round_trunc() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT ROUND(3.14159, 2), TRUNC(3.9876, 2) FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();

    assert!((get_numeric(col0.get(0).unwrap()) - 3.14).abs() < 0.001);
    assert!((get_numeric(col1.get(0).unwrap()) - 3.98).abs() < 0.001);
}

#[test]
fn test_ch_trig_basic() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT SIN(0), COS(0), TAN(0) FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();
    let col2 = result.column(2).unwrap();

    assert!(get_numeric(col0.get(0).unwrap()).abs() < 0.001);
    assert!((get_numeric(col1.get(0).unwrap()) - 1.0).abs() < 0.001);
    assert!(get_numeric(col2.get(0).unwrap()).abs() < 0.001);
}

#[test]
fn test_ch_inverse_trig() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT ASIN(0.5), ACOS(0.5), ATAN(1) FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();
    let col2 = result.column(2).unwrap();

    assert!((get_numeric(col0.get(0).unwrap()) - 0.5236).abs() < 0.01);
    assert!((get_numeric(col1.get(0).unwrap()) - 1.0472).abs() < 0.01);
    assert!((get_numeric(col2.get(0).unwrap()) - 0.7854).abs() < 0.01);
}

#[test]
fn test_ch_log_ln_exp() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT LOG10(100), LN(EXP(1)), EXP(1) FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();
    let col2 = result.column(2).unwrap();

    assert!((get_numeric(col0.get(0).unwrap()) - 2.0).abs() < 0.001);
    assert!((get_numeric(col1.get(0).unwrap()) - 1.0).abs() < 0.001);
    assert!((get_numeric(col2.get(0).unwrap()) - std::f64::consts::E).abs() < 0.001);
}

#[test]
fn test_ch_stddev_variance() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT STDDEV(price), VARIANCE(price) FROM products")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();

    assert!(get_numeric(col0.get(0).unwrap()) >= 0.0);
    assert!(get_numeric(col1.get(0).unwrap()) >= 0.0);
}

#[test]
fn test_ch_corr_covar() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT CORR(x, y), COVAR_POP(x, y) FROM points")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();

    assert!((get_numeric(col0.get(0).unwrap()) - 1.0).abs() < 0.001);
    assert!(get_numeric(col1.get(0).unwrap()) > 0.0);
}

#[test]
fn test_ch_random() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT RANDOM() FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let val = get_numeric(col0.get(0).unwrap());
    assert!(val >= 0.0 && val < 1.0);
}

#[test]
fn test_ch_greatest_least() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT GREATEST(a, b, c), LEAST(a, b, c) FROM nums ORDER BY id")
        .unwrap();
    assert_eq!(result.num_rows(), 2);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();

    assert!((get_numeric(col0.get(0).unwrap()) - 30.0).abs() < 0.001);
    assert!((get_numeric(col1.get(0).unwrap()) - 10.0).abs() < 0.001);
}

#[test]
fn test_ch_null_handling() {
    let mut executor = setup_ch();
    let result = executor
        .execute_sql("SELECT ABS(NULL), SQRT(NULL), SIN(NULL) FROM products LIMIT 1")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let col0 = result.column(0).unwrap();
    let col1 = result.column(1).unwrap();
    let col2 = result.column(2).unwrap();

    assert!(col0.get(0).unwrap().is_null());
    assert!(col1.get(0).unwrap().is_null());
    assert!(col2.get(0).unwrap().is_null());
}
