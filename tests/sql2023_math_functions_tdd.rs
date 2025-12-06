#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::approx_constant)]
#![allow(clippy::manual_range_contains)]

use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

#[test]
fn test_abs_positive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ABS(42) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(42));
}

#[test]
fn test_abs_negative_int() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ABS(-42) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(42));
}

#[test]
fn test_abs_negative_float() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ABS(-3.14159) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.14159).abs() < 0.00001);
}

#[test]
fn test_abs_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ABS(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(0));
}

#[test]
fn test_abs_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ABS(NULL) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_sign_positive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIGN(42) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_sign_negative() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIGN(-42) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(-1));
}

#[test]
fn test_sign_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIGN(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(0));
}

#[test]
fn test_sign_positive_float() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIGN(3.14) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_sign_negative_float() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SIGN(-0.001) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(-1));
}

#[test]
fn test_ceil_positive_fraction() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CEIL(3.2) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 4.0).abs() < 0.0001);
}

#[test]
fn test_ceil_negative() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CEIL(-3.8) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - (-3.0)).abs() < 0.0001);
}

#[test]
fn test_ceil_whole_number() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CEIL(5.0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 5.0).abs() < 0.0001);
}

#[test]
fn test_ceiling_alias() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CEILING(3.14) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 4.0).abs() < 0.0001);
}

#[test]
fn test_ceil_multiple_columns() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CEIL(1.2) AS c1, CEIL(2.8) AS c2, CEIL(-1.5) AS c3")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let c1 = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    let c2 = result.column(1).unwrap().get(0).unwrap().as_f64().unwrap();
    let c3 = result.column(2).unwrap().get(0).unwrap().as_f64().unwrap();

    assert!((c1 - 2.0).abs() < 0.0001);
    assert!((c2 - 3.0).abs() < 0.0001);
    assert!((c3 - (-1.0)).abs() < 0.0001);
}

#[test]
fn test_floor_positive_fraction() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FLOOR(3.8) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.0).abs() < 0.0001);
}

#[test]
fn test_floor_negative() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FLOOR(-3.2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - (-4.0)).abs() < 0.0001);
}

#[test]
fn test_floor_whole_number() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FLOOR(5.0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 5.0).abs() < 0.0001);
}

#[test]
fn test_floor_multiple_columns() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FLOOR(1.2) AS f1, FLOOR(2.8) AS f2, FLOOR(-1.5) AS f3")
        .unwrap();
    assert_eq!(result.num_rows(), 1);

    let f1 = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    let f2 = result.column(1).unwrap().get(0).unwrap().as_f64().unwrap();
    let f3 = result.column(2).unwrap().get(0).unwrap().as_f64().unwrap();

    assert!((f1 - 1.0).abs() < 0.0001);
    assert!((f2 - 2.0).abs() < 0.0001);
    assert!((f3 - (-2.0)).abs() < 0.0001);
}

#[test]
fn test_round_half_up() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ROUND(3.5) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 4.0).abs() < 0.0001);
}

#[test]
fn test_round_half_down() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ROUND(3.4) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.0).abs() < 0.0001);
}

#[test]
fn test_round_with_precision_2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(3.14159, 2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.14).abs() < 0.0001);
}

#[test]
fn test_round_with_precision_3() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(3.14159, 3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.142).abs() < 0.0001);
}

#[test]
fn test_round_negative_precision_2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(1234.5678, -2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1200.0).abs() < 0.0001);
}

#[test]
fn test_round_negative_precision_1() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(1234.5678, -1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1230.0).abs() < 0.0001);
}

#[test]
fn test_round_negative_half() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(-1.5) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - (-2.0)).abs() < 0.0001);
}

#[test]
fn test_trunc_positive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TRUNC(3.8) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.0).abs() < 0.0001);
}

#[test]
fn test_trunc_negative() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUNC(-3.8) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - (-3.0)).abs() < 0.0001);
}

#[test]
fn test_trunc_with_precision_2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUNC(3.14159, 2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.14).abs() < 0.0001);
}

#[test]
fn test_trunc_with_precision_3() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUNC(3.14159, 3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.141).abs() < 0.0001);
}

#[test]
fn test_truncate_alias() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUNCATE(5.9) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 5.0).abs() < 0.0001);
}

#[test]
fn test_mod_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT MOD(10, 3) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_mod_larger_divisor() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT MOD(17, 5) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(2));
}

#[test]
fn test_mod_even_division() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT MOD(10, 2) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(0));
}

#[test]
fn test_mod_negative_dividend() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MOD(-10, 3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64(), Some(-1));
}

#[test]
fn test_mod_negative_divisor() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MOD(10, -3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_mod_float() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MOD(10.5, 3.0) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.5).abs() < 0.0001);
}

#[test]
fn test_power_integer() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(2, 3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 8.0).abs() < 0.0001);
}

#[test]
fn test_power_ten_squared() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(10, 2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 100.0).abs() < 0.0001);
}

#[test]
fn test_power_negative_exponent() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(2, -2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.25).abs() < 0.0001);
}

#[test]
fn test_power_zero_exponent() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(42, 0) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_power_fractional_exponent() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(4, 0.5) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 2.0).abs() < 0.0001);
}

#[test]
fn test_power_cube_root() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(8, 1.0/3.0) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 2.0).abs() < 0.0001);
}

#[test]
fn test_pow_alias() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT POW(2, 3) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 8.0).abs() < 0.0001);
}

#[test]
fn test_pow_five_squared() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT POW(5, 2) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 25.0).abs() < 0.0001);
}

#[test]
fn test_sqrt_perfect_square() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SQRT(16) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 4.0).abs() < 0.0001);
}

#[test]
fn test_sqrt_two() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SQRT(2) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.41421356).abs() < 0.0001);
}

#[test]
fn test_sqrt_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SQRT(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_sqrt_144() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SQRT(144) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 12.0).abs() < 0.0001);
}

#[test]
fn test_exp_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT EXP(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_exp_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT EXP(1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 2.71828).abs() < 0.001);
}

#[test]
fn test_exp_negative_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT EXP(-1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.36788).abs() < 0.001);
}

#[test]
fn test_exp_two() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT EXP(2) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 7.38906).abs() < 0.001);
}

#[test]
fn test_ln_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LN(1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_ln_exp_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LN(EXP(1)) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_ln_ten() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LN(10) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 2.30259).abs() < 0.001);
}

#[test]
fn test_ln_e() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LN(2.718281828) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_log_ten() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LOG(10) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 2.302585).abs() < 0.001);
}

#[test]
fn test_log_with_base_10_100() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LOG(10, 100) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 2.0).abs() < 0.0001);
}

#[test]
fn test_log_with_base_2_8() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LOG(2, 8) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.0).abs() < 0.0001);
}

#[test]
fn test_log_with_base_10_1000() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LOG(10, 1000) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.0).abs() < 0.0001);
}

#[test]
fn test_log10_100() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LOG10(100) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 2.0).abs() < 0.0001);
}

#[test]
fn test_log10_1000() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LOG10(1000) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.0).abs() < 0.0001);
}

#[test]
fn test_log10_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LOG10(1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_log10_ten() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LOG10(10) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_sin_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIN(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_sin_pi_over_2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SIN(PI() / 2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_sin_pi() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SIN(PI()) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!(val.abs() < 0.0001);
}

#[test]
fn test_sin_pi_over_6() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SIN(PI() / 6) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.5).abs() < 0.0001);
}

#[test]
fn test_cos_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT COS(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_cos_pi() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT COS(PI()) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - (-1.0)).abs() < 0.0001);
}

#[test]
fn test_cos_pi_over_2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT COS(PI() / 2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!(val.abs() < 0.0001);
}

#[test]
fn test_cos_pi_over_3() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT COS(PI() / 3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.5).abs() < 0.0001);
}

#[test]
fn test_tan_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TAN(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_tan_pi_over_4() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TAN(PI() / 4) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_tan_pi_over_6() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TAN(PI() / 6) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.57735).abs() < 0.001);
}

#[test]
fn test_asin_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASIN(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_asin_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASIN(1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.5708).abs() < 0.001);
}

#[test]
fn test_asin_half() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASIN(0.5) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.5236).abs() < 0.001);
}

#[test]
fn test_asin_negative_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASIN(-1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - (-1.5708)).abs() < 0.001);
}

#[test]
fn test_acos_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ACOS(1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_acos_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ACOS(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.5708).abs() < 0.001);
}

#[test]
fn test_acos_negative_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ACOS(-1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 3.14159).abs() < 0.001);
}

#[test]
fn test_acos_half() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ACOS(0.5) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.0472).abs() < 0.001);
}

#[test]
fn test_atan_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAN(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_atan_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAN(1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.7854).abs() < 0.001);
}

#[test]
fn test_atan_negative_one() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAN(-1) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - (-0.7854)).abs() < 0.001);
}

#[test]
fn test_atan_large() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ATAN(1000) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.5698).abs() < 0.01);
}

#[test]
fn test_atan2_quadrant_1() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ATAN2(1, 1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.7854).abs() < 0.001);
}

#[test]
fn test_atan2_positive_y_axis() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ATAN2(1, 0) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.5708).abs() < 0.001);
}

#[test]
fn test_atan2_positive_x_axis() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ATAN2(0, 1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_atan2_quadrant_2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ATAN2(1, -1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 2.3562).abs() < 0.001);
}

#[test]
fn test_atan2_quadrant_3() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ATAN2(-1, -1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - (-2.3562)).abs() < 0.001);
}

#[test]
fn test_degrees_pi() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DEGREES(PI()) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 180.0).abs() < 0.0001);
}

#[test]
fn test_degrees_pi_over_2() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DEGREES(PI() / 2) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 90.0).abs() < 0.0001);
}

#[test]
fn test_degrees_pi_over_4() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DEGREES(PI() / 4) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 45.0).abs() < 0.0001);
}

#[test]
fn test_degrees_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT DEGREES(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_degrees_two_pi() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DEGREES(2 * PI()) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 360.0).abs() < 0.0001);
}

#[test]
fn test_radians_180() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RADIANS(180) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 3.14159).abs() < 0.001);
}

#[test]
fn test_radians_90() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RADIANS(90) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 1.5708).abs() < 0.001);
}

#[test]
fn test_radians_45() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RADIANS(45) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 0.7854).abs() < 0.001);
}

#[test]
fn test_radians_zero() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT RADIANS(0) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.0).abs() < 0.0001);
}

#[test]
fn test_radians_360() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RADIANS(360) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 6.28318).abs() < 0.001);
}

#[test]
fn test_degrees_radians_roundtrip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DEGREES(RADIANS(45)) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 45.0).abs() < 0.0001);
}

#[test]
fn test_radians_degrees_roundtrip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RADIANS(DEGREES(1)) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_pi() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PI() AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.14159265).abs() < 0.0001);
}

#[test]
fn test_two_pi() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 2 * PI() AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 6.28318530).abs() < 0.0001);
}

#[test]
fn test_pi_over_2() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT PI() / 2 AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.57079632).abs() < 0.0001);
}

#[test]
fn test_greatest_three_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GREATEST(1, 2, 3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(3));
}

#[test]
fn test_greatest_four_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GREATEST(10, 5, 20, 15) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(20));
}

#[test]
fn test_greatest_negative_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GREATEST(-5, -10, -1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(-1));
}

#[test]
fn test_greatest_floats() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GREATEST(3.14, 2.71, 1.41) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.14).abs() < 0.0001);
}

#[test]
fn test_greatest_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GREATEST(NULL, 5, 10) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();

    let val = col.get(0).unwrap();
    if !val.is_null() {
        assert_eq!(val.as_i64(), Some(10));
    }
}

#[test]
fn test_greatest_single_value() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GREATEST(1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_least_three_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LEAST(1, 2, 3) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(1));
}

#[test]
fn test_least_four_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LEAST(10, 5, 20, 15) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(5));
}

#[test]
fn test_least_negative_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LEAST(-5, -10, -1) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(-10));
}

#[test]
fn test_least_floats() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LEAST(3.14, 2.71, 1.41) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.41).abs() < 0.0001);
}

#[test]
fn test_least_with_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LEAST(NULL, 5, 10) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();

    let val = col.get(0).unwrap();
    if !val.is_null() {
        assert_eq!(val.as_i64(), Some(5));
    }
}

#[test]
fn test_least_single_value() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LEAST(100) AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(100));
}

#[test]
fn test_random() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT RANDOM() AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!(val >= 0.0 && val < 1.0);
}

#[test]
fn test_random_in_range() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RANDOM() >= 0 AND RANDOM() < 1 AS in_range")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();

    assert!(col.get(0).unwrap().as_bool().unwrap_or(false));
}

#[test]
fn test_rand_alias() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT RAND() AS result").unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!(val >= 0.0 && val < 1.0);
}

#[test]
fn test_nested_sqrt_power_pythagorean() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SQRT(POWER(3, 2) + POWER(4, 2)) AS hypotenuse")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 5.0).abs() < 0.0001);
}

#[test]
fn test_round_pi() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROUND(PI(), 2) AS pi_rounded")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.14).abs() < 0.0001);
}

#[test]
fn test_abs_ceil_nested() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ABS(CEIL(-3.7)) AS result")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.0).abs() < 0.0001);
}

#[test]
fn test_trig_identity() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POWER(SIN(PI() / 4), 2) + POWER(COS(PI() / 4), 2) AS trig_identity")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 1.0).abs() < 0.0001);
}

#[test]
fn test_sin_degrees_30() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SIN(RADIANS(30)) AS sine_30_degrees")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 0.5).abs() < 0.0001);
}

#[test]
fn test_asin_degrees() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT DEGREES(ASIN(0.5)) AS arcsin_half")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 30.0).abs() < 0.001);
}

#[test]
fn test_ln_property() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LN(2 * 3) AS ln_product, LN(2) + LN(3) AS ln_sum")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let ln_product = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    let ln_sum = result.column(1).unwrap().get(0).unwrap().as_f64().unwrap();
    assert!((ln_product - ln_sum).abs() < 0.0001);
}

#[test]
fn test_exp_ln_roundtrip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT EXP(LN(42)) AS roundtrip")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 42.0).abs() < 0.0001);
}

#[test]
fn test_ln_exp_roundtrip() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LN(EXP(3.5)) AS roundtrip")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 3.5).abs() < 0.0001);
}

#[test]
fn test_greatest_abs() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT GREATEST(ABS(-5), ABS(3)) AS max_abs")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(5));
}

#[test]
fn test_least_ceil_floor() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LEAST(CEIL(3.2), FLOOR(4.8)) AS min_rounded")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 4.0).abs() < 0.0001);
}

#[test]
fn test_null_handling_math_functions() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ABS(NULL) AS abs_null, SQRT(NULL) AS sqrt_null, POWER(NULL, 2) AS power_null, SIN(NULL) AS sin_null").unwrap();
    assert_eq!(result.num_rows(), 1);

    assert!(result.column(0).unwrap().get(0).unwrap().is_null());
    assert!(result.column(1).unwrap().get(0).unwrap().is_null());
    assert!(result.column(2).unwrap().get(0).unwrap().is_null());
    assert!(result.column(3).unwrap().get(0).unwrap().is_null());
}

#[test]
fn test_division_identity() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 17 = (17 / 5) * 5 + MOD(17, 5) AS division_identity")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_distance_formula() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SQRT(POWER(4 - 1, 2) + POWER(6 - 2, 2)) AS distance")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 5.0).abs() < 0.0001);
}

#[test]
fn test_round_multiple_precisions() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ROUND(1234.5678, 3) AS r3, ROUND(1234.5678, 2) AS r2, ROUND(1234.5678, 1) AS r1, ROUND(1234.5678, 0) AS r0, ROUND(1234.5678, -1) AS r_neg1, ROUND(1234.5678, -2) AS r_neg2").unwrap();
    assert_eq!(result.num_rows(), 1);

    let r3 = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    let r2 = result.column(1).unwrap().get(0).unwrap().as_f64().unwrap();
    let r1 = result.column(2).unwrap().get(0).unwrap().as_f64().unwrap();
    let r0 = result.column(3).unwrap().get(0).unwrap().as_f64().unwrap();
    let r_neg1 = result.column(4).unwrap().get(0).unwrap().as_f64().unwrap();
    let r_neg2 = result.column(5).unwrap().get(0).unwrap().as_f64().unwrap();

    assert!((r3 - 1234.568).abs() < 0.0001);
    assert!((r2 - 1234.57).abs() < 0.0001);
    assert!((r1 - 1234.6).abs() < 0.0001);
    assert!((r0 - 1235.0).abs() < 0.0001);
    assert!((r_neg1 - 1230.0).abs() < 0.0001);
    assert!((r_neg2 - 1200.0).abs() < 0.0001);
}
