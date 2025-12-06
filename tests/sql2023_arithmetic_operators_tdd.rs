#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::approx_constant)]

use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

#[test]
fn test_integer_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS numbers")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS sum FROM numbers")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(30));
}

#[test]
fn test_integer_subtraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS numbers2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers2 (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers2 VALUES (50, 20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a - b AS diff FROM numbers2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(30));
}

#[test]
fn test_integer_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS numbers3")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers3 (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers3 VALUES (7, 8)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a * b AS product FROM numbers3")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(56));
}

#[test]
fn test_integer_division() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS numbers4")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers4 (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers4 VALUES (20, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS quotient FROM numbers4")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(5));
}

#[test]
fn test_integer_modulo() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS numbers5")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE numbers5 (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers5 VALUES (17, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a % b AS remainder FROM numbers5")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(2));
}

#[test]
fn test_float_addition() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS floats").unwrap();
    executor
        .execute_sql("CREATE TABLE floats (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats VALUES (3.14, 2.86)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS sum FROM floats")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 6.0).abs() < 0.0001);
}

#[test]
fn test_float_subtraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS floats2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE floats2 (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats2 VALUES (10.5, 3.2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a - b AS diff FROM floats2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 7.3).abs() < 0.0001);
}

#[test]
fn test_float_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS floats3")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE floats3 (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats3 VALUES (2.5, 4.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a * b AS product FROM floats3")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 10.0).abs() < 0.0001);
}

#[test]
fn test_float_division() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS floats4")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE floats4 (a FLOAT64, b FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO floats4 VALUES (15.0, 3.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS quotient FROM floats4")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 5.0).abs() < 0.0001);
}

#[test]
fn test_mixed_addition() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS mixed").unwrap();
    executor
        .execute_sql("CREATE TABLE mixed (int_val INT64, float_val FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed VALUES (10, 3.5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT int_val + float_val AS sum FROM mixed")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 13.5).abs() < 0.0001);
}

#[test]
fn test_mixed_subtraction() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS mixed2").unwrap();
    executor
        .execute_sql("CREATE TABLE mixed2 (float_val FLOAT64, int_val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed2 VALUES (20.5, 7)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT float_val - int_val AS diff FROM mixed2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 13.5).abs() < 0.0001);
}

#[test]
fn test_mixed_multiplication() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS mixed3").unwrap();
    executor
        .execute_sql("CREATE TABLE mixed3 (int_val INT64, float_val FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed3 VALUES (5, 2.5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT int_val * float_val AS product FROM mixed3")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 12.5).abs() < 0.0001);
}

#[test]
fn test_mixed_division() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS mixed4").unwrap();
    executor
        .execute_sql("CREATE TABLE mixed4 (int_val INT64, float_val FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed4 VALUES (10, 4.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT int_val / float_val AS quotient FROM mixed4")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - 2.5).abs() < 0.0001);
}

#[test]
fn test_unary_negation_integer() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS unary").unwrap();
    executor
        .execute_sql("CREATE TABLE unary (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unary VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT -value AS negated FROM unary")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(-42));
}

#[test]
fn test_unary_negation_float() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS unary2").unwrap();
    executor
        .execute_sql("CREATE TABLE unary2 (value FLOAT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unary2 VALUES (3.14)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT -value AS negated FROM unary2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();
    assert!((val - (-3.14)).abs() < 0.0001);
}

#[test]
fn test_unary_positive() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS unary3").unwrap();
    executor
        .execute_sql("CREATE TABLE unary3 (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unary3 VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT +value AS positive FROM unary3")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(42));
}

#[test]
fn test_double_negation() {
    let mut executor = create_executor();
    executor.execute_sql("DROP TABLE IF EXISTS unary4").unwrap();
    executor
        .execute_sql("CREATE TABLE unary4 (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unary4 VALUES (42)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT -(-value) AS double_neg FROM unary4")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(42));
}

#[test]
fn test_null_plus_literal() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS null_arith")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_arith (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_arith VALUES (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value + 10 FROM null_arith")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_value_plus_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS null_arith2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_arith2 (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_arith2 VALUES (10, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b FROM null_arith2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_null_multiply() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS null_arith3")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_arith3 (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_arith3 VALUES (NULL, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a * b FROM null_arith3")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_divide_by_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS null_arith4")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_arith4 (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_arith4 VALUES (10, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b FROM null_arith4")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_unary_negation_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS null_unary")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_unary (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_unary VALUES (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT -value FROM null_unary")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert!(col.get(0).unwrap().is_null());
}

#[test]
fn test_precedence_multiply_before_add() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS precedence")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE precedence (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO precedence VALUES (2, 3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b * c AS result FROM precedence")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(14));
}

#[test]
fn test_precedence_divide_before_subtract() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS precedence2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE precedence2 (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO precedence2 VALUES (20, 10, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a - b / c AS result FROM precedence2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(15));
}

#[test]
fn test_precedence_parentheses_add() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS precedence3")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE precedence3 (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO precedence3 VALUES (2, 3, 4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT (a + b) * c AS result FROM precedence3")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(20));
}

#[test]
fn test_precedence_nested_parentheses() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS precedence4")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE precedence4 (a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO precedence4 VALUES (10, 5, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ((a - b) * c) + a AS result FROM precedence4")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(20));
}

#[test]
fn test_all_operators_precedence() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS all_ops")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE all_ops (a INT64, b INT64, c INT64, d INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO all_ops VALUES (10, 5, 3, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b * c - d / 2 AS result FROM all_ops")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(24));
}

#[test]
fn test_arithmetic_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS where_arith")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE where_arith (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO where_arith VALUES (10, 5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO where_arith VALUES (20, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO where_arith VALUES (30, 15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM where_arith WHERE a * 2 > b + 25")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_arithmetic_in_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS order_arith")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE order_arith (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO order_arith VALUES (10, 5)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO order_arith VALUES (5, 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO order_arith VALUES (8, 7)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a, b, a + b AS total FROM order_arith ORDER BY a DESC")
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let total_col = result.column(2).unwrap();
    assert_eq!(total_col.get(0).unwrap().as_i64(), Some(15));
    assert_eq!(total_col.get(1).unwrap().as_i64(), Some(15));
    assert_eq!(total_col.get(2).unwrap().as_i64(), Some(15));
}

#[test]
fn test_literal_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS lit_arith")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE lit_arith (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lit_arith VALUES (100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value + 50 AS result FROM lit_arith")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(150));
}

#[test]
fn test_literal_subtraction_from_literal() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS lit_arith2")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE lit_arith2 (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lit_arith2 VALUES (30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 100 - value AS result FROM lit_arith2")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(70));
}

#[test]
fn test_integer_division_truncation() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS int_div")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE int_div (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO int_div VALUES (10, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS result FROM int_div")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(3));
}

#[test]
fn test_negative_integer_division() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS neg_div")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE neg_div (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neg_div VALUES (-10, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a / b AS result FROM neg_div")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();

    assert_eq!(col.get(0).unwrap().as_i64(), Some(-3));
}

#[test]
fn test_negative_addition() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS neg_add")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE neg_add (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neg_add VALUES (-10, -20)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS result FROM neg_add")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(-30));
}

#[test]
fn test_negative_multiplication() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS neg_mult")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE neg_mult (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO neg_mult VALUES (-5, -4)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a * b AS result FROM neg_mult")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(20));
}

#[test]
fn test_zero_plus_zero() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS zero_ops")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE zero_ops (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO zero_ops VALUES (0, 0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a + b AS result FROM zero_ops")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(0));
}

#[test]
fn test_multiply_by_zero() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS zero_mult")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE zero_mult (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO zero_mult VALUES (12345)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value * 0 AS result FROM zero_mult")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(0));
}

#[test]
fn test_zero_divided_by_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS zero_div")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE zero_div (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO zero_div VALUES (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 0 / value AS result FROM zero_div")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    assert_eq!(col.get(0).unwrap().as_i64(), Some(0));
}

#[test]
fn test_sum_of_products() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS agg_arith")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE agg_arith (price INT64, quantity INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO agg_arith VALUES (10, 3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO agg_arith VALUES (20, 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT price * quantity AS product FROM agg_arith")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    let col = result.column(0).unwrap();

    let product1 = col.get(0).unwrap().as_i64().unwrap();
    let product2 = col.get(1).unwrap().as_i64().unwrap();
    assert_eq!(product1 + product2, 70);
}

#[test]
fn test_avg_of_sums() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP TABLE IF EXISTS agg_avg")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE agg_avg (score1 INT64, score2 INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO agg_avg VALUES (80, 90)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO agg_avg VALUES (70, 80)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(score1 + score2) AS avg_total FROM agg_avg")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    let col = result.column(0).unwrap();
    let val = col.get(0).unwrap().as_f64().unwrap();

    assert!((val - 160.0).abs() < 0.0001);
}
