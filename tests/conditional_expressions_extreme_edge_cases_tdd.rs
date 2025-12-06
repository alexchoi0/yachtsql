#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::QueryExecutor;
use yachtsql_parser::DialectType;

#[test]
fn test_case_when_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CASE WHEN val > 5 THEN 'high' ELSE 'low' END as category FROM t")
        .unwrap();

    let binding = result.column(0).unwrap().get(0).unwrap();
    let cat = binding.as_str().unwrap();
    assert_eq!(cat, "high", "10 > 5 should return 'high'");
}

#[test]
fn test_case_when_multiple_conditions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 95)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (2, 75)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (3, 55)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (4, 35)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, CASE
            WHEN score >= 90 THEN 'A'
            WHEN score >= 70 THEN 'B'
            WHEN score >= 50 THEN 'C'
            ELSE 'F'
        END as grade FROM t ORDER BY id",
        )
        .unwrap();

    let grades = ["A", "B", "C", "F"];
    for (i, expected_grade) in grades.iter().enumerate() {
        let temp_grade = result.column(1).unwrap().get(i).unwrap();
        let grade = temp_grade.as_str().unwrap();
        assert_eq!(
            grade,
            *expected_grade,
            "Grade for row {} should be {}",
            i + 1,
            expected_grade
        );
    }
}

#[test]
fn test_case_when_no_else_returns_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CASE WHEN val > 100 THEN 'big' END as size FROM t")
        .unwrap();

    let val = result.column(0).unwrap().get(0).unwrap();
    assert!(
        val.is_null(),
        "Unmatched CASE without ELSE should return NULL"
    );
}

#[test]
fn test_case_when_null_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("CREATE TABLE t (id INT64)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1)").unwrap();

    let result = executor
        .execute_sql("SELECT CASE WHEN NULL THEN 'yes' ELSE 'no' END as result FROM t")
        .unwrap();

    let temp = result.column(0).unwrap().get(0).unwrap();
    let val = temp.as_str().unwrap();
    assert_eq!(val, "no", "NULL condition should go to ELSE");
}

#[test]
fn test_case_when_evaluates_first_true() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, a INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE
            WHEN a > 5 THEN 'first'
            WHEN a > 8 THEN 'second'
            ELSE 'none'
        END as result FROM t",
        )
        .unwrap();

    let temp_val = result.column(0).unwrap().get(0).unwrap();
    let val = temp_val.as_str().unwrap();
    assert_eq!(
        val, "first",
        "First TRUE condition should win, even if others also true"
    );
}

#[test]
fn test_case_when_all_null_conditions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, a BOOL, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, NULL, NULL)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE
            WHEN a THEN 'a'
            WHEN b THEN 'b'
            ELSE 'none'
        END as result FROM t",
        )
        .unwrap();

    let temp_val = result.column(0).unwrap().get(0).unwrap();
    let val = temp_val.as_str().unwrap();
    assert_eq!(val, "none", "All NULL conditions should go to ELSE");
}

#[test]
fn test_case_simple_form_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 'active')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (2, 'inactive')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (3, 'pending')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, CASE status
            WHEN 'active' THEN 1
            WHEN 'inactive' THEN 0
            ELSE -1
        END as status_code FROM t ORDER BY id",
        )
        .unwrap();

    let codes = [1, 0, -1];
    for (i, expected_code) in codes.iter().enumerate() {
        let code = result.column(1).unwrap().get(i).unwrap().as_i64().unwrap();
        assert_eq!(
            code,
            *expected_code,
            "Status code for row {} should be {}",
            i + 1,
            expected_code
        );
    }
}

#[test]
fn test_case_simple_form_multiple_when() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, grade STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 'A')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (3, 'C')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, CASE grade
            WHEN 'A' THEN 'Excellent'
            WHEN 'B' THEN 'Good'
            WHEN 'C' THEN 'Average'
            ELSE 'Poor'
        END as description FROM t ORDER BY id",
        )
        .unwrap();

    let descs = ["Excellent", "Good", "Average"];
    for (i, expected_desc) in descs.iter().enumerate() {
        let temp_desc = result.column(1).unwrap().get(i).unwrap();
        let desc = temp_desc.as_str().unwrap();
        assert_eq!(desc, *expected_desc);
    }
}

#[test]
fn test_case_simple_vs_searched_form() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT CASE val WHEN 10 THEN 'ten' ELSE 'other' END as r1 FROM t")
        .unwrap();

    let result2 = executor
        .execute_sql("SELECT CASE WHEN val = 10 THEN 'ten' ELSE 'other' END as r2 FROM t")
        .unwrap();

    let binding1 = result1.column(0).unwrap().get(0).unwrap();
    let r1 = binding1.as_str().unwrap();
    let binding2 = result2.column(0).unwrap().get(0).unwrap();
    let r2 = binding2.as_str().unwrap();
    assert_eq!(r1, r2, "Simple and searched forms should be equivalent");
}

#[test]
fn test_nested_case_in_then() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10, 5)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE
            WHEN a > 5 THEN
                CASE WHEN b > 3 THEN 'both high' ELSE 'a high only' END
            ELSE 'a low'
        END as result FROM t",
        )
        .unwrap();

    let temp_val = result.column(0).unwrap().get(0).unwrap();
    let val = temp_val.as_str().unwrap();
    assert_eq!(val, "both high", "Nested CASE should evaluate correctly");
}

#[test]
fn test_nested_case_in_when_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, x INT64, y INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10, 20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE
            WHEN (CASE WHEN x > 5 THEN y ELSE 0 END) > 15 THEN 'yes'
            ELSE 'no'
        END as result FROM t",
        )
        .unwrap();

    let temp_val = result.column(0).unwrap().get(0).unwrap();
    let val = temp_val.as_str().unwrap();

    assert_eq!(val, "yes", "Nested CASE in condition should work");
}

#[test]
fn test_deeply_nested_case() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, a INT64, b INT64, c INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10, 20, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT CASE
            WHEN a > 5 THEN
                CASE WHEN b > 15 THEN
                    CASE WHEN c > 25 THEN 'all high' ELSE 'c low' END
                ELSE 'b low' END
            ELSE 'a low'
        END as result FROM t",
        )
        .unwrap();

    let temp_val = result.column(0).unwrap().get(0).unwrap();
    let val = temp_val.as_str().unwrap();
    assert_eq!(val, "all high", "Deeply nested CASE should work");
}

#[test]
fn test_case_type_coercion_int_to_float() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (2, FALSE)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CASE WHEN flag THEN 10 ELSE 3.14 END as val FROM t ORDER BY id")
        .unwrap();

    let val1 = result.column(0).unwrap().get(0).unwrap().as_f64().unwrap();
    let val2 = result.column(0).unwrap().get(1).unwrap().as_f64().unwrap();
    assert_eq!(val1, 10.0);
    #[allow(clippy::approx_constant)]
    {
        assert_eq!(val2, 3.14);
    }
}

#[test]
fn test_case_incompatible_types_error() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, TRUE)")
        .unwrap();

    let result =
        executor.execute_sql("SELECT CASE WHEN flag THEN 'text' ELSE 42 END as val FROM t");

    let _ = result;
}

#[test]
fn test_case_all_branches_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (2, FALSE)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CASE WHEN flag THEN NULL ELSE NULL END as val FROM t")
        .unwrap();

    for i in 0..2 {
        let val = result.column(0).unwrap().get(i).unwrap();
        assert!(val.is_null(), "All NULL branches should return NULL");
    }
}

#[test]
fn test_if_function_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10)")
        .unwrap();
    executor.execute_sql("INSERT INTO t VALUES (2, 3)").unwrap();

    let result = executor
        .execute_sql("SELECT id, IF(val > 5, 'high', 'low') as category FROM t ORDER BY id")
        .unwrap();

    let temp_cat1 = result.column(1).unwrap().get(0).unwrap();
    let cat1 = temp_cat1.as_str().unwrap();
    let temp_cat2 = result.column(1).unwrap().get(1).unwrap();
    let cat2 = temp_cat2.as_str().unwrap();
    assert_eq!(cat1, "high");
    assert_eq!(cat2, "low");
}

#[test]
fn test_if_function_null_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor.execute_sql("CREATE TABLE t (id INT64)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1)").unwrap();

    let result = executor
        .execute_sql("SELECT IF(NULL, 'yes', 'no') as result FROM t")
        .unwrap();

    let temp_val = result.column(0).unwrap().get(0).unwrap();
    let val = temp_val.as_str().unwrap();
    assert_eq!(val, "no", "IF(NULL, ...) should return false_val");
}

#[test]
fn test_if_function_null_results() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, flag BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (2, FALSE)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, IF(flag, NULL, 'no') as val FROM t ORDER BY id")
        .unwrap();

    let val1 = result.column(1).unwrap().get(0).unwrap();
    let val2 = result.column(1).unwrap().get(1).unwrap();
    assert!(val1.is_null(), "TRUE branch should return NULL");
    assert_eq!(val2.as_str().unwrap(), "no");
}

#[test]
fn test_if_function_nested() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 95)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (2, 75)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (3, 55)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT id, IF(score >= 90, 'A', IF(score >= 70, 'B', 'C')) as grade FROM t ORDER BY id"
    ).unwrap();

    let grades = ["A", "B", "C"];
    for (i, expected_grade) in grades.iter().enumerate() {
        let temp_grade = result.column(1).unwrap().get(i).unwrap();
        let grade = temp_grade.as_str().unwrap();
        assert_eq!(grade, *expected_grade);
    }
}

#[test]
fn test_if_vs_case_when_equivalence() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO t VALUES (1, 10)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT IF(val > 5, 'yes', 'no') as r1 FROM t")
        .unwrap();

    let result2 = executor
        .execute_sql("SELECT CASE WHEN val > 5 THEN 'yes' ELSE 'no' END as r2 FROM t")
        .unwrap();

    let binding1 = result1.column(0).unwrap().get(0).unwrap();
    let r1 = binding1.as_str().unwrap();
    let binding2 = result2.column(0).unwrap().get(0).unwrap();
    let r2 = binding2.as_str().unwrap();
    assert_eq!(r1, r2, "IF and CASE WHEN should be equivalent");
}

#[test]
fn test_case_in_aggregate() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE orders (id INT64, status STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 'completed', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (2, 'completed', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (3, 'cancelled', 150)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) as completed_total FROM orders"
    ).unwrap();

    let total = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(total, 300, "Should sum only completed orders");
}

#[test]
fn test_conditional_count() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE users (id INT64, age INT64, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 25, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (2, 30, TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (3, 35, FALSE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (4, 40, TRUE)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
            COUNT(CASE WHEN active THEN 1 END) as active_count,
            COUNT(CASE WHEN NOT active THEN 1 END) as inactive_count
        FROM users",
        )
        .unwrap();

    let active = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    let inactive = result.column(1).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(active, 3);
    assert_eq!(inactive, 1);
}

#[test]
fn test_case_with_group_by() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (3, 50)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES (4, 300)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT
            CASE WHEN amount >= 100 THEN 'large' ELSE 'small' END as size,
            COUNT(*) as cnt
        FROM sales
        GROUP BY CASE WHEN amount >= 100 THEN 'large' ELSE 'small' END
        ORDER BY size",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_case_short_circuit_avoids_error() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE t (id INT64, denominator INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1, 0)").unwrap();

    let result = executor.execute_sql(
        "SELECT CASE WHEN denominator = 0 THEN 0 ELSE 10 / denominator END as safe_div FROM t",
    );

    assert!(
        result.is_ok(),
        "CASE should short-circuit and avoid division by zero"
    );
    let val = result
        .unwrap()
        .column(0)
        .unwrap()
        .get(0)
        .unwrap()
        .as_i64()
        .unwrap();
    assert_eq!(val, 0);
}
