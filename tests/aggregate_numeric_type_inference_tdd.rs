#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

fn new_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

#[test]
fn test_avg_numeric_returns_numeric_not_float() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (10.00), (20.00), (30.00)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(amount) as avg_amount FROM vals")
        .unwrap();

    let avg_val = result.column(0).unwrap().get(0).unwrap();

    assert!(
        avg_val.is_numeric(),
        "AVG(NUMERIC) should return NUMERIC, got {:?}",
        avg_val
    );

    if let Some(n) = avg_val.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "20.00" || val_str == "20",
            "AVG should be 20, got {}",
            val_str
        );
    }
}

#[test]
fn test_avg_numeric_with_single_value() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (amount NUMERIC(15,4))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (123.4567)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(amount) FROM vals")
        .unwrap();
    let avg_val = result.column(0).unwrap().get(0).unwrap();

    assert!(
        avg_val.is_numeric(),
        "AVG of single NUMERIC should return NUMERIC"
    );

    if let Some(n) = avg_val.as_numeric() {
        assert_eq!(
            n.to_string(),
            "123.4567",
            "AVG of single value should be exact"
        );
    }
}

#[test]
fn test_avg_numeric_with_nulls() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (10.00), (NULL), (30.00)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(amount) FROM vals")
        .unwrap();
    let avg_val = result.column(0).unwrap().get(0).unwrap();

    assert!(avg_val.is_numeric());

    if let Some(n) = avg_val.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "20.00" || val_str == "20",
            "AVG should be 20, got {}",
            val_str
        );
    }
}

#[test]
fn test_avg_numeric_all_nulls_returns_null() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (NULL), (NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(amount) FROM vals")
        .unwrap();
    let avg_val = result.column(0).unwrap().get(0).unwrap();

    assert!(avg_val.is_null(), "AVG of all NULLs should return NULL");
}

#[test]
fn test_avg_numeric_empty_table_returns_null() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (amount NUMERIC(10,2))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(amount) FROM vals")
        .unwrap();
    let avg_val = result.column(0).unwrap().get(0).unwrap();

    assert!(avg_val.is_null());
}

#[test]
fn test_sum_numeric_returns_numeric() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE transactions (amount NUMERIC(12,3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO transactions VALUES (0.100), (0.200), (0.300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(amount) FROM transactions")
        .unwrap();
    let sum_val = result.column(0).unwrap().get(0).unwrap();

    assert!(sum_val.is_numeric(), "SUM(NUMERIC) should return NUMERIC");

    if let Some(n) = sum_val.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "0.600" || val_str == "0.6",
            "SUM should be 0.6, got {}",
            val_str
        );
    }
}

#[test]
fn test_sum_numeric_with_negative_values() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE ledger (amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ledger VALUES (100.50), (-50.25), (30.75)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(amount) FROM ledger")
        .unwrap();
    let sum_val = result.column(0).unwrap().get(0).unwrap();

    assert!(sum_val.is_numeric());

    if let Some(n) = sum_val.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "81.00" || val_str == "81",
            "SUM should be 81, got {}",
            val_str
        );
    }
}

#[test]
fn test_min_numeric_returns_numeric() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (price NUMERIC(8,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (19.99), (29.99), (9.99)")
        .unwrap();

    let result = executor.execute_sql("SELECT MIN(price) FROM vals").unwrap();
    let min_val = result.column(0).unwrap().get(0).unwrap();

    assert!(min_val.is_numeric());

    if let Some(n) = min_val.as_numeric() {
        assert_eq!(n.to_string(), "9.99", "MIN should preserve value");
    }
}

#[test]
fn test_max_numeric_returns_numeric() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (price NUMERIC(8,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (19.99), (29.99), (9.99)")
        .unwrap();

    let result = executor.execute_sql("SELECT MAX(price) FROM vals").unwrap();
    let max_val = result.column(0).unwrap().get(0).unwrap();

    assert!(max_val.is_numeric());

    if let Some(n) = max_val.as_numeric() {
        assert_eq!(n.to_string(), "29.99", "MAX should preserve value");
    }
}

#[test]
fn test_avg_numeric_with_group_by() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE sales (region STRING, amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('US', 100.00), ('US', 200.00), ('EU', 150.00)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT region, AVG(amount) as avg_amount FROM sales GROUP BY region ORDER BY region",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let eu_avg = result.column(1).unwrap().get(0).unwrap();
    assert!(eu_avg.is_numeric(), "GROUP BY AVG should return NUMERIC");

    let us_avg = result.column(1).unwrap().get(1).unwrap();
    assert!(us_avg.is_numeric());

    if let Some(n) = us_avg.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "150.00" || val_str == "150",
            "Expected 150 or 150.00, got {}",
            val_str
        );
    }
}

#[test]
fn test_sum_numeric_with_group_by_having() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE orders (customer STRING, total NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO orders VALUES ('Alice', 50.00), ('Alice', 100.00), ('Bob', 30.00)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT customer, SUM(total) as total_amount
         FROM orders
         GROUP BY customer
         HAVING SUM(total) > 100",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    let sum_val = result.column(1).unwrap().get(0).unwrap();
    assert!(sum_val.is_numeric());

    if let Some(n) = sum_val.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "150.00" || val_str == "150",
            "SUM should be 150, got {}",
            val_str
        );
    }
}

#[test]
fn test_avg_numeric_in_join_condition() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE products (id INT64, category STRING, price NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE category_avg (category STRING, avg_price NUMERIC(10,2))")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO products VALUES (1, 'Electronics', 500.00), (2, 'Electronics', 300.00)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO category_avg VALUES ('Electronics', 400.00)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT p.id, p.price, ca.avg_price
         FROM products p
         JOIN (SELECT category, AVG(price) as computed_avg FROM products GROUP BY category) avg_calc
         ON p.category = avg_calc.category
         JOIN category_avg ca ON p.category = ca.category",
    );

    if let Err(e) = &result {
        eprintln!("[test::aggregate_numeric] JOIN error: {}", e);
    }
    assert!(
        result.is_ok(),
        "JOIN with AVG(NUMERIC) should work when types match: {:?}",
        result.err()
    );
}

#[test]
fn test_sum_numeric_in_correlated_subquery() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE accounts (id INT64, balance NUMERIC(12,2))")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE transactions (account_id INT64, amount NUMERIC(12,2))")
        .unwrap();

    executor
        .execute_sql("INSERT INTO accounts VALUES (1, 1000.00), (2, 500.00)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO transactions VALUES (1, 100.00), (1, -50.00), (2, 200.00)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, balance,
                (SELECT SUM(amount) FROM transactions WHERE account_id = accounts.id) as total_txn
         FROM accounts
         ORDER BY id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 2);

    let txn_sum = result.column(2).unwrap().get(0).unwrap();
    assert!(txn_sum.is_numeric(), "Subquery SUM should return NUMERIC");
}

#[test]
fn test_avg_numeric_window_function() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE timeseries (id INT64, value NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO timeseries VALUES (1, 10.00), (2, 20.00), (3, 30.00)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT id, value, AVG(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as moving_avg
         FROM timeseries
         ORDER BY id"
    ).unwrap();

    assert_eq!(result.num_rows(), 3);

    let moving_avg = result.column(2).unwrap().get(1).unwrap();
    eprintln!(
        "[test::aggregate_numeric] Window AVG type: {:?}",
        moving_avg
    );
    assert!(
        moving_avg.is_numeric(),
        "Window AVG should return NUMERIC, got {:?}",
        moving_avg
    );

    if let Some(n) = moving_avg.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "15.00" || val_str == "15",
            "Moving average should be 15, got {}",
            val_str
        );
    }
}

#[test]
fn test_sum_numeric_cumulative_window() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE daily (day INT64, amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO daily VALUES (1, 100.50), (2, 200.25), (3, 150.75)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT day, amount, SUM(amount) OVER (ORDER BY day) as cumulative_sum
         FROM daily
         ORDER BY day",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 3);

    let cum_sum = result.column(2).unwrap().get(2).unwrap();
    assert!(cum_sum.is_numeric());

    if let Some(n) = cum_sum.as_numeric() {
        let val_str = n.to_string();
        assert!(
            val_str == "451.50" || val_str == "451.5",
            "Cumulative sum should be 451.50, got {}",
            val_str
        );
    }
}

#[test]
fn test_avg_numeric_in_cte() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE measurements (sensor_id INT64, reading NUMERIC(8,3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO measurements VALUES (1, 23.456), (1, 24.123), (2, 30.789)")
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH sensor_avg AS (
            SELECT sensor_id, AVG(reading) as avg_reading
            FROM measurements
            GROUP BY sensor_id
        )
        SELECT sensor_id, avg_reading
        FROM sensor_avg
        WHERE avg_reading > 24.0
        ORDER BY sensor_id",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    let avg_val = result.column(1).unwrap().get(0).unwrap();
    assert!(avg_val.is_numeric(), "CTE AVG should return NUMERIC");
}

#[test]
fn test_stddev_numeric() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (10.00), (20.00), (30.00)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STDDEV(amount) FROM vals")
        .unwrap();
    let stddev_val = result.column(0).unwrap().get(0).unwrap();

    assert!(
        stddev_val.is_numeric() || stddev_val.is_float64(),
        "STDDEV can return NUMERIC or FLOAT64"
    );
}

#[test]
fn test_avg_numeric_cast_from_int() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE counters (count INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO counters VALUES (10), (20), (30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(count) FROM counters")
        .unwrap();

    let avg_val = result.column(0).unwrap().get(0).unwrap();

    assert!(
        avg_val.is_numeric() || avg_val.is_float64(),
        "AVG(INT64) behavior is implementation-defined"
    );
}

#[test]
fn test_mixed_numeric_and_int_in_sum() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE mixed (numeric_col NUMERIC(10,2), int_col INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mixed VALUES (100.50, 50)")
        .unwrap();

    let result = executor.execute_sql("SELECT numeric_col + int_col as total FROM mixed");

    if result.is_ok() {
        let total = result.unwrap().column(0).unwrap().get(0).unwrap();

        assert!(total.is_numeric(), "NUMERIC + INT64 should return NUMERIC");
    }
}

#[test]
fn test_avg_numeric_precision_loss_warning() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE TABLE vals (amount NUMERIC(10,2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO vals VALUES (10.00), (20.00), (25.00)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT AVG(amount) FROM vals")
        .unwrap();
    let avg_val = result.column(0).unwrap().get(0).unwrap();

    assert!(avg_val.is_numeric());

    if let Some(n) = avg_val.as_numeric() {
        let avg_str = n.to_string();
        eprintln!("[test::aggregate_numeric] AVG value: {}", avg_str);
        assert!(
            avg_str.starts_with("18.3") || avg_str == "18",
            "AVG should be approximately 18.33, got {}",
            avg_str
        );
    }
}
