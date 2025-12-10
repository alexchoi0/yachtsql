use std::thread;
use std::time::Duration;

use yachtsql_executor::{QueryExecutor, ResourceLimitsConfig};

#[test]
fn test_query_timeout_exceeded() {
    use yachtsql_executor::resource_limits::ResourceTracker;

    let config = ResourceLimitsConfig {
        max_memory_bytes: Some(1024 * 1024 * 1024),
        timeout: Some(Duration::from_millis(10)),
        enable_memory_tracking: true,
    };

    let tracker = ResourceTracker::new(config);

    assert!(tracker.check_timeout().is_ok());

    thread::sleep(Duration::from_millis(50));

    let result = tracker.check_timeout();
    assert!(result.is_err(), "Should fail after timeout");
    assert!(
        result.unwrap_err().to_string().contains("timeout"),
        "Error should mention timeout"
    );
}

#[test]
fn test_memory_limit_exceeded() {
    use yachtsql_executor::resource_limits::ResourceTracker;

    let config = ResourceLimitsConfig::new(10, 60);
    let tracker = ResourceTracker::new(config);

    assert!(tracker.allocate(5 * 1024 * 1024).is_ok());

    let result = tracker.allocate(10 * 1024 * 1024);
    assert!(result.is_err(), "Should exceed memory limit");
    assert!(
        result.unwrap_err().to_string().contains("memory"),
        "Error should mention memory limit"
    );
}

#[test]
fn test_query_within_limits() {
    let config = ResourceLimitsConfig::new(1024, 60);
    let mut executor = QueryExecutor::new().with_resource_limits(config);

    executor
        .execute_sql("CREATE TABLE normal_test (id INT64, name STRING)")
        .unwrap();

    for i in 0..100 {
        executor
            .execute_sql(&format!(
                "INSERT INTO normal_test VALUES ({}, 'name_{}')",
                i, i
            ))
            .unwrap();
    }

    let result = executor.execute_sql("SELECT * FROM normal_test WHERE id < 50");
    assert!(result.is_ok(), "Normal query should succeed");

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 50, "Should return 50 rows");

    let result = executor.execute_sql("SELECT COUNT(*) FROM normal_test");
    assert!(result.is_ok(), "Aggregate query should succeed");

    let result = executor.execute_sql("UPDATE normal_test SET name = 'updated' WHERE id < 10");
    assert!(result.is_ok(), "UPDATE should succeed");

    let result = executor.execute_sql("DELETE FROM normal_test WHERE id >= 90");
    assert!(result.is_ok(), "DELETE should succeed");
}

#[test]
fn test_unlimited_config() {
    use yachtsql_executor::resource_limits::ResourceTracker;

    let config = ResourceLimitsConfig::unlimited();
    let tracker = ResourceTracker::new(config);

    assert!(tracker.allocate(10 * 1024 * 1024 * 1024).is_ok());

    thread::sleep(Duration::from_millis(10));
    assert!(tracker.check_timeout().is_ok());
}

#[test]
fn test_production_config() {
    let config = ResourceLimitsConfig::new(256, 10);
    let mut executor = QueryExecutor::new().with_resource_limits(config);

    executor
        .execute_sql("CREATE TABLE prod_test (id INT64, value STRING)")
        .unwrap();

    for i in 0..100 {
        executor
            .execute_sql(&format!(
                "INSERT INTO prod_test VALUES ({}, 'value_{}')",
                i, i
            ))
            .unwrap();
    }

    let result = executor.execute_sql("SELECT COUNT(*) FROM prod_test");
    assert!(
        result.is_ok(),
        "Normal query should work in production config"
    );
}

#[test]
fn test_query_cancellation() {
    use yachtsql_executor::resource_limits::{ResourceLimitsConfig, ResourceTracker};

    let config = ResourceLimitsConfig::new(1024, 60);
    let tracker = ResourceTracker::new(config);

    assert!(tracker.check_cancellation().is_ok());

    tracker.cancellation_token().cancel();

    let result = tracker.check_cancellation();
    assert!(result.is_err(), "Should fail after cancellation");
    assert!(
        result.unwrap_err().to_string().contains("cancelled"),
        "Error should mention cancellation"
    );
}

#[test]
fn test_resource_stats_logging() {
    use yachtsql_executor::resource_limits::{ResourceLimitsConfig, ResourceTracker};

    let config = ResourceLimitsConfig::new(1024, 60);
    let tracker = ResourceTracker::new(config);

    tracker.record_batch(100);
    tracker.allocate(1024 * 1024).unwrap();

    let stats = tracker.stats();
    assert_eq!(stats.batch_count, 1);
    assert_eq!(stats.row_count, 100);
    assert_eq!(stats.memory_bytes, 1024 * 1024);
    assert_eq!(stats.peak_memory_bytes, 1024 * 1024);

    drop(tracker);
}

#[test]
fn test_memory_tracking_disabled() {
    use yachtsql_executor::resource_limits::{ResourceLimitsConfig, ResourceTracker};

    let config = ResourceLimitsConfig {
        max_memory_bytes: Some(1024),
        timeout: None,
        enable_memory_tracking: false,
    };
    let tracker = ResourceTracker::new(config);

    assert!(tracker.allocate(1_000_000_000).is_ok());
    assert_eq!(tracker.memory_bytes(), 0, "Memory should not be tracked");
}

#[test]
fn test_resource_stats_format() {
    use yachtsql_executor::resource_limits::{ResourceLimitsConfig, ResourceTracker};

    let config = ResourceLimitsConfig::new(1024, 60);
    let tracker = ResourceTracker::new(config);

    tracker.record_batch(1000);
    tracker.allocate(1024 * 1024 * 100).unwrap();

    let stats = tracker.stats();
    let formatted = stats.format();

    assert!(formatted.contains("MB"), "Should show memory in MB");
    assert!(formatted.contains("1000"), "Should show row count");
    assert!(formatted.contains("batches"), "Should mention batches");
}

#[test]
fn test_end_to_end_query_with_resource_tracking() {
    let config = ResourceLimitsConfig::new(1024, 60);
    let mut executor = QueryExecutor::new().with_resource_limits(config);

    executor
        .execute_sql("CREATE TABLE e2e_test (id INT64, value STRING)")
        .unwrap();

    for i in 0..1000 {
        executor
            .execute_sql(&format!(
                "INSERT INTO e2e_test VALUES ({}, 'value_{}')",
                i, i
            ))
            .unwrap();
    }

    let start = std::time::Instant::now();
    let result = executor.execute_sql("SELECT * FROM e2e_test WHERE id < 500");
    let elapsed = start.elapsed();

    assert!(result.is_ok(), "Query should succeed");
    let batch = result.unwrap();

    println!(
        "Query completed in {:.3}s: {} rows, {} cols",
        elapsed.as_secs_f64(),
        batch.num_rows(),
        batch.num_columns()
    );

    assert_eq!(batch.num_rows(), 500, "Should return 500 rows");
    assert!(elapsed.as_secs() < 5, "Should complete quickly");
}

#[test]
fn test_end_to_end_memory_with_large_strings() {
    let config = ResourceLimitsConfig::new(5, 60);
    let mut executor = QueryExecutor::new().with_resource_limits(config);

    executor
        .execute_sql("CREATE TABLE memory_heavy (id INT64, data STRING)")
        .unwrap();

    let medium_string = "x".repeat(1000);
    for i in 0..100 {
        executor
            .execute_sql(&format!(
                "INSERT INTO memory_heavy VALUES ({}, '{}')",
                i, medium_string
            ))
            .unwrap();
    }

    let result = executor.execute_sql("SELECT * FROM memory_heavy");

    match result {
        Ok(batch) => {
            println!(
                "Query succeeded: {} rows, {} cols",
                batch.num_rows(),
                batch.num_columns()
            );
            assert!(batch.num_rows() <= 100);
        }
        Err(e) => {
            let err_str = e.to_string();
            assert!(
                err_str.contains("memory") || err_str.contains("limit"),
                "If query fails, should be due to memory, got: {}",
                err_str
            );
        }
    }
}
