mod common;

use common::{get_i64, get_string};
use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

mod basic_do_nothing {
    use super::*;

    #[test]
    fn test_basic_on_conflict_do_nothing() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS users").unwrap();
        executor
            .execute_sql("CREATE TABLE users (id INT64 PRIMARY KEY, name STRING, email STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO users VALUES (1, 'Bob', 'bob@example.com')
                 ON CONFLICT (id) DO NOTHING",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT name FROM users WHERE id = 1")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_string(&result, 0, 0), "Alice");
    }

    #[test]
    fn test_do_nothing_no_conflict() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
        executor
            .execute_sql("CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO items VALUES (1, 'First')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO items VALUES (2, 'Second')
                 ON CONFLICT (id) DO NOTHING",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM items")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 2);
    }

    #[test]
    fn test_do_nothing_on_unique_constraint() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS emails").unwrap();
        executor
            .execute_sql("CREATE TABLE emails (id INT64, email STRING UNIQUE, verified BOOL)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO emails VALUES (1, 'test@example.com', false)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO emails VALUES (2, 'test@example.com', true)
                 ON CONFLICT (email) DO NOTHING",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM emails")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 1);

        let result = executor
            .execute_sql("SELECT id, verified FROM emails WHERE email = 'test@example.com'")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 1);
    }
}

mod basic_do_update {
    use super::*;

    #[test]
    fn test_basic_on_conflict_do_update() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS equipment")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE equipment (id INT64 PRIMARY KEY, name STRING, stock INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO equipment VALUES (1, 'Widget', 100)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO equipment VALUES (1, 'Widget', 50)
                 ON CONFLICT (id) DO UPDATE SET stock = EXCLUDED.stock",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT stock FROM equipment WHERE id = 1")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 50);
    }

    #[test]
    fn test_do_update_multiple_columns() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS inventory")
            .unwrap();
        executor
            .execute_sql(
                "CREATE TABLE inventory (sku STRING PRIMARY KEY, quantity INT64, price FLOAT64)",
            )
            .unwrap();
        executor
            .execute_sql("INSERT INTO inventory VALUES ('ABC123', 10, 19.99)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO inventory VALUES ('ABC123', 5, 24.99)
                 ON CONFLICT (sku) DO UPDATE
                 SET quantity = EXCLUDED.quantity,
                     price = EXCLUDED.price",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT quantity, price FROM inventory WHERE sku = 'ABC123'")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 5);

        let price = result.column(1).unwrap().get(0).unwrap().as_f64().unwrap();
        assert!((price - 24.99).abs() < 0.01);
    }

    #[test]
    fn test_do_update_no_conflict_inserts() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
        executor
            .execute_sql("CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO items VALUES (1, 'First')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO items VALUES (2, 'Second')
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM items")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 2);

        let result = executor
            .execute_sql("SELECT name FROM items WHERE id = 2")
            .unwrap();
        assert_eq!(get_string(&result, 0, 0), "Second");
    }
}

mod excluded_keyword {
    use super::*;

    #[test]
    fn test_excluded_combined_with_existing() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS counters")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE counters (id INT64 PRIMARY KEY, count INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO counters VALUES (1, 10)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO counters VALUES (1, 5)
                 ON CONFLICT (id) DO UPDATE SET count = counters.count + EXCLUDED.count",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT count FROM counters WHERE id = 1")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 15);
    }

    #[test]
    fn test_excluded_with_computed_values() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS stats").unwrap();
        executor
            .execute_sql(
                "CREATE TABLE stats (id INT64 PRIMARY KEY, total INT64, count INT64, avg FLOAT64)",
            )
            .unwrap();
        executor
            .execute_sql("INSERT INTO stats VALUES (1, 100, 10, 10.0)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO stats VALUES (1, 50, 5, 0.0)
                 ON CONFLICT (id) DO UPDATE
                 SET total = stats.total + EXCLUDED.total,
                     count = stats.count + EXCLUDED.count,
                     avg = CAST((stats.total + EXCLUDED.total) AS FLOAT64) / CAST((stats.count + EXCLUDED.count) AS FLOAT64)",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT total, count, avg FROM stats WHERE id = 1")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 150);
        assert_eq!(get_i64(&result, 1, 0), 15);

        let avg = result.column(2).unwrap().get(0).unwrap().as_f64().unwrap();
        assert!((avg - 10.0).abs() < 0.01);
    }

    #[test]
    fn test_excluded_on_unique_constraint() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS emails").unwrap();
        executor
            .execute_sql("CREATE TABLE emails (id INT64, email STRING UNIQUE, verified BOOL)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO emails VALUES (1, 'test@example.com', false)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO emails VALUES (2, 'test@example.com', true)
                 ON CONFLICT (email) DO UPDATE SET verified = EXCLUDED.verified",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT id, verified FROM emails WHERE email = 'test@example.com'")
            .unwrap();

        assert_eq!(result.num_rows(), 1);

        assert_eq!(get_i64(&result, 0, 0), 1);

        let verified = result.column(1).unwrap().get(0).unwrap().as_bool().unwrap();
        assert!(verified);
    }
}

mod where_clause {
    use super::*;

    #[test]
    fn test_where_clause_condition_met() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
        executor
            .execute_sql("CREATE TABLE data (id INT64 PRIMARY KEY, value INT64, updated_at INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO data VALUES (1, 100, 1000)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO data VALUES (1, 50, 2000)
                 ON CONFLICT (id) DO UPDATE
                 SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                 WHERE data.updated_at < EXCLUDED.updated_at",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT value, updated_at FROM data WHERE id = 1")
            .unwrap();

        assert_eq!(result.num_rows(), 1);

        assert_eq!(get_i64(&result, 0, 0), 50);
        assert_eq!(get_i64(&result, 1, 0), 2000);
    }

    #[test]
    fn test_where_clause_condition_not_met() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS data").unwrap();
        executor
            .execute_sql("CREATE TABLE data (id INT64 PRIMARY KEY, value INT64, updated_at INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO data VALUES (1, 100, 2000)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO data VALUES (1, 50, 1000)
                 ON CONFLICT (id) DO UPDATE
                 SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at
                 WHERE data.updated_at < EXCLUDED.updated_at",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT value, updated_at FROM data WHERE id = 1")
            .unwrap();

        assert_eq!(result.num_rows(), 1);

        assert_eq!(get_i64(&result, 0, 0), 100);
        assert_eq!(get_i64(&result, 1, 0), 2000);
    }

    #[test]
    fn test_where_clause_last_write_wins() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS cache").unwrap();
        executor
            .execute_sql(
                "CREATE TABLE cache (key STRING PRIMARY KEY, value STRING, timestamp INT64)",
            )
            .unwrap();
        executor
            .execute_sql("INSERT INTO cache VALUES ('user:1', 'old_data', 1000)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO cache VALUES ('user:1', 'new_data', 2000)
                 ON CONFLICT (key) DO UPDATE
                 SET value = EXCLUDED.value, timestamp = EXCLUDED.timestamp
                 WHERE cache.timestamp < EXCLUDED.timestamp",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT value FROM cache WHERE key = 'user:1'")
            .unwrap();
        assert_eq!(get_string(&result, 0, 0), "new_data");

        executor
            .execute_sql(
                "INSERT INTO cache VALUES ('user:1', 'stale_data', 1500)
                 ON CONFLICT (key) DO UPDATE
                 SET value = EXCLUDED.value, timestamp = EXCLUDED.timestamp
                 WHERE cache.timestamp < EXCLUDED.timestamp",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT value, timestamp FROM cache WHERE key = 'user:1'")
            .unwrap();

        assert_eq!(get_string(&result, 0, 0), "new_data");
        assert_eq!(get_i64(&result, 1, 0), 2000);
    }
}

mod multi_column_constraint {
    use super::*;

    #[test]
    fn test_multi_column_unique_conflict() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS bookings")
            .unwrap();
        executor
            .execute_sql(
                "CREATE TABLE bookings (room_id INT64, date INT64, guest STRING, UNIQUE(room_id, date))",
            )
            .unwrap();
        executor
            .execute_sql("INSERT INTO bookings VALUES (101, 20240115, 'Alice')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO bookings VALUES (101, 20240115, 'Bob')
                 ON CONFLICT (room_id, date) DO UPDATE SET guest = EXCLUDED.guest",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT guest FROM bookings WHERE room_id = 101 AND date = 20240115")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_string(&result, 0, 0), "Bob");
    }

    #[test]
    fn test_multi_column_unique_no_conflict() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS bookings")
            .unwrap();
        executor
            .execute_sql(
                "CREATE TABLE bookings (room_id INT64, date INT64, guest STRING, UNIQUE(room_id, date))",
            )
            .unwrap();
        executor
            .execute_sql("INSERT INTO bookings VALUES (101, 20240115, 'Alice')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO bookings VALUES (101, 20240116, 'Bob')
                 ON CONFLICT (room_id, date) DO UPDATE SET guest = EXCLUDED.guest",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM bookings")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 2);

        executor
            .execute_sql(
                "INSERT INTO bookings VALUES (102, 20240115, 'Charlie')
                 ON CONFLICT (room_id, date) DO UPDATE SET guest = EXCLUDED.guest",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM bookings")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 3);
    }

    #[test]
    fn test_multi_column_unique_do_nothing() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS reservations")
            .unwrap();
        executor
            .execute_sql(
                "CREATE TABLE reservations (date INT64, slot INT64, name STRING, UNIQUE(date, slot))",
            )
            .unwrap();
        executor
            .execute_sql("INSERT INTO reservations VALUES (20240115, 1, 'Alice')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO reservations VALUES (20240115, 1, 'Bob')
                 ON CONFLICT (date, slot) DO NOTHING",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT name FROM reservations WHERE date = 20240115 AND slot = 1")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_string(&result, 0, 0), "Alice");
    }
}

mod multiple_rows {
    use super::*;

    #[test]
    fn test_insert_multiple_rows_with_conflict() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
        executor
            .execute_sql("CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO items VALUES (1, 'First'), (2, 'Second')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO items VALUES (1, 'Updated1'), (2, 'Updated2'), (3, 'New')
                 ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT id, name FROM items ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 3);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "Updated1");
        assert_eq!(get_i64(&result, 0, 1), 2);
        assert_eq!(get_string(&result, 1, 1), "Updated2");
        assert_eq!(get_i64(&result, 0, 2), 3);
        assert_eq!(get_string(&result, 1, 2), "New");
    }

    #[test]
    fn test_insert_multiple_rows_do_nothing() {
        let mut executor = create_executor();

        executor.execute_sql("DROP TABLE IF EXISTS items").unwrap();
        executor
            .execute_sql("CREATE TABLE items (id INT64 PRIMARY KEY, name STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO items VALUES (1, 'First')")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO items VALUES (1, 'Conflict'), (2, 'New')
                 ON CONFLICT (id) DO NOTHING",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT id, name FROM items ORDER BY id")
            .unwrap();

        assert_eq!(result.num_rows(), 2);
        assert_eq!(get_string(&result, 1, 0), "First");
        assert_eq!(get_string(&result, 1, 1), "New");
    }
}

mod null_handling {
    use super::*;

    #[test]
    fn test_nulls_dont_conflict_in_unique() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS nullable_unique")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE nullable_unique (id INT64, email STRING UNIQUE)")
            .unwrap();

        executor
            .execute_sql("INSERT INTO nullable_unique VALUES (1, NULL)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO nullable_unique VALUES (2, NULL)")
            .unwrap();

        let result = executor
            .execute_sql("SELECT COUNT(*) as cnt FROM nullable_unique WHERE email IS NULL")
            .unwrap();

        assert_eq!(get_i64(&result, 0, 0), 2);
    }
}

mod counter_pattern {
    use super::*;

    #[test]
    fn test_counter_increment_existing() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS view_counts")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE view_counts (page_id INT64 PRIMARY KEY, views INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO view_counts VALUES (1, 5)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO view_counts VALUES (1, 1)
                 ON CONFLICT (page_id) DO UPDATE SET views = view_counts.views + 1",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT views FROM view_counts WHERE page_id = 1")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 6);

        executor
            .execute_sql(
                "INSERT INTO view_counts VALUES (1, 1)
                 ON CONFLICT (page_id) DO UPDATE SET views = view_counts.views + 1",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT views FROM view_counts WHERE page_id = 1")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 7);
    }

    #[test]
    fn test_counter_create_if_not_exists() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS view_counts")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE view_counts (page_id INT64 PRIMARY KEY, views INT64)")
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO view_counts VALUES (1, 1)
                 ON CONFLICT (page_id) DO UPDATE SET views = view_counts.views + 1",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT views FROM view_counts WHERE page_id = 1")
            .unwrap();
        assert_eq!(get_i64(&result, 0, 0), 1);
    }
}

mod error_conditions {
    use common::assert_error_contains;

    use super::*;

    #[test]
    fn test_error_on_conflict_without_constraint() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS no_constraint")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE no_constraint (id INT64, value STRING)")
            .unwrap();

        let result = executor.execute_sql(
            "INSERT INTO no_constraint VALUES (1, 'test')
             ON CONFLICT DO NOTHING",
        );

        assert_error_contains(result, &["conflict", "target", "constraint", "specify"]);
    }

    #[test]
    fn test_error_on_nonexistent_conflict_column() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS test_table")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE test_table (id INT64 PRIMARY KEY)")
            .unwrap();

        let result = executor.execute_sql(
            "INSERT INTO test_table VALUES (1)
             ON CONFLICT (nonexistent_column) DO NOTHING",
        );

        assert_error_contains(result, &["column", "not found", "nonexistent"]);
    }
}

mod returning_clause {
    use super::*;

    #[test]
    fn test_returning_on_update() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS records")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE records (id INT64 PRIMARY KEY, value STRING)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO records VALUES (1, 'original')")
            .unwrap();

        let result = executor
            .execute_sql(
                "INSERT INTO records VALUES (1, 'updated')
                 ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value
                 RETURNING *",
            )
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "updated");
    }

    #[test]
    fn test_returning_on_insert() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS records")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE records (id INT64 PRIMARY KEY, value STRING)")
            .unwrap();

        let result = executor
            .execute_sql(
                "INSERT INTO records VALUES (1, 'new')
                 ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value
                 RETURNING *",
            )
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_string(&result, 1, 0), "new");
    }

    #[test]
    fn test_returning_specific_columns() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS records")
            .unwrap();
        executor
            .execute_sql("CREATE TABLE records (id INT64 PRIMARY KEY, name STRING, value INT64)")
            .unwrap();
        executor
            .execute_sql("INSERT INTO records VALUES (1, 'test', 100)")
            .unwrap();

        let result = executor
            .execute_sql(
                "INSERT INTO records VALUES (1, 'test', 200)
                 ON CONFLICT (id) DO UPDATE SET value = EXCLUDED.value
                 RETURNING id, value",
            )
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.num_columns(), 2);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 1, 0), 200);
    }
}

mod idempotent_operations {
    use super::*;

    #[test]
    fn test_idempotent_user_creation() {
        let mut executor = create_executor();

        executor
            .execute_sql("DROP TABLE IF EXISTS app_users")
            .unwrap();
        executor
            .execute_sql(
                "CREATE TABLE app_users (id INT64, email STRING PRIMARY KEY, created_at INT64)",
            )
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO app_users VALUES (1, 'alice@example.com', 1000)
                 ON CONFLICT (email) DO NOTHING",
            )
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO app_users VALUES (2, 'alice@example.com', 2000)
                 ON CONFLICT (email) DO NOTHING",
            )
            .unwrap();

        executor
            .execute_sql(
                "INSERT INTO app_users VALUES (3, 'alice@example.com', 3000)
                 ON CONFLICT (email) DO NOTHING",
            )
            .unwrap();

        let result = executor
            .execute_sql("SELECT id, created_at FROM app_users WHERE email = 'alice@example.com'")
            .unwrap();

        assert_eq!(result.num_rows(), 1);
        assert_eq!(get_i64(&result, 0, 0), 1);
        assert_eq!(get_i64(&result, 1, 0), 1000);
    }
}
