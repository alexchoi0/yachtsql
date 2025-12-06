mod common;

use common::get_i64;
use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

mod check_not_enforced {
    use super::*;

    #[test]
    fn test_create_table_with_not_enforced_check() {
        let mut executor = create_executor();

        let result = executor.execute_sql(
            "CREATE TABLE products (
                id INT PRIMARY KEY,
                price DECIMAL(10,2),
                name TEXT,
                CHECK (price > 0) NOT ENFORCED
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TABLE with NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_not_enforced_check_allows_violating_data() {
        let mut executor = create_executor();

        executor
            .execute_sql(
                "CREATE TABLE prices (
                    id INT PRIMARY KEY,
                    amount DECIMAL(10,2),
                    CHECK (amount >= 0) NOT ENFORCED
                )",
            )
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("INSERT INTO prices VALUES (1, -100.00)");
        assert!(
            result.is_ok(),
            "INSERT violating NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );

        let result = executor
            .execute_sql("SELECT amount FROM prices WHERE id = 1")
            .expect("SELECT should succeed");
        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_enforced_check_still_works() {
        let mut executor = create_executor();

        executor
            .execute_sql(
                "CREATE TABLE strict_prices (
                    id INT PRIMARY KEY,
                    amount DECIMAL(10,2) CHECK (amount >= 0)
                )",
            )
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("INSERT INTO strict_prices VALUES (1, -100.00)");
        assert!(
            result.is_err(),
            "INSERT violating enforced CHECK should fail"
        );
    }

    #[test]
    fn test_named_not_enforced_check() {
        let mut executor = create_executor();

        let result = executor.execute_sql(
            "CREATE TABLE inventory (
                id INT PRIMARY KEY,
                quantity INT,
                CONSTRAINT positive_qty CHECK (quantity > 0) NOT ENFORCED
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TABLE with named NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );

        let result = executor.execute_sql("INSERT INTO inventory VALUES (1, -5)");
        assert!(
            result.is_ok(),
            "INSERT violating named NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_multiple_checks_mixed_enforcement() {
        let mut executor = create_executor();

        executor
            .execute_sql(
                "CREATE TABLE orders (
                    id INT PRIMARY KEY,
                    quantity INT CHECK (quantity > 0),
                    discount DECIMAL(5,2),
                    CHECK (discount >= 0 AND discount <= 100) NOT ENFORCED
                )",
            )
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("INSERT INTO orders VALUES (1, -1, 10.00)");
        assert!(result.is_err(), "Violating enforced CHECK should fail");

        let result = executor.execute_sql("INSERT INTO orders VALUES (1, 5, 150.00)");
        assert!(
            result.is_ok(),
            "Violating NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );
    }
}

mod foreign_key_not_enforced {
    use super::*;

    #[test]
    fn test_create_table_with_not_enforced_fk() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)")
            .expect("CREATE departments should succeed");

        let result = executor.execute_sql(
            "CREATE TABLE employees (
                id INT PRIMARY KEY,
                name TEXT,
                dept_id INT REFERENCES departments(id) NOT ENFORCED
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TABLE with NOT ENFORCED FK should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_not_enforced_fk_allows_orphan_records() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE categories (id INT PRIMARY KEY, name TEXT)")
            .expect("CREATE categories should succeed");

        executor
            .execute_sql(
                "CREATE TABLE items (
                    id INT PRIMARY KEY,
                    name TEXT,
                    category_id INT REFERENCES categories(id) NOT ENFORCED
                )",
            )
            .expect("CREATE items should succeed");

        let result = executor.execute_sql("INSERT INTO items VALUES (1, 'Widget', 999)");
        assert!(
            result.is_ok(),
            "INSERT with orphan FK (NOT ENFORCED) should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_enforced_fk_still_works() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE parent_table (id INT PRIMARY KEY)")
            .expect("CREATE parent should succeed");

        executor
            .execute_sql(
                "CREATE TABLE child_table (
                    id INT PRIMARY KEY,
                    parent_id INT REFERENCES parent_table(id)
                )",
            )
            .expect("CREATE child should succeed");

        let result = executor.execute_sql("INSERT INTO child_table VALUES (1, 999)");
        assert!(
            result.is_err(),
            "INSERT with orphan FK (enforced) should fail"
        );
    }

    #[test]
    fn test_named_not_enforced_fk() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE regions (id INT PRIMARY KEY, name TEXT)")
            .expect("CREATE regions should succeed");

        let result = executor.execute_sql(
            "CREATE TABLE stores (
                id INT PRIMARY KEY,
                name TEXT,
                region_id INT,
                CONSTRAINT fk_region FOREIGN KEY (region_id) REFERENCES regions(id) NOT ENFORCED
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TABLE with named NOT ENFORCED FK should succeed: {:?}",
            result.err()
        );

        let result = executor.execute_sql("INSERT INTO stores VALUES (1, 'Store A', 999)");
        assert!(
            result.is_ok(),
            "INSERT with orphan named NOT ENFORCED FK should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_not_enforced_fk_delete_parent() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE authors (id INT PRIMARY KEY, name TEXT)")
            .expect("CREATE authors should succeed");

        executor
            .execute_sql(
                "CREATE TABLE books (
                    id INT PRIMARY KEY,
                    title TEXT,
                    author_id INT REFERENCES authors(id) NOT ENFORCED
                )",
            )
            .expect("CREATE books should succeed");

        executor
            .execute_sql("INSERT INTO authors VALUES (1, 'Alice')")
            .expect("INSERT author should succeed");

        executor
            .execute_sql("INSERT INTO books VALUES (1, 'Book One', 1)")
            .expect("INSERT book should succeed");

        let result = executor.execute_sql("DELETE FROM authors WHERE id = 1");
        assert!(
            result.is_ok(),
            "DELETE parent with NOT ENFORCED FK should succeed: {:?}",
            result.err()
        );
    }
}

mod alter_table_not_enforced {
    use super::*;

    #[test]
    fn test_alter_table_add_not_enforced_check() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE test_alter (id INT PRIMARY KEY, value INT)")
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql(
            "ALTER TABLE test_alter ADD CONSTRAINT chk_positive CHECK (value > 0) NOT ENFORCED",
        );
        assert!(
            result.is_ok(),
            "ALTER TABLE ADD NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );

        let result = executor.execute_sql("INSERT INTO test_alter VALUES (1, -10)");
        assert!(
            result.is_ok(),
            "INSERT violating NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_alter_table_add_not_enforced_fk() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE ref_table (id INT PRIMARY KEY)")
            .expect("CREATE ref_table should succeed");

        executor
            .execute_sql("CREATE TABLE main_table (id INT PRIMARY KEY, ref_id INT)")
            .expect("CREATE main_table should succeed");

        let result = executor.execute_sql(
            "ALTER TABLE main_table ADD CONSTRAINT fk_ref
             FOREIGN KEY (ref_id) REFERENCES ref_table(id) NOT ENFORCED",
        );
        assert!(
            result.is_ok(),
            "ALTER TABLE ADD NOT ENFORCED FK should succeed: {:?}",
            result.err()
        );

        let result = executor.execute_sql("INSERT INTO main_table VALUES (1, 999)");
        assert!(
            result.is_ok(),
            "INSERT with orphan NOT ENFORCED FK should succeed: {:?}",
            result.err()
        );
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn test_not_enforced_with_null_values() {
        let mut executor = create_executor();

        executor
            .execute_sql(
                "CREATE TABLE nullable_check (
                    id INT PRIMARY KEY,
                    value INT,
                    CHECK (value > 0) NOT ENFORCED
                )",
            )
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("INSERT INTO nullable_check VALUES (1, NULL)");
        assert!(
            result.is_ok(),
            "INSERT NULL with NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_not_enforced_update() {
        let mut executor = create_executor();

        executor
            .execute_sql(
                "CREATE TABLE updateable (
                    id INT PRIMARY KEY,
                    amount INT,
                    CHECK (amount >= 0) NOT ENFORCED
                )",
            )
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql("INSERT INTO updateable VALUES (1, 100)")
            .expect("INSERT should succeed");

        let result = executor.execute_sql("UPDATE updateable SET amount = -50 WHERE id = 1");
        assert!(
            result.is_ok(),
            "UPDATE violating NOT ENFORCED CHECK should succeed: {:?}",
            result.err()
        );

        let result = executor
            .execute_sql("SELECT amount FROM updateable WHERE id = 1")
            .expect("SELECT should succeed");
        assert_eq!(get_i64(&result, 0, 0), -50);
    }

    #[test]
    fn test_not_enforced_complex_check() {
        let mut executor = create_executor();

        executor
            .execute_sql(
                "CREATE TABLE date_range (
                    id INT PRIMARY KEY,
                    start_date DATE,
                    end_date DATE,
                    CHECK (end_date > start_date) NOT ENFORCED
                )",
            )
            .expect("CREATE TABLE should succeed");

        let result =
            executor.execute_sql("INSERT INTO date_range VALUES (1, '2025-12-31', '2025-01-01')");
        assert!(
            result.is_ok(),
            "INSERT violating date range CHECK should succeed: {:?}",
            result.err()
        );
    }
}
