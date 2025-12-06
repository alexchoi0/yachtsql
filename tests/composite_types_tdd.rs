mod common;

use common::setup_executor;

fn create_executor() -> yachtsql::QueryExecutor {
    setup_executor()
}

mod create_type {
    use super::*;

    #[test]
    fn test_create_simple_composite_type() {
        let mut executor = create_executor();

        let result = executor.execute_sql(
            "CREATE TYPE address AS (
                street TEXT,
                city TEXT
            )",
        );
        assert!(result.is_ok(), "CREATE TYPE should succeed: {:?}", result);
    }

    #[test]
    fn test_create_composite_type_multiple_field_types() {
        let mut executor = create_executor();

        let result = executor.execute_sql(
            "CREATE TYPE person AS (
                id INT,
                name TEXT,
                age INT,
                salary NUMERIC(10, 2),
                active BOOLEAN
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TYPE with multiple field types should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_create_composite_type_with_array_field() {
        let mut executor = create_executor();

        let result = executor.execute_sql(
            "CREATE TYPE contact AS (
                name TEXT,
                phone_numbers TEXT[]
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TYPE with array field should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_create_duplicate_type_error() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("First CREATE TYPE should succeed");

        let result = executor.execute_sql("CREATE TYPE point AS (a INT, b INT)");
        assert!(result.is_err(), "Creating duplicate type should fail");
        let err = result.unwrap_err().to_string().to_lowercase();
        assert!(
            err.contains("exists") || err.contains("already") || err.contains("duplicate"),
            "Error should mention type already exists: {}",
            err
        );
    }

    #[test]
    fn test_create_type_if_not_exists() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("First CREATE TYPE should succeed");

        let result = executor.execute_sql("CREATE TYPE IF NOT EXISTS point AS (a INT, b INT)");
        assert!(
            result.is_ok(),
            "CREATE TYPE IF NOT EXISTS should succeed silently: {:?}",
            result
        );
    }

    #[test]
    fn test_drop_type() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE temp_type AS (val INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor.execute_sql("DROP TYPE temp_type");
        assert!(result.is_ok(), "DROP TYPE should succeed: {:?}", result);

        let result = executor.execute_sql("CREATE TYPE temp_type AS (other_val TEXT)");
        assert!(
            result.is_ok(),
            "CREATE TYPE after DROP should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_drop_type_if_exists() {
        let mut executor = create_executor();

        let result = executor.execute_sql("DROP TYPE IF EXISTS nonexistent_type");
        assert!(
            result.is_ok(),
            "DROP TYPE IF EXISTS should succeed silently: {:?}",
            result
        );
    }

    #[test]
    fn test_drop_nonexistent_type_error() {
        let mut executor = create_executor();

        let result = executor.execute_sql("DROP TYPE nonexistent_type");
        assert!(
            result.is_err(),
            "DROP TYPE for nonexistent type should fail"
        );
    }
}

mod row_constructor {
    use super::*;

    #[test]
    fn test_row_constructor_basic() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ROW(1, 'hello', true) AS r")
            .expect("ROW constructor should succeed");

        assert_eq!(result.num_rows(), 1);
        let col = result.column(0).expect("Should have column r");
        let val = col.get(0).expect("Should have value");

        assert!(
            val.as_struct().is_some(),
            "ROW() should return a struct value, got: {:?}",
            val
        );
    }

    #[test]
    fn test_row_constructor_with_named_type() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT ROW(10, 20)::point AS p")
            .expect("ROW cast to composite type should succeed");

        assert_eq!(result.num_rows(), 1);
        let col = result.column(0).expect("Should have column p");
        let val = col.get(0).expect("Should have value");
        let struct_val = val.as_struct().expect("Should be a struct");

        assert_eq!(
            struct_val.get("x").and_then(|v| v.as_i64()),
            Some(10),
            "x field should be 10"
        );
        assert_eq!(
            struct_val.get("y").and_then(|v| v.as_i64()),
            Some(20),
            "y field should be 20"
        );
    }

    #[test]
    fn test_row_constructor_type_mismatch_error() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor.execute_sql("SELECT ROW(1, 2, 3)::point AS p");
        assert!(result.is_err(), "ROW with wrong field count should fail");
    }

    #[test]
    fn test_row_constructor_field_type_coercion() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE measurements AS (width NUMERIC, height NUMERIC)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT ROW(10, 20)::measurements AS m")
            .expect("ROW with coercible types should succeed");

        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_row_constructor_null_fields() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE optional_data AS (name TEXT, value INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT ROW('test', NULL)::optional_data AS d")
            .expect("ROW with NULL field should succeed");

        assert_eq!(result.num_rows(), 1);
        let col = result.column(0).expect("Should have column");
        let val = col.get(0).expect("Should have value");
        let struct_val = val.as_struct().expect("Should be a struct");

        assert!(
            struct_val
                .get("value")
                .map(|v| v.is_null())
                .unwrap_or(false),
            "value field should be NULL"
        );
    }

    #[test]
    fn test_row_shorthand_without_keyword() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT (5, 10)::point AS p")
            .expect("Parenthesized tuple cast should succeed");

        assert_eq!(result.num_rows(), 1);
    }
}

mod field_access {
    use super::*;

    #[test]
    fn test_field_access_basic() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT (ROW(3, 4)::point).x AS x_val")
            .expect("Field access should succeed");

        assert_eq!(result.num_rows(), 1);
        let col = result.column(0).expect("Should have column");
        let val = col.get(0).expect("Should have value");
        assert_eq!(val.as_i64(), Some(3), "x field should be 3");
    }

    #[test]
    fn test_field_access_multiple_fields() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE person AS (name TEXT, age INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql(
                "SELECT (ROW('Alice', 30)::person).name AS n,
                        (ROW('Alice', 30)::person).age AS a",
            )
            .expect("Multiple field access should succeed");

        assert_eq!(result.num_rows(), 1);

        let name_col = result.column(0).expect("Should have name column");
        let name_val = name_col.get(0).expect("Should have name value");
        assert_eq!(name_val.as_str(), Some("Alice"));

        let age_col = result.column(1).expect("Should have age column");
        let age_val = age_col.get(0).expect("Should have age value");
        assert_eq!(age_val.as_i64(), Some(30));
    }

    #[test]
    fn test_field_access_from_column() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE address AS (street TEXT, city TEXT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql(
                "CREATE TABLE locations (
                    id INT,
                    addr address
                )",
            )
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql(
                "INSERT INTO locations VALUES
                    (1, ROW('123 Main St', 'Boston')::address)",
            )
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql("SELECT id, addr.street, addr.city FROM locations")
            .expect("Field access from column should succeed");

        assert_eq!(result.num_rows(), 1);

        let street_col = result.column(1).expect("Should have street column");
        let street_val = street_col.get(0).expect("Should have street value");
        assert_eq!(street_val.as_str(), Some("123 Main St"));

        let city_col = result.column(2).expect("Should have city column");
        let city_val = city_col.get(0).expect("Should have city value");
        assert_eq!(city_val.as_str(), Some("Boston"));
    }

    #[test]
    fn test_field_access_invalid_field_error() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor.execute_sql("SELECT (ROW(1, 2)::point).z AS z_val");
        assert!(result.is_err(), "Accessing non-existent field should fail");
        let err = result.unwrap_err().to_string().to_lowercase();
        assert!(
            err.contains("field") || err.contains("not found") || err.contains("does not have"),
            "Error should mention missing field: {}",
            err
        );
    }

    #[test]
    fn test_field_access_null_composite() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT (NULL::point).x AS x_val")
            .expect("Field access on NULL should succeed");

        assert_eq!(result.num_rows(), 1);
        let col = result.column(0).expect("Should have column");
        let val = col.get(0).expect("Should have value");
        assert!(val.is_null(), "Field of NULL composite should be NULL");
    }
}

mod table_columns {
    use super::*;

    #[test]
    fn test_table_with_composite_column() {
        let mut executor = create_executor();

        executor
            .execute_sql(
                "CREATE TYPE inventory_item AS (name TEXT, supplier_id INT, price NUMERIC)",
            )
            .expect("CREATE TYPE should succeed");

        let result = executor.execute_sql(
            "CREATE TABLE on_hand (
                item inventory_item,
                count INT
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TABLE with composite column should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_insert_composite_value() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("INSERT INTO shapes VALUES (1, ROW(10, 20)::point)");
        assert!(
            result.is_ok(),
            "INSERT with composite value should succeed: {:?}",
            result
        );

        let select = executor
            .execute_sql("SELECT id, center FROM shapes")
            .expect("SELECT should succeed");
        assert_eq!(select.num_rows(), 1);
    }

    #[test]
    fn test_update_composite_column() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql("INSERT INTO shapes VALUES (1, ROW(10, 20)::point)")
            .expect("INSERT should succeed");

        let result =
            executor.execute_sql("UPDATE shapes SET center = ROW(30, 40)::point WHERE id = 1");
        assert!(
            result.is_ok(),
            "UPDATE composite column should succeed: {:?}",
            result
        );

        let select = executor
            .execute_sql("SELECT center.x, center.y FROM shapes WHERE id = 1")
            .expect("SELECT should succeed");
        assert_eq!(select.num_rows(), 1);

        let x_val = select.column(0).unwrap().get(0).unwrap();
        let y_val = select.column(1).unwrap().get(0).unwrap();
        assert_eq!(x_val.as_i64(), Some(30));
        assert_eq!(y_val.as_i64(), Some(40));
    }

    #[test]
    fn test_update_composite_field() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql("INSERT INTO shapes VALUES (1, ROW(10, 20)::point)")
            .expect("INSERT should succeed");

        let result = executor.execute_sql("UPDATE shapes SET center.x = 100 WHERE id = 1");
        assert!(
            result.is_ok(),
            "UPDATE composite field should succeed: {:?}",
            result
        );

        let select = executor
            .execute_sql("SELECT center.x, center.y FROM shapes WHERE id = 1")
            .expect("SELECT should succeed");

        let x_val = select.column(0).unwrap().get(0).unwrap();
        let y_val = select.column(1).unwrap().get(0).unwrap();
        assert_eq!(x_val.as_i64(), Some(100), "x should be updated to 100");
        assert_eq!(y_val.as_i64(), Some(20), "y should remain 20");
    }
}

mod comparison {
    use super::*;

    #[test]
    fn test_composite_equality() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql(
                "SELECT ROW(1, 2)::point = ROW(1, 2)::point AS eq,
                        ROW(1, 2)::point = ROW(1, 3)::point AS neq",
            )
            .expect("Composite equality should succeed");

        assert_eq!(result.num_rows(), 1);

        let eq_val = result.column(0).unwrap().get(0).unwrap();
        let neq_val = result.column(1).unwrap().get(0).unwrap();

        assert_eq!(
            eq_val.as_bool(),
            Some(true),
            "Equal composites should be true"
        );
        assert_eq!(
            neq_val.as_bool(),
            Some(false),
            "Different composites should be false"
        );
    }

    #[test]
    fn test_composite_inequality() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql(
                "SELECT ROW(1, 2)::point <> ROW(1, 3)::point AS neq,
                        ROW(1, 2)::point <> ROW(1, 2)::point AS eq",
            )
            .expect("Composite inequality should succeed");

        assert_eq!(result.num_rows(), 1);

        let neq_val = result.column(0).unwrap().get(0).unwrap();
        let eq_val = result.column(1).unwrap().get(0).unwrap();

        assert_eq!(
            neq_val.as_bool(),
            Some(true),
            "Different composites should be not equal"
        );
        assert_eq!(
            eq_val.as_bool(),
            Some(false),
            "Same composites should not be not equal"
        );
    }

    #[test]
    fn test_composite_less_than() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql(
                "SELECT ROW(1, 2)::point < ROW(1, 3)::point AS lt1,
                        ROW(1, 2)::point < ROW(2, 1)::point AS lt2,
                        ROW(2, 1)::point < ROW(1, 3)::point AS lt3",
            )
            .expect("Composite less than should succeed");

        assert_eq!(result.num_rows(), 1);

        let lt1_val = result.column(0).unwrap().get(0).unwrap();
        let lt2_val = result.column(1).unwrap().get(0).unwrap();
        let lt3_val = result.column(2).unwrap().get(0).unwrap();

        assert_eq!(
            lt1_val.as_bool(),
            Some(true),
            "(1,2) < (1,3) should be true"
        );
        assert_eq!(
            lt2_val.as_bool(),
            Some(true),
            "(1,2) < (2,1) should be true (first field comparison)"
        );
        assert_eq!(
            lt3_val.as_bool(),
            Some(false),
            "(2,1) < (1,3) should be false"
        );
    }

    #[test]
    fn test_composite_comparison_with_nulls() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT ROW(1, NULL)::point = ROW(1, 2)::point AS cmp")
            .expect("Composite comparison with NULL should succeed");

        assert_eq!(result.num_rows(), 1);
        let cmp_val = result.column(0).unwrap().get(0).unwrap();
        assert!(
            cmp_val.is_null(),
            "Comparison involving NULL should be NULL"
        );
    }

    #[test]
    fn test_composite_is_null() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql(
                "SELECT NULL::point IS NULL AS is_null,
                        ROW(1, 2)::point IS NULL AS not_null",
            )
            .expect("IS NULL on composite should succeed");

        assert_eq!(result.num_rows(), 1);

        let is_null_val = result.column(0).unwrap().get(0).unwrap();
        let not_null_val = result.column(1).unwrap().get(0).unwrap();

        assert_eq!(
            is_null_val.as_bool(),
            Some(true),
            "NULL composite IS NULL should be true"
        );
        assert_eq!(
            not_null_val.as_bool(),
            Some(false),
            "Non-NULL composite IS NULL should be false"
        );
    }

    #[test]
    fn test_composite_is_distinct_from() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql(
                "SELECT ROW(1, NULL)::point IS DISTINCT FROM ROW(1, NULL)::point AS same_null,
                        ROW(1, NULL)::point IS DISTINCT FROM ROW(1, 2)::point AS diff_null",
            )
            .expect("IS DISTINCT FROM on composite should succeed");

        assert_eq!(result.num_rows(), 1);

        let same_null_val = result.column(0).unwrap().get(0).unwrap();
        let diff_null_val = result.column(1).unwrap().get(0).unwrap();

        assert_eq!(
            same_null_val.as_bool(),
            Some(false),
            "Same NULL composites are not distinct"
        );
        assert_eq!(
            diff_null_val.as_bool(),
            Some(true),
            "Different composites (one with NULL) are distinct"
        );
    }
}

mod nested_composites {
    use super::*;

    #[test]
    fn test_nested_composite_type_definition() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE point should succeed");

        let result = executor.execute_sql(
            "CREATE TYPE rectangle AS (
                top_left point,
                bottom_right point
            )",
        );
        assert!(
            result.is_ok(),
            "CREATE TYPE with nested composite should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_nested_composite_construction() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE point should succeed");

        executor
            .execute_sql("CREATE TYPE rectangle AS (top_left point, bottom_right point)")
            .expect("CREATE TYPE rectangle should succeed");

        let result = executor
            .execute_sql("SELECT ROW(ROW(0, 0)::point, ROW(10, 10)::point)::rectangle AS rect");
        assert!(
            result.is_ok(),
            "Nested composite construction should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_nested_field_access() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE point should succeed");

        executor
            .execute_sql("CREATE TYPE rectangle AS (top_left point, bottom_right point)")
            .expect("CREATE TYPE rectangle should succeed");

        let result = executor
            .execute_sql(
                "SELECT (ROW(ROW(0, 5)::point, ROW(10, 15)::point)::rectangle).top_left.x AS tlx,
                        (ROW(ROW(0, 5)::point, ROW(10, 15)::point)::rectangle).bottom_right.y AS bry",
            )
            .expect("Nested field access should succeed");

        assert_eq!(result.num_rows(), 1);

        let tlx_val = result.column(0).unwrap().get(0).unwrap();
        let bry_val = result.column(1).unwrap().get(0).unwrap();

        assert_eq!(tlx_val.as_i64(), Some(0), "top_left.x should be 0");
        assert_eq!(bry_val.as_i64(), Some(15), "bottom_right.y should be 15");
    }

    #[test]
    fn test_deeply_nested_composite() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE inner_type AS (val INT)")
            .expect("CREATE TYPE inner should succeed");

        executor
            .execute_sql("CREATE TYPE middle_type AS (inner_field inner_type)")
            .expect("CREATE TYPE middle should succeed");

        executor
            .execute_sql("CREATE TYPE outer_type AS (middle_field middle_type)")
            .expect("CREATE TYPE outer should succeed");

        let result = executor
            .execute_sql(
                "SELECT (ROW(ROW(ROW(42)::inner_type)::middle_type)::outer_type).middle_field.inner_field.val AS v",
            )
            .expect("Deeply nested field access should succeed");

        assert_eq!(result.num_rows(), 1);
        let val = result.column(0).unwrap().get(0).unwrap();
        assert_eq!(val.as_i64(), Some(42));
    }
}

mod type_catalog {
    use super::*;

    #[test]
    fn test_type_persists_across_queries() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result1 = executor.execute_sql("SELECT ROW(1, 2)::point AS p");
        assert!(result1.is_ok(), "First use of type should succeed");

        let result2 = executor.execute_sql("SELECT ROW(3, 4)::point AS p");
        assert!(result2.is_ok(), "Second use of type should succeed");
    }

    #[test]
    fn test_type_used_in_multiple_tables() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE shapes should succeed");

        executor
            .execute_sql("CREATE TABLE markers (name TEXT, location point)")
            .expect("CREATE TABLE markers should succeed");

        executor
            .execute_sql("INSERT INTO shapes VALUES (1, ROW(10, 20)::point)")
            .expect("INSERT into shapes should succeed");

        executor
            .execute_sql("INSERT INTO markers VALUES ('home', ROW(100, 200)::point)")
            .expect("INSERT into markers should succeed");
    }

    #[test]
    fn test_cannot_drop_type_in_use() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("DROP TYPE point");
        assert!(result.is_err(), "DROP TYPE in use should fail");
        let err = result.unwrap_err().to_string().to_lowercase();
        assert!(
            err.contains("in use") || err.contains("depend") || err.contains("cannot"),
            "Error should mention type is in use: {}",
            err
        );
    }

    #[test]
    fn test_drop_type_cascade() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("DROP TYPE point CASCADE");
        assert!(
            result.is_ok(),
            "DROP TYPE CASCADE should succeed: {:?}",
            result
        );
    }
}

mod expressions {
    use super::*;

    #[test]
    fn test_composite_in_case_expression() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql(
                "SELECT CASE
                    WHEN ROW(1, 2)::point = ROW(1, 2)::point THEN 'equal'
                    ELSE 'not equal'
                 END AS result",
            )
            .expect("CASE with composite comparison should succeed");

        assert_eq!(result.num_rows(), 1);
        let val = result.column(0).unwrap().get(0).unwrap();
        assert_eq!(val.as_str(), Some("equal"));
    }

    #[test]
    fn test_composite_in_where_clause() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql(
                "INSERT INTO shapes VALUES (1, ROW(10, 20)::point), (2, ROW(30, 40)::point)",
            )
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql("SELECT id FROM shapes WHERE center = ROW(10, 20)::point")
            .expect("WHERE with composite comparison should succeed");

        assert_eq!(result.num_rows(), 1);
        let id_val = result.column(0).unwrap().get(0).unwrap();
        assert_eq!(id_val.as_i64(), Some(1));
    }

    #[test]
    fn test_composite_in_order_by() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql(
                "INSERT INTO shapes VALUES
                    (1, ROW(2, 3)::point),
                    (2, ROW(1, 5)::point),
                    (3, ROW(2, 1)::point)",
            )
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql("SELECT id FROM shapes ORDER BY center")
            .expect("ORDER BY composite should succeed");

        assert_eq!(result.num_rows(), 3);

        let ids: Vec<i64> = (0..3)
            .map(|i| result.column(0).unwrap().get(i).unwrap().as_i64().unwrap())
            .collect();

        assert_eq!(
            ids,
            vec![2, 3, 1],
            "Order should be lexicographic by composite"
        );
    }

    #[test]
    fn test_composite_in_group_by() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE events (name TEXT, location point)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql(
                "INSERT INTO events VALUES
                    ('A', ROW(1, 1)::point),
                    ('B', ROW(1, 1)::point),
                    ('C', ROW(2, 2)::point)",
            )
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql(
                "SELECT location, COUNT(*) AS cnt FROM events GROUP BY location ORDER BY cnt",
            )
            .expect("GROUP BY composite should succeed");

        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn test_composite_in_distinct() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE markers (location point)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql(
                "INSERT INTO markers VALUES
                    (ROW(1, 1)::point),
                    (ROW(1, 1)::point),
                    (ROW(2, 2)::point)",
            )
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql("SELECT DISTINCT location FROM markers")
            .expect("DISTINCT on composite should succeed");

        assert_eq!(result.num_rows(), 2, "Should have 2 distinct locations");
    }
}

mod edge_cases {
    use super::*;

    #[test]
    fn test_empty_composite_type_error() {
        let mut executor = create_executor();

        let result = executor.execute_sql("CREATE TYPE empty AS ()");
        assert!(result.is_err(), "Empty composite type should fail");
    }

    #[test]
    fn test_reserved_type_name_error() {
        let mut executor = create_executor();

        let _result = executor.execute_sql("CREATE TYPE integer AS (val INT)");
    }

    #[test]
    fn test_self_referential_type_error() {
        let mut executor = create_executor();

        let result = executor.execute_sql("CREATE TYPE node AS (val INT, next node)");
        assert!(result.is_err(), "Self-referential type should fail");
    }

    #[test]
    fn test_very_long_composite_type() {
        let mut executor = create_executor();

        let fields: Vec<String> = (0..50).map(|i| format!("field_{} INT", i)).collect();
        let sql = format!("CREATE TYPE wide_type AS ({})", fields.join(", "));

        let result = executor.execute_sql(&sql);
        assert!(
            result.is_ok(),
            "Composite type with many fields should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_composite_with_same_field_name_error() {
        let mut executor = create_executor();

        let result = executor.execute_sql("CREATE TYPE bad_type AS (x INT, x TEXT)");
        assert!(result.is_err(), "Duplicate field names should fail");
        let err = result.unwrap_err().to_string().to_lowercase();
        assert!(
            err.contains("duplicate") || err.contains("already") || err.contains("field"),
            "Error should mention duplicate field: {}",
            err
        );
    }

    #[test]
    fn test_case_sensitivity_of_field_names() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE mixed_case AS (Name TEXT, AGE INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT (ROW('Alice', 30)::mixed_case).name AS n")
            .expect("Field access should succeed (case insensitive)");

        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_quoted_field_names() {
        let mut executor = create_executor();

        executor
            .execute_sql(r#"CREATE TYPE special AS ("Field-With-Dash" INT, "123start" TEXT)"#)
            .expect("CREATE TYPE with quoted field names should succeed");

        let result = executor
            .execute_sql(r#"SELECT (ROW(42, 'test')::special)."Field-With-Dash" AS val"#)
            .expect("Quoted field access should succeed");

        assert_eq!(result.num_rows(), 1);
        let val = result.column(0).unwrap().get(0).unwrap();
        assert_eq!(val.as_i64(), Some(42));
    }
}

mod functions {
    use super::*;

    #[test]
    fn test_composite_as_function_argument() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result = executor
            .execute_sql("SELECT COALESCE(NULL::point, ROW(1, 2)::point) AS p")
            .expect("COALESCE with composite should succeed");

        assert_eq!(result.num_rows(), 1);
    }

    #[test]
    fn test_array_of_composites() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        let result =
            executor.execute_sql("SELECT ARRAY[ROW(1, 2)::point, ROW(3, 4)::point] AS points");
        assert!(
            result.is_ok(),
            "Array of composites should succeed: {:?}",
            result
        );
    }

    #[test]
    fn test_composite_in_subquery() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TYPE point AS (x INT, y INT)")
            .expect("CREATE TYPE should succeed");

        executor
            .execute_sql("CREATE TABLE shapes (id INT, center point)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql("INSERT INTO shapes VALUES (1, ROW(10, 20)::point)")
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql("SELECT * FROM shapes WHERE center IN (SELECT ROW(10, 20)::point)")
            .expect("Composite in subquery should succeed");

        assert_eq!(result.num_rows(), 1);
    }
}
