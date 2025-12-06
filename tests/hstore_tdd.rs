mod common;

use common::{get_string, is_null};
use yachtsql::{DialectType, QueryExecutor};

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::PostgreSQL)
}

mod hstore_constructors {
    use super::*;

    #[test]
    fn test_hstore_from_arrays() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT hstore(ARRAY['a', 'b'], ARRAY['1', '2']) as h")
            .expect("hstore(keys, values) should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore should not return NULL - hstore function not implemented"
        );
    }

    #[test]
    fn test_hstore_from_arrays_with_null_value() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT hstore(ARRAY['a', 'b'], ARRAY['1', NULL]) as h")
            .expect("hstore with NULL value should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore should not return NULL - hstore function not implemented"
        );
    }

    #[test]
    fn test_hstore_from_text() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT 'a=>1, b=>2'::hstore as h")
            .expect("hstore cast from text should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore should not return NULL - hstore parsing not implemented"
        );
    }

    #[test]
    fn test_hstore_from_text_with_quotes() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(r#"SELECT '"key with space"=>"value with space"'::hstore as h"#)
            .expect("hstore with quoted text should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore should not return NULL - hstore parsing not implemented"
        );
    }

    #[test]
    fn test_hstore_from_text_with_null() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT 'a=>1, b=>NULL'::hstore as h")
            .expect("hstore with NULL text should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore should not return NULL - hstore parsing not implemented"
        );
    }

    #[test]
    fn test_hstore_empty() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ''::hstore as h")
            .expect("empty hstore should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "empty hstore should not return NULL - hstore parsing not implemented"
        );
    }
}

mod hstore_accessor {
    use super::*;

    #[test]
    fn test_hstore_get_existing_key() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore) -> 'a' as val")
            .expect("hstore -> operator should parse");

        assert_eq!(result.num_rows(), 1);

        let val = get_string(&result, 0, 0);
        assert_eq!(val, "1", "hstore -> 'a' should return '1'");
    }

    #[test]
    fn test_hstore_get_nonexistent_key() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore) -> 'c' as val")
            .expect("hstore -> non-existent key should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            is_null(&result, 0, 0),
            "hstore -> non-existent key should return NULL"
        );
    }

    #[test]
    fn test_hstore_get_null_value() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>NULL'::hstore) -> 'b' as val")
            .expect("hstore -> NULL value should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            is_null(&result, 0, 0),
            "hstore -> key with NULL value should return NULL"
        );
    }

    #[test]
    fn test_hstore_get_multiple_keys() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2, c=>3'::hstore) -> ARRAY['a', 'c'] as vals")
            .expect("hstore -> array should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore -> array should return array of values"
        );
    }
}

mod hstore_existence {
    use common::get_bool;

    use super::*;

    #[test]
    fn test_hstore_exists_true() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore) ? 'a' as exists")
            .expect("hstore ? operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(get_bool(&result, 0, 0), "hstore ? 'a' should return true");
    }

    #[test]
    fn test_hstore_exists_false() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore) ? 'c' as exists")
            .expect("hstore ? operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(!get_bool(&result, 0, 0), "hstore ? 'c' should return false");
    }

    #[test]
    fn test_hstore_exists_with_null_value() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>NULL'::hstore) ? 'b' as exists")
            .expect("hstore ? with NULL value should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "hstore ? should return true even for key with NULL value"
        );
    }

    #[test]
    fn test_hstore_exists_all_true() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2, c=>3'::hstore) ?& ARRAY['a', 'b'] as all_exist")
            .expect("hstore ?& operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "hstore ?& should return true when all keys exist"
        );
    }

    #[test]
    fn test_hstore_exists_all_false() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore) ?& ARRAY['a', 'c'] as all_exist")
            .expect("hstore ?& operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !get_bool(&result, 0, 0),
            "hstore ?& should return false when not all keys exist"
        );
    }

    #[test]
    fn test_hstore_exists_any_true() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore) ?| ARRAY['a', 'c'] as any_exist")
            .expect("hstore ?| operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "hstore ?| should return true when any key exists"
        );
    }

    #[test]
    fn test_hstore_exists_any_false() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore) ?| ARRAY['c', 'd'] as any_exist")
            .expect("hstore ?| operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !get_bool(&result, 0, 0),
            "hstore ?| should return false when no keys exist"
        );
    }
}

mod hstore_concat_delete {
    use common::get_bool;

    use super::*;

    #[test]
    fn test_hstore_concat() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1'::hstore || 'b=>2'::hstore) ? 'b' as has_b")
            .expect("hstore || operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "concatenated hstore should contain key 'b'"
        );
    }

    #[test]
    fn test_hstore_concat_overwrite() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1'::hstore || 'a=>2'::hstore) -> 'a' as val")
            .expect("hstore || overwrite should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "2", "concatenation should overwrite existing key");
    }

    #[test]
    fn test_hstore_delete_key() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore - 'a') ? 'a' as has_a")
            .expect("hstore - key should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !get_bool(&result, 0, 0),
            "hstore after deletion should not contain key 'a'"
        );
    }

    #[test]
    fn test_hstore_delete_keys_array() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2, c=>3'::hstore - ARRAY['a', 'b']) ? 'c' as has_c")
            .expect("hstore - array should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "hstore after deletion should still contain key 'c'"
        );
    }

    #[test]
    fn test_hstore_delete_hstore() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore - 'a=>1'::hstore) ? 'a' as has_a")
            .expect("hstore - hstore should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !get_bool(&result, 0, 0),
            "hstore after deletion should not contain key 'a'"
        );
    }
}

mod hstore_containment {
    use common::get_bool;

    use super::*;

    #[test]
    fn test_hstore_contains_true() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore @> 'a=>1'::hstore) as contains")
            .expect("hstore @> operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "hstore @> should return true when left contains right"
        );
    }

    #[test]
    fn test_hstore_contains_false_key_missing() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore @> 'c=>3'::hstore) as contains")
            .expect("hstore @> operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !get_bool(&result, 0, 0),
            "hstore @> should return false when key is missing"
        );
    }

    #[test]
    fn test_hstore_contains_false_value_different() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, b=>2'::hstore @> 'a=>2'::hstore) as contains")
            .expect("hstore @> operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !get_bool(&result, 0, 0),
            "hstore @> should return false when value is different"
        );
    }

    #[test]
    fn test_hstore_contained_by_true() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1'::hstore <@ 'a=>1, b=>2'::hstore) as contained")
            .expect("hstore <@ operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "hstore <@ should return true when left is contained by right"
        );
    }

    #[test]
    fn test_hstore_contained_by_false() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT ('a=>1, c=>3'::hstore <@ 'a=>1, b=>2'::hstore) as contained")
            .expect("hstore <@ operator should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !get_bool(&result, 0, 0),
            "hstore <@ should return false when left has keys not in right"
        );
    }
}

mod hstore_utility_functions {
    use super::*;

    #[test]
    fn test_akeys() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT akeys('a=>1, b=>2'::hstore) as keys")
            .expect("akeys function should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0), "akeys should return array of keys");
    }

    #[test]
    fn test_avals() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT avals('a=>1, b=>2'::hstore) as vals")
            .expect("avals function should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "avals should return array of values"
        );
    }

    #[test]
    fn test_skeys() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT skeys('a=>1, b=>2'::hstore) as key")
            .expect("skeys function should parse");

        assert_eq!(
            result.num_rows(),
            2,
            "skeys should return 2 rows for 2 keys"
        );
    }

    #[test]
    fn test_svals() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT svals('a=>1, b=>2'::hstore) as val")
            .expect("svals function should parse");

        assert_eq!(
            result.num_rows(),
            2,
            "svals should return 2 rows for 2 values"
        );
    }

    #[test]
    fn test_each() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT * FROM each('a=>1, b=>2'::hstore)")
            .expect("each function should parse");

        assert_eq!(result.num_rows(), 2, "each should return 2 rows");
        assert_eq!(
            result.num_columns(),
            2,
            "each should return 2 columns (key, value)"
        );
    }

    #[test]
    fn test_hstore_to_json() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT hstore_to_json('a=>1, b=>2'::hstore) as json")
            .expect("hstore_to_json should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore_to_json should return JSON object"
        );
    }

    #[test]
    fn test_hstore_to_jsonb() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT hstore_to_jsonb('a=>1, b=>2'::hstore) as jsonb")
            .expect("hstore_to_jsonb should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore_to_jsonb should return JSONB object"
        );
    }

    #[test]
    fn test_hstore_to_array() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT hstore_to_array('a=>1, b=>2'::hstore) as arr")
            .expect("hstore_to_array should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore_to_array should return flattened array"
        );
    }

    #[test]
    fn test_hstore_to_matrix() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT hstore_to_matrix('a=>1, b=>2'::hstore) as matrix")
            .expect("hstore_to_matrix should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "hstore_to_matrix should return 2D array"
        );
    }

    #[test]
    fn test_slice() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT slice('a=>1, b=>2, c=>3'::hstore, ARRAY['a', 'c']) as sliced")
            .expect("slice function should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "slice should return hstore with selected keys"
        );
    }

    #[test]
    fn test_defined() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT defined('a=>1, b=>NULL'::hstore, 'a') as is_defined")
            .expect("defined function should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0), "defined should return boolean");
    }

    #[test]
    fn test_delete_key() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT delete('a=>1, b=>2'::hstore, 'a') as deleted")
            .expect("delete function should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(
            !is_null(&result, 0, 0),
            "delete should return hstore without deleted key"
        );
    }

    #[test]
    fn test_exist() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT exist('a=>1, b=>2'::hstore, 'a') as exists")
            .expect("exist function should parse");

        assert_eq!(result.num_rows(), 1);
        assert!(!is_null(&result, 0, 0), "exist should return boolean");
    }

    #[test]
    fn test_populate_record() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE test_record (a TEXT, b TEXT)")
            .expect("CREATE TABLE should succeed");

        let result = executor
            .execute_sql(
                "SELECT * FROM populate_record(NULL::test_record, 'a=>hello, b=>world'::hstore) AS t",
            )
            .expect("populate_record should parse");

        assert_eq!(result.num_rows(), 1);
    }
}

mod hstore_table_storage {
    use common::get_bool;

    use super::*;

    #[test]
    fn test_hstore_column_create() {
        let mut executor = create_executor();

        let result = executor.execute_sql("CREATE TABLE test_hstore (id INT, data HSTORE)");

        assert!(
            result.is_ok(),
            "CREATE TABLE with HSTORE column should succeed"
        );
    }

    #[test]
    fn test_hstore_column_insert() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE test_hstore (id INT, data HSTORE)")
            .expect("CREATE TABLE should succeed");

        let result = executor.execute_sql("INSERT INTO test_hstore VALUES (1, 'a=>1, b=>2')");

        assert!(result.is_ok(), "INSERT with hstore value should succeed");
    }

    #[test]
    fn test_hstore_column_select() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE test_hstore (id INT, data HSTORE)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql("INSERT INTO test_hstore VALUES (1, 'a=>1, b=>2')")
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql("SELECT data -> 'a' as val FROM test_hstore WHERE id = 1")
            .expect("SELECT with hstore accessor should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "1", "Retrieved hstore value should be '1'");
    }

    #[test]
    fn test_hstore_column_update() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE test_hstore (id INT, data HSTORE)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql("INSERT INTO test_hstore VALUES (1, 'a=>1, b=>2')")
            .expect("INSERT should succeed");

        executor
            .execute_sql("UPDATE test_hstore SET data = data || 'c=>3' WHERE id = 1")
            .expect("UPDATE with hstore concatenation should succeed");

        let result = executor
            .execute_sql("SELECT data ? 'c' as has_c FROM test_hstore WHERE id = 1")
            .expect("SELECT should work");

        assert_eq!(result.num_rows(), 1);
        assert!(
            get_bool(&result, 0, 0),
            "Updated hstore should have key 'c'"
        );
    }

    #[test]
    fn test_hstore_column_where_contains() {
        let mut executor = create_executor();

        executor
            .execute_sql("CREATE TABLE test_hstore (id INT, data HSTORE)")
            .expect("CREATE TABLE should succeed");

        executor
            .execute_sql("INSERT INTO test_hstore VALUES (1, 'a=>1, b=>2'), (2, 'c=>3, d=>4')")
            .expect("INSERT should succeed");

        let result = executor
            .execute_sql("SELECT id FROM test_hstore WHERE data @> 'a=>1'")
            .expect("SELECT with containment should work");

        assert_eq!(result.num_rows(), 1);
    }
}

mod hstore_edge_cases {
    use super::*;

    #[test]
    fn test_hstore_special_characters_in_key() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(
                r#"SELECT '"key=>with=>arrows"=>"value"'::hstore -> 'key=>with=>arrows' as val"#,
            )
            .expect("hstore with special chars in key should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "value");
    }

    #[test]
    fn test_hstore_special_characters_in_value() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(r#"SELECT '"key"=>"value=>with=>arrows"'::hstore -> 'key' as val"#)
            .expect("hstore with special chars in value should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "value=>with=>arrows");
    }

    #[test]
    fn test_hstore_unicode_characters() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT '日本語=>こんにちは'::hstore -> '日本語' as val")
            .expect("hstore with unicode should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "こんにちは");
    }

    #[test]
    fn test_hstore_duplicate_keys() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT 'a=>1, a=>2'::hstore -> 'a' as val")
            .expect("hstore with duplicate keys should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "2", "Last value should win for duplicate keys");
    }

    #[test]
    fn test_hstore_whitespace_handling() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql("SELECT 'a  =>  1,  b  =>  2'::hstore -> 'a' as val")
            .expect("hstore with whitespace should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "1");
    }

    #[test]
    fn test_hstore_empty_string_key() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(r#"SELECT '""=>"empty_key_value"'::hstore -> '' as val"#)
            .expect("hstore with empty key should work");

        assert_eq!(result.num_rows(), 1);
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "empty_key_value");
    }

    #[test]
    fn test_hstore_empty_string_value() {
        let mut executor = create_executor();

        let result = executor
            .execute_sql(r#"SELECT '"key"=>""'::hstore -> 'key' as val"#)
            .expect("hstore with empty value should work");

        assert_eq!(result.num_rows(), 1);

        assert!(
            !is_null(&result, 0, 0),
            "Empty string value should not be NULL"
        );
        let val = get_string(&result, 0, 0);
        assert_eq!(val, "", "Empty string value should be ''");
    }

    #[test]
    fn test_hstore_mismatched_array_lengths() {
        let mut executor = create_executor();

        let result =
            executor.execute_sql("SELECT hstore(ARRAY['a', 'b', 'c'], ARRAY['1', '2']) as h");

        assert!(
            result.is_err(),
            "hstore with mismatched array lengths should error"
        );
    }
}
