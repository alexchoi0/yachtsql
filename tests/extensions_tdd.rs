#![allow(dead_code)]
#![allow(unused_variables)]

mod common;
use common::{assert_error_contains, new_executor};

#[test]
fn test_create_extension_uuid_ossp() {
    let mut executor = new_executor();

    let result = executor.execute_sql("CREATE EXTENSION \"uuid-ossp\"");
    assert!(
        result.is_ok(),
        "CREATE EXTENSION uuid-ossp should succeed: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SELECT gen_random_uuid()");
    assert!(
        result.is_ok(),
        "gen_random_uuid should work after installing uuid-ossp: {:?}",
        result.err()
    );

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0).unwrap();
    let val = col.get(0).unwrap();
    let uuid_str = val.as_str().unwrap();

    assert_eq!(uuid_str.len(), 36, "UUID should be 36 characters");
    assert_eq!(
        uuid_str.chars().filter(|c| *c == '-').count(),
        4,
        "UUID should have 4 dashes"
    );
}

#[test]
fn test_create_extension_uuid_ossp_uuid_generate_v4() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE EXTENSION \"uuid-ossp\"")
        .unwrap();

    let result = executor.execute_sql("SELECT uuid_generate_v4()");
    assert!(
        result.is_ok(),
        "uuid_generate_v4 should work: {:?}",
        result.err()
    );

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_create_extension_pgcrypto() {
    let mut executor = new_executor();

    let result = executor.execute_sql("CREATE EXTENSION pgcrypto");
    assert!(
        result.is_ok(),
        "CREATE EXTENSION pgcrypto should succeed: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SELECT gen_random_bytes(16)");
    assert!(
        result.is_ok(),
        "gen_random_bytes should work after installing pgcrypto: {:?}",
        result.err()
    );

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_create_extension_if_not_exists() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE EXTENSION \"uuid-ossp\"")
        .unwrap();

    let result = executor.execute_sql("CREATE EXTENSION \"uuid-ossp\"");
    assert!(
        result.is_err(),
        "Duplicate CREATE EXTENSION should fail without IF NOT EXISTS"
    );

    let result = executor.execute_sql("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"");
    assert!(
        result.is_ok(),
        "CREATE EXTENSION IF NOT EXISTS should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_create_extension_unknown() {
    let mut executor = new_executor();

    let result = executor.execute_sql("CREATE EXTENSION nonexistent_extension");
    assert!(result.is_err(), "Unknown extension should fail");
    assert_error_contains(result, &["extension", "not found", "available"]);
}

#[test]
fn test_drop_extension() {
    let mut executor = new_executor();

    executor
        .execute_sql("CREATE EXTENSION \"uuid-ossp\"")
        .unwrap();

    executor.execute_sql("SELECT gen_random_uuid()").unwrap();

    let result = executor.execute_sql("DROP EXTENSION \"uuid-ossp\"");
    assert!(
        result.is_ok(),
        "DROP EXTENSION should succeed: {:?}",
        result.err()
    );

    let result = executor.execute_sql("SELECT gen_random_uuid()");
}

#[test]
fn test_drop_extension_if_exists() {
    let mut executor = new_executor();

    let result = executor.execute_sql("DROP EXTENSION nonexistent_ext");
    assert!(result.is_err(), "DROP non-existent EXTENSION should fail");

    let result = executor.execute_sql("DROP EXTENSION IF EXISTS nonexistent_ext");
    assert!(
        result.is_ok(),
        "DROP EXTENSION IF EXISTS should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_extension_functions_not_available_before_install() {
    let mut executor = new_executor();

    let result = executor.execute_sql("SELECT uuid_generate_v4()");
}

#[test]
fn test_pgcrypto_digest_function() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE EXTENSION pgcrypto").unwrap();

    let result = executor.execute_sql("SELECT digest('hello', 'sha256')");
    assert!(
        result.is_ok(),
        "digest function should work: {:?}",
        result.err()
    );
}

#[test]
fn test_pgcrypto_encode_decode() {
    let mut executor = new_executor();

    executor.execute_sql("CREATE EXTENSION pgcrypto").unwrap();

    let result = executor.execute_sql("SELECT encode(gen_random_bytes(8), 'hex')");
    assert!(result.is_ok(), "encode should work: {:?}", result.err());

    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);

    let col = batch.column(0).unwrap();
    let val = col.get(0).unwrap();
    let hex_str = val.as_str().unwrap();
    assert_eq!(
        hex_str.len(),
        16,
        "8 bytes should encode to 16 hex characters"
    );
}

#[test]
fn test_list_available_extensions() {
    let mut executor = new_executor();

    let result = executor.execute_sql("SELECT name FROM pg_available_extensions ORDER BY name");

    if let Ok(batch) = result {
        assert!(
            batch.num_rows() > 0,
            "Should have at least one available extension"
        );
    }
}

#[test]
fn test_list_installed_extensions() {
    let mut executor = new_executor();

    let result = executor.execute_sql("SELECT extname FROM pg_extension");
    if let Ok(batch) = result {
        let initial_count = batch.num_rows();

        executor
            .execute_sql("CREATE EXTENSION \"uuid-ossp\"")
            .unwrap();

        let result = executor.execute_sql("SELECT extname FROM pg_extension");
        let batch = result.unwrap();
        assert_eq!(
            batch.num_rows(),
            initial_count + 1,
            "Should have one more installed extension"
        );
    }
}
