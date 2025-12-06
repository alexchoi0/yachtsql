use yachtsql::QueryExecutor;
use yachtsql_parser::DialectType;

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::BigQuery)
}

fn setup_users_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE users (id INT64, first_name STRING, last_name STRING, email STRING, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'John', 'Doe', 'JOHN@GMAIL.COM', 'John Doe')")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO users VALUES (2, 'Jane', 'Smith', 'jane@example.com', 'Jane Smith')",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO users VALUES (3, 'Bob', 'Johnson', 'bob@gmail.com', 'Bob Johnson')",
        )
        .unwrap();
}

fn setup_products_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING, description STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Widget', 'A useful widget')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (2, 'Gadget', 'An amazing gadget')")
        .unwrap();
}

fn setup_data_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE data (id INT64, name STRING, text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, '  hello  ', 'The old car is old')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (2, 'world', 'Replace 123 with numbers')")
        .unwrap();
}

fn setup_contacts_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE contacts (id INT64, phone STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO contacts VALUES (1, '(555)-123-4567')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO contacts VALUES (2, '(800)-555-1234')")
        .unwrap();
}

fn get_string(result: &yachtsql_executor::RecordBatch, col: &str, row: usize) -> String {
    result
        .column_by_name(col)
        .unwrap()
        .get(row)
        .unwrap()
        .as_str()
        .unwrap()
        .to_string()
}

fn get_int(result: &yachtsql_executor::RecordBatch, col: &str, row: usize) -> i64 {
    result
        .column_by_name(col)
        .unwrap()
        .get(row)
        .unwrap()
        .as_i64()
        .unwrap()
}

#[allow(dead_code)]
fn is_null(result: &yachtsql_executor::RecordBatch, col: &str, row: usize) -> bool {
    result
        .column_by_name(col)
        .unwrap()
        .get(row)
        .unwrap()
        .is_null()
}

#[test]
fn test_concat_first_last_name() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM users WHERE id = 1",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "full_name", 0), "John Doe");
}

#[test]
fn test_concat_multiple_values() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT CONCAT('Hello', ' ', 'World', '!') AS greeting")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "greeting", 0), "Hello World!");
}

#[test]
fn test_concat_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (a STRING, b STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('hello', NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CONCAT(a, b) AS result FROM test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    assert_eq!(get_string(&result, "result", 0), "hello");
}

#[test]
fn test_substr_from_position() {
    let mut executor = create_executor();
    setup_products_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUBSTR(name, 1, 3) AS prefix FROM products WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "prefix", 0), "Wid");
}

#[test]
fn test_substring_alias() {
    let mut executor = create_executor();
    setup_products_table(&mut executor);

    let result = executor
        .execute_sql("SELECT SUBSTRING(name, 1, 3) AS prefix FROM products WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "prefix", 0), "Wid");
}

#[test]
fn test_substr_unicode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE unicode_test (text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unicode_test VALUES ('hello世界')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUBSTR(text, 6, 2) AS chars FROM unicode_test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "chars", 0), "世界");
}

#[test]
fn test_length_basic() {
    let mut executor = create_executor();
    setup_products_table(&mut executor);

    let result = executor
        .execute_sql("SELECT LENGTH(description) AS len FROM products WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_int(&result, "len", 0), 15);
}

#[test]
fn test_length_unicode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE unicode_test (text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unicode_test VALUES ('hello世界')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT LENGTH(text) AS len FROM unicode_test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);

    assert_eq!(get_int(&result, "len", 0), 7);
}

#[test]
fn test_upper() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT UPPER(name) AS upper_name FROM users WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "upper_name", 0), "JOHN DOE");
}

#[test]
fn test_lower() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT LOWER(email) AS lower_email FROM users WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "lower_email", 0), "john@gmail.com");
}

#[test]
fn test_upper_lower_combined() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT UPPER(name) AS upper_name, LOWER(email) AS lower_email FROM users WHERE id = 1",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "upper_name", 0), "JOHN DOE");
    assert_eq!(get_string(&result, "lower_email", 0), "john@gmail.com");
}

#[test]
fn test_trim() {
    let mut executor = create_executor();
    setup_data_table(&mut executor);

    let result = executor
        .execute_sql("SELECT TRIM(name) AS trimmed FROM data WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "trimmed", 0), "hello");
}

#[test]
fn test_ltrim() {
    let mut executor = create_executor();
    setup_data_table(&mut executor);

    let result = executor
        .execute_sql("SELECT LTRIM(name) AS trimmed FROM data WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "trimmed", 0), "hello  ");
}

#[test]
fn test_rtrim() {
    let mut executor = create_executor();
    setup_data_table(&mut executor);

    let result = executor
        .execute_sql("SELECT RTRIM(name) AS trimmed FROM data WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "trimmed", 0), "  hello");
}

#[test]
fn test_replace() {
    let mut executor = create_executor();
    setup_data_table(&mut executor);

    let result = executor
        .execute_sql("SELECT REPLACE(text, 'old', 'new') AS replaced FROM data WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "replaced", 0), "The new car is new");
}

#[test]
fn test_replace_no_match() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('hello world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT REPLACE(text, 'xyz', 'abc') AS replaced FROM test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "replaced", 0), "hello world");
}

#[test]
fn test_translate() {
    let mut executor = create_executor();
    setup_contacts_table(&mut executor);

    let result = executor
        .execute_sql("SELECT TRANSLATE(phone, '()-', '') AS clean_phone FROM contacts WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "clean_phone", 0), "5551234567");
}

#[test]
fn test_like_prefix() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users WHERE name LIKE 'John%'")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "first_name", 0), "John");
}

#[test]
fn test_like_suffix() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users WHERE name LIKE '%Smith'")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "first_name", 0), "Jane");
}

#[test]
fn test_like_contains() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users WHERE name LIKE '%ohn%'")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_regexp_replace() {
    let mut executor = create_executor();
    setup_data_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT REGEXP_REPLACE(text, '[0-9]+', 'X') AS replaced FROM data WHERE id = 2",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "replaced", 0), "Replace X with numbers");
}

#[test]
fn test_left() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('hello world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT LEFT(text, 5) AS prefix FROM test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "prefix", 0), "hello");
}

#[test]
fn test_right() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('hello world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT RIGHT(text, 5) AS suffix FROM test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "suffix", 0), "world");
}

#[test]
fn test_reverse() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT REVERSE('hello') AS reversed")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "reversed", 0), "olleh");
}

#[test]
fn test_chr() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT CHR(65) AS char").unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "char", 0), "A");
}

#[test]
fn test_empty_string() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT LENGTH(text) AS len, UPPER(text) AS upper FROM test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_int(&result, "len", 0), 0);
    assert_eq!(get_string(&result, "upper", 0), "");
}

#[test]
fn test_starts_with() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users WHERE STARTS_WITH(name, 'John')")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "first_name", 0), "John");
}

#[test]
fn test_ends_with() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users WHERE ENDS_WITH(name, 'Smith')")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "first_name", 0), "Jane");
}
