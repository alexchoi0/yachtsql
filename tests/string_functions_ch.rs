#![cfg(feature = "clickhouse-tests")]

use yachtsql::QueryExecutor;
use yachtsql_parser::DialectType;

fn create_executor() -> QueryExecutor {
    QueryExecutor::with_dialect(DialectType::ClickHouse)
}

fn setup_users_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE users (id Int32, first_name String, last_name String, email String, name String)")
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
        .execute_sql("CREATE TABLE products (id Int32, name String, description String)")
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
        .execute_sql("CREATE TABLE data (id Int32, name String, text String)")
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
        .execute_sql("CREATE TABLE contacts (id Int32, phone String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO contacts VALUES (1, '(555)-123-4567')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO contacts VALUES (2, '(800)-555-1234')")
        .unwrap();
}

fn setup_employees_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE employees (id Int32, name String, department String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (2, 'Bob', 'Engineering')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (3, 'Carol', 'Sales')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (4, 'Dave', 'Sales')")
        .unwrap();
}

fn setup_links_table(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE links (id Int32, url String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO links VALUES (1, 'https://example.com/path')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO links VALUES (2, 'http://test.org/page')")
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
            "SELECT concat(first_name, ' ', last_name) AS full_name FROM users WHERE id = 1",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "full_name", 0), "John Doe");
}

#[test]
fn test_concat_multiple_values() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT concat('Hello', ' ', 'World', '!') AS greeting")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "greeting", 0), "Hello World!");
}

#[test]
fn test_concat_uppercase() {
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
fn test_substring_from_position() {
    let mut executor = create_executor();
    setup_products_table(&mut executor);

    let result = executor
        .execute_sql("SELECT substring(name, 1, 3) AS prefix FROM products WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "prefix", 0), "Wid");
}

#[test]
fn test_substring_unicode() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE unicode_test (text String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unicode_test VALUES ('hello世界')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT substring(text, 6, 2) AS chars FROM unicode_test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "chars", 0), "世界");
}

#[test]
fn test_length_basic() {
    let mut executor = create_executor();
    setup_products_table(&mut executor);

    let result = executor
        .execute_sql("SELECT length(description) AS len FROM products WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_int(&result, "len", 0), 15);
}

#[test]
fn test_char_length() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('hello')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT CHAR_LENGTH(text) AS len FROM test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_int(&result, "len", 0), 5);
}

#[test]
fn test_upper() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT upper(name) AS upper_name FROM users WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "upper_name", 0), "JOHN DOE");
}

#[test]
fn test_lower() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT lower(email) AS lower_email FROM users WHERE id = 1")
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
            "SELECT upper(name) AS upper_name, lower(email) AS lower_email FROM users WHERE id = 1",
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
        .execute_sql("SELECT trim(name) AS trimmed FROM data WHERE id = 1")
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
        .execute_sql("SELECT replace(text, 'old', 'new') AS replaced FROM data WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "replaced", 0), "The new car is new");
}

#[test]
fn test_replace_no_match() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('hello world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT replace(text, 'xyz', 'abc') AS replaced FROM test")
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
            "SELECT replaceRegexpAll(text, '[0-9]+', 'X') AS replaced FROM data WHERE id = 2",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "replaced", 0), "Replace X with numbers");
}

#[test]
#[ignore = "ClickHouse extract() conflicts with SQL EXTRACT for date parts"]
fn test_regexp_substr() {
    let mut executor = create_executor();
    setup_links_table(&mut executor);

    let result = executor
        .execute_sql("SELECT extract(url, 'https?://[^/]+') AS domain FROM links WHERE id = 1")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "domain", 0), "https://example.com");
}

#[test]
fn test_string_agg() {
    let mut executor = create_executor();
    setup_employees_table(&mut executor);

    let result = executor
        .execute_sql("SELECT department, STRING_AGG(name, ', ') AS names FROM employees GROUP BY department ORDER BY department")
        .unwrap();

    assert_eq!(result.num_rows(), 2);
    assert_eq!(get_string(&result, "department", 0), "Engineering");
    let names = get_string(&result, "names", 0);
    assert!(names.contains("Alice"));
    assert!(names.contains("Bob"));
}

#[test]
fn test_position() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT POSITION('world' IN 'hello world') AS pos")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_int(&result, "pos", 0), 7);
}

#[test]
fn test_position_not_found() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT POSITION('xyz' IN 'hello world') AS pos")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_int(&result, "pos", 0), 0);
}

#[test]
fn test_left() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text String)")
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
        .execute_sql("CREATE TABLE test (text String)")
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
fn test_repeat() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT repeat('ab', 3) AS repeated")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "repeated", 0), "ababab");
}

#[test]
fn test_reverse() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT reverse('hello') AS reversed")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "reversed", 0), "olleh");
}

#[test]
fn test_lpad() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT lpad('42', 5, '0') AS padded")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "padded", 0), "00042");
}

#[test]
fn test_rpad() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT rpad('42', 5, '0') AS padded")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "padded", 0), "42000");
}

#[test]
fn test_string_functions_with_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text Nullable(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES (NULL)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT length(text) AS len, upper(text) AS upper, lower(text) AS lower FROM test",
        )
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert!(is_null(&result, "len", 0));
    assert!(is_null(&result, "upper", 0));
    assert!(is_null(&result, "lower", 0));
}

#[test]
fn test_empty_string() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (text String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT length(text) AS len, upper(text) AS upper FROM test")
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
        .execute_sql("SELECT * FROM users WHERE startsWith(name, 'John')")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "first_name", 0), "John");
}

#[test]
fn test_ends_with() {
    let mut executor = create_executor();
    setup_users_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM users WHERE endsWith(name, 'Smith')")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "first_name", 0), "Jane");
}

#[test]
#[ignore = "Array indexing with column alias not yet supported"]
fn test_split_part() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE test (path String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO test VALUES ('a/b/c/d')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT splitByChar('/', path)[2] AS part FROM test")
        .unwrap();

    assert_eq!(result.num_rows(), 1);
    assert_eq!(get_string(&result, "part", 0), "b");
}
