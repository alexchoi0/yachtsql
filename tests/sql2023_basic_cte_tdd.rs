#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::approx_constant)]

mod common;
use common::{
    assert_row_count, exec_ok, get_f64_by_name, get_i64_by_name, get_string_by_name, new_executor,
    query,
};
use yachtsql::QueryExecutor;

fn setup_basic_cte_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE yacht_owners (
            id INT64 PRIMARY KEY,
            name STRING,
            email STRING,
            status STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO yacht_owners VALUES
            (1, 'Alice Johnson', 'alice@example.com', 'active'),
            (2, 'Bob Smith', 'bob@example.com', 'active'),
            (3, 'Carol Williams', 'carol@example.com', 'inactive'),
            (4, 'David Brown', 'david@example.com', 'active')",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE orders (
            id INT64 PRIMARY KEY,
            owner_id INT64,
            amount FLOAT64,
            order_date STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO orders VALUES
            (1, 1, 1500.00, '2025-01-01'),
            (2, 1, 800.00, '2025-01-05'),
            (3, 2, 2000.00, '2025-01-03'),
            (4, 2, 500.00, '2025-01-07'),
            (5, 3, 300.00, '2024-10-01'),
            (6, 4, 1200.00, '2025-01-10')",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE equipment (
            id INT64 PRIMARY KEY,
            name STRING,
            category STRING,
            price FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO equipment VALUES
            (1, 'Anchor', 'Safety', 250.00),
            (2, 'GPS', 'Navigation', 500.00),
            (3, 'Life Jacket', 'Safety', 75.00),
            (4, 'Radar', 'Navigation', 2500.00),
            (5, 'Fire Extinguisher', 'Safety', 50.00)",
    );

    executor
}

fn setup_employee_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE fleets (
            id INT64 PRIMARY KEY,
            name STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO fleets VALUES
            (1, 'Atlantic Fleet'),
            (2, 'Pacific Fleet'),
            (3, 'Mediterranean Fleet')",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE crew_members (
            id INT64 PRIMARY KEY,
            name STRING,
            email STRING,
            dept_id INT64,
            salary FLOAT64,
            hire_date STRING,
            crew_member_id INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO crew_members VALUES
            (1, 'John Captain', 'john@fleet.com', 1, 80000.00, '2020-03-15', 1),
            (2, 'Jane Navigator', 'jane@fleet.com', 1, 70000.00, '2021-06-01', 2),
            (3, 'Mike Engineer', 'mike@fleet.com', 2, 90000.00, '2019-01-10', 3),
            (4, 'Sarah Cook', 'sarah@fleet.com', 2, 60000.00, '2022-04-20', 4),
            (5, 'Tom Deckhand', 'tom@fleet.com', 3, 50000.00, '2023-01-05', 5),
            (6, 'Lisa Mechanic', 'lisa@fleet.com', 3, 65000.00, '2020-08-12', 6)",
    );

    executor
}

fn setup_inventory_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE equipment (
            id INT64 PRIMARY KEY,
            name STRING,
            category STRING,
            price FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO equipment VALUES
            (1, 'Anchor', 'Safety', 250.00),
            (2, 'GPS', 'Navigation', 500.00),
            (3, 'Life Jacket', 'Safety', 75.00)",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE order_items (
            id INT64 PRIMARY KEY,
            equipment_id INT64,
            quantity FLOAT64,
            amount FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO order_items VALUES
            (1, 1, 5.0, 1250.00),
            (2, 2, 3.0, 1500.00),
            (3, 1, 2.0, 500.00),
            (4, 3, 10.0, 750.00)",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE inventory (
            id INT64 PRIMARY KEY,
            equipment_id INT64,
            unit_cost FLOAT64,
            inventory_quantity FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO inventory VALUES
            (1, 1, 150.00, 20.0),
            (2, 2, 300.00, 10.0),
            (3, 3, 40.00, 50.0)",
    );

    executor
}

fn setup_sales_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE sales (
            id INT64 PRIMARY KEY,
            sale_date STRING,
            amount FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO sales VALUES
            (1, '2025-01-01', 500.00),
            (2, '2025-01-01', 300.00),
            (3, '2025-01-02', 700.00),
            (4, '2025-01-02', 200.00),
            (5, '2025-01-03', 1000.00)",
    );

    executor
}

fn setup_contacts_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE yacht_owners (
            id INT64 PRIMARY KEY,
            name STRING,
            email STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO yacht_owners VALUES
            (1, 'Alice', 'alice@customer.com'),
            (2, 'Bob', 'bob@customer.com')",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE suppliers (
            id INT64 PRIMARY KEY,
            name STRING,
            email STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO suppliers VALUES
            (1, 'Marine Supply Co', 'contact@marine.com'),
            (2, 'Boat Parts Inc', 'info@boatparts.com')",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE crew_members (
            id INT64 PRIMARY KEY,
            name STRING,
            email STRING,
            dept_id INT64,
            salary FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO crew_members VALUES
            (1, 'John', 'john@crew.com', 1, 50000.00),
            (2, 'Jane', 'jane@crew.com', 2, 60000.00),
            (3, 'Mike', 'mike@crew.com', 1, 55000.00)",
    );

    executor
}

fn setup_performance_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE crew_members (
            id INT64 PRIMARY KEY,
            name STRING,
            dept_id INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO crew_members VALUES
            (1, 'John', 1),
            (2, 'Jane', 2),
            (3, 'Mike', 1),
            (4, 'Sarah', 2)",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE performance_reviews (
            id INT64 PRIMARY KEY,
            crew_member_id INT64,
            rating INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO performance_reviews VALUES
            (1, 1, 5),
            (2, 2, 3),
            (3, 3, 4),
            (4, 4, 2)",
    );

    executor
}

fn setup_user_tables() -> QueryExecutor {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE users (
            id INT64 PRIMARY KEY,
            name STRING,
            email STRING,
            last_login STRING
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO users VALUES
            (1, 'Alice', 'alice@test.com', '2025-01-01'),
            (2, 'Bob', 'bob@test.com', '2025-01-02'),
            (3, 'Carol', 'carol@test.com', '2024-06-01'),
            (4, 'David', 'david@test.com', '2025-01-03')",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE orders (
            id INT64 PRIMARY KEY,
            user_id INT64,
            total_amount FLOAT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO orders VALUES
            (1, 1, 15000.00),
            (2, 1, 2000.00),
            (3, 2, 6000.00),
            (4, 4, 500.00),
            (5, 4, 300.00)",
    );

    executor
}

#[test]
fn test_basic_cte_with_aggregates() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH sales_summary AS (
            SELECT owner_id,
                   SUM(amount) AS total_sales,
                   COUNT(*) AS order_count
            FROM orders
            GROUP BY owner_id
        )
        SELECT c.name,
               ss.total_sales,
               ss.order_count
        FROM yacht_owners c
        JOIN sales_summary ss ON c.id = ss.owner_id
        WHERE ss.total_sales > 1000
        ORDER BY ss.total_sales DESC",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_string_by_name(&result, 0, "name"), "Bob Smith");
    assert_eq!(get_f64_by_name(&result, 0, "total_sales"), 2500.0);
    assert_eq!(get_i64_by_name(&result, 0, "order_count"), 2);
}

#[test]
fn test_cte_with_where_clause() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH high_value_equipment AS (
            SELECT *
            FROM equipment
            WHERE price > 100
        )
        SELECT category, COUNT(*) AS product_count
        FROM high_value_equipment
        GROUP BY category
        ORDER BY category",
    );

    assert_row_count(&result, 2);

    assert_eq!(get_string_by_name(&result, 0, "category"), "Navigation");
    assert_eq!(get_i64_by_name(&result, 0, "product_count"), 2);
    assert_eq!(get_string_by_name(&result, 1, "category"), "Safety");
    assert_eq!(get_i64_by_name(&result, 1, "product_count"), 1);
}

#[test]
fn test_multiple_ctes_basic() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH
            active_yacht_owners AS (
                SELECT id, name
                FROM yacht_owners
                WHERE status = 'active'
            ),
            owner_order_totals AS (
                SELECT owner_id,
                       COUNT(*) AS order_count,
                       SUM(amount) AS total_amount
                FROM orders
                GROUP BY owner_id
            )
        SELECT ac.name, ot.order_count, ot.total_amount
        FROM active_yacht_owners ac
        JOIN owner_order_totals ot ON ac.id = ot.owner_id
        ORDER BY ot.total_amount DESC",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_string_by_name(&result, 0, "name"), "Bob Smith");
    assert_eq!(get_i64_by_name(&result, 0, "order_count"), 2);
}

#[test]
fn test_chained_ctes() {
    let mut executor = setup_employee_tables();

    let result = query(
        &mut executor,
        "WITH
            dept_salaries AS (
                SELECT dept_id,
                       AVG(salary) AS avg_salary
                FROM crew_members
                GROUP BY dept_id
            ),
            high_paying_depts AS (
                SELECT dept_id, avg_salary
                FROM dept_salaries
                WHERE avg_salary > 60000
            )
        SELECT d.name AS fleet,
               hpd.avg_salary
        FROM fleets d
        JOIN high_paying_depts hpd ON d.id = hpd.dept_id
        ORDER BY hpd.avg_salary DESC",
    );

    assert_row_count(&result, 2);

    assert!(get_f64_by_name(&result, 0, "avg_salary") == 75000.0);
}

#[test]
fn test_cte_containing_join() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH customer_orders AS (
            SELECT c.id AS owner_id,
                   c.name,
                   o.id AS order_id,
                   o.amount
            FROM yacht_owners c
            INNER JOIN orders o ON c.id = o.owner_id
        )
        SELECT name, COUNT(order_id) AS order_count
        FROM customer_orders
        GROUP BY name
        ORDER BY order_count DESC",
    );

    assert_row_count(&result, 4);

    assert_eq!(get_i64_by_name(&result, 0, "order_count"), 2);
}

#[test]
fn test_multiple_ctes_with_joins() {
    let mut executor = setup_inventory_tables();

    let result = query(
        &mut executor,
        "WITH
            product_sales AS (
                SELECT equipment_id,
                       SUM(quantity) AS total_quantity,
                       SUM(amount) AS total_revenue
                FROM order_items
                GROUP BY equipment_id
            ),
            product_with_costs AS (
                SELECT ps.equipment_id,
                       ps.total_revenue,
                       i.unit_cost * i.inventory_quantity AS total_cost,
                       ps.total_revenue - (i.unit_cost * i.inventory_quantity) AS profit
                FROM product_sales ps
                JOIN inventory i ON ps.equipment_id = i.equipment_id
            )
        SELECT equipment_id, total_revenue, total_cost, profit
        FROM product_with_costs
        ORDER BY equipment_id",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_i64_by_name(&result, 0, "equipment_id"), 1);
    assert_eq!(get_f64_by_name(&result, 0, "total_revenue"), 1750.0);
    assert_eq!(get_f64_by_name(&result, 0, "total_cost"), 3000.0);
    assert_eq!(get_f64_by_name(&result, 0, "profit"), -1250.0);
}

#[test]
fn test_cte_with_group_by() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH order_stats AS (
            SELECT owner_id,
                   MIN(amount) AS min_amount,
                   MAX(amount) AS max_amount,
                   AVG(amount) AS avg_amount
            FROM orders
            GROUP BY owner_id
        )
        SELECT y.name, os.min_amount, os.max_amount, os.avg_amount
        FROM yacht_owners y
        JOIN order_stats os ON y.id = os.owner_id
        ORDER BY os.avg_amount DESC",
    );

    assert_row_count(&result, 4);
}

#[test]
fn test_nested_cte_references() {
    let mut executor = setup_employee_tables();

    let result = query(
        &mut executor,
        "WITH
            base_data AS (
                SELECT dept_id, id AS crew_member_id, salary
                FROM crew_members
                WHERE hire_date >= '2020-01-01'
            ),
            dept_stats AS (
                SELECT dept_id,
                       COUNT(*) AS emp_count,
                       AVG(salary) AS avg_salary
                FROM base_data
                GROUP BY dept_id
            )
        SELECT d.name,
               ds.emp_count,
               ds.avg_salary
        FROM fleets d
        JOIN dept_stats ds ON d.id = ds.dept_id
        ORDER BY ds.avg_salary DESC",
    );

    assert_row_count(&result, 3);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Atlantic Fleet");
}

#[test]
fn test_cte_with_union() {
    let mut executor = setup_contacts_tables();

    let result = query(
        &mut executor,
        "WITH all_contacts AS (
            SELECT email, 'customer' AS type
            FROM yacht_owners
            UNION
            SELECT email, 'supplier' AS type
            FROM suppliers
            UNION
            SELECT email, 'crew_member' AS type
            FROM crew_members
        )
        SELECT type, COUNT(*) AS contact_count
        FROM all_contacts
        GROUP BY type
        ORDER BY type",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_string_by_name(&result, 0, "type"), "crew_member");
    assert_eq!(get_i64_by_name(&result, 0, "contact_count"), 3);
    assert_eq!(get_string_by_name(&result, 1, "type"), "customer");
    assert_eq!(get_i64_by_name(&result, 1, "contact_count"), 2);
    assert_eq!(get_string_by_name(&result, 2, "type"), "supplier");
    assert_eq!(get_i64_by_name(&result, 2, "contact_count"), 2);
}

#[test]
fn test_cte_with_rank_window_function() {
    let mut executor = setup_contacts_tables();

    let result = query(
        &mut executor,
        "WITH ranked_crew_members AS (
            SELECT dept_id,
                   name,
                   salary,
                   RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) AS salary_rank
            FROM crew_members
        )
        SELECT dept_id, name, salary
        FROM ranked_crew_members
        WHERE salary_rank <= 3
        ORDER BY dept_id, salary DESC",
    );

    assert_row_count(&result, 3);
}

#[test]
fn test_cte_with_aggregation_and_filter() {
    let mut executor = setup_sales_tables();

    let result = query(
        &mut executor,
        "WITH daily_totals AS (
            SELECT sale_date,
                   SUM(amount) AS daily_total
            FROM sales
            GROUP BY sale_date
        )
        SELECT sale_date, daily_total
        FROM daily_totals
        WHERE daily_total >= 900
        ORDER BY sale_date",
    );

    assert_row_count(&result, 2);
    assert_eq!(get_f64_by_name(&result, 0, "daily_total"), 900.0);
    assert_eq!(get_f64_by_name(&result, 1, "daily_total"), 1000.0);
}

#[test]
fn test_cte_with_scalar_subquery_simple() {
    let mut executor = setup_employee_tables();

    let result = query(
        &mut executor,
        "WITH dept_totals AS (
            SELECT dept_id, SUM(salary) AS total_salary
            FROM crew_members
            GROUP BY dept_id
        )
        SELECT d.name,
               dt.total_salary
        FROM fleets d
        JOIN dept_totals dt ON d.id = dt.dept_id
        ORDER BY dt.total_salary DESC",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_f64_by_name(&result, 0, "total_salary"), 150000.0);
}

#[test]
fn test_cte_in_where_join_subquery() {
    let mut executor = setup_performance_tables();

    let result = query(
        &mut executor,
        "WITH high_performers AS (
            SELECT crew_member_id
            FROM performance_reviews
            WHERE rating >= 4
        )
        SELECT c.name, c.dept_id
        FROM crew_members c
        JOIN high_performers hp ON c.id = hp.crew_member_id
        ORDER BY c.name",
    );

    assert_row_count(&result, 2);
    assert_eq!(get_string_by_name(&result, 0, "name"), "John");
    assert_eq!(get_string_by_name(&result, 1, "name"), "Mike");
}

#[test]
fn test_cte_with_order_by_and_limit() {
    let mut executor = setup_inventory_tables();

    let result = query(
        &mut executor,
        "WITH top_sellers AS (
            SELECT equipment_id,
                   SUM(quantity) AS total_sold
            FROM order_items
            GROUP BY equipment_id
            ORDER BY total_sold DESC
            LIMIT 2
        )
        SELECT p.name,
               p.category,
               ts.total_sold
        FROM equipment p
        JOIN top_sellers ts ON p.id = ts.equipment_id
        ORDER BY ts.total_sold DESC",
    );

    assert_row_count(&result, 2);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Life Jacket");
    assert_eq!(get_f64_by_name(&result, 0, "total_sold"), 10.0);
    assert_eq!(get_string_by_name(&result, 1, "name"), "Anchor");
    assert_eq!(get_f64_by_name(&result, 1, "total_sold"), 7.0);
}

#[test]
fn test_complex_cte_user_segmentation() {
    let mut executor = setup_user_tables();

    let result = query(
        &mut executor,
        "WITH
            active_users AS (
                SELECT id, name, email
                FROM users
                WHERE last_login >= '2025-01-01'
            ),
            order_totals AS (
                SELECT user_id,
                       COUNT(*) AS order_count,
                       SUM(total_amount) AS total_spent
                FROM orders
                GROUP BY user_id
            ),
            user_segments AS (
                SELECT ot.user_id,
                       ot.order_count,
                       ot.total_spent,
                       CASE
                           WHEN ot.total_spent > 10000 THEN 'VIP'
                           WHEN ot.total_spent > 5000 THEN 'Premium'
                           WHEN ot.total_spent > 1000 THEN 'Standard'
                           ELSE 'Basic'
                       END AS segment
                FROM order_totals ot
                JOIN active_users au ON ot.user_id = au.id
            )
        SELECT au.name,
               au.email,
               us.segment,
               us.order_count,
               us.total_spent
        FROM active_users au
        JOIN user_segments us ON au.id = us.user_id
        ORDER BY us.total_spent DESC",
    );

    assert_row_count(&result, 3);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Alice");
    assert_eq!(get_string_by_name(&result, 0, "segment"), "VIP");
    assert_eq!(get_f64_by_name(&result, 0, "total_spent"), 17000.0);
    assert_eq!(get_string_by_name(&result, 1, "name"), "Bob");
    assert_eq!(get_string_by_name(&result, 1, "segment"), "Premium");
    assert_eq!(get_string_by_name(&result, 2, "name"), "David");
    assert_eq!(get_string_by_name(&result, 2, "segment"), "Basic");
}

#[test]
fn test_cte_multiple_references_via_aggregate() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH order_totals AS (
            SELECT owner_id, SUM(amount) AS total
            FROM orders
            GROUP BY owner_id
        )
        SELECT
            COUNT(*) AS num_owners,
            AVG(total) AS avg_total,
            MAX(total) AS max_total
        FROM order_totals",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_i64_by_name(&result, 0, "num_owners"), 4);
}

#[test]
fn test_cte_empty_result() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH no_orders AS (
            SELECT *
            FROM orders
            WHERE amount > 100000
        )
        SELECT COUNT(*) AS cnt FROM no_orders",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_i64_by_name(&result, 0, "cnt"), 0);
}

#[test]
fn test_cte_with_null_handling() {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE nullable_data (
            id INT64 PRIMARY KEY,
            value INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO nullable_data VALUES (1, 100), (2, NULL), (3, 200), (4, NULL)",
    );

    let result = query(
        &mut executor,
        "WITH null_stats AS (
            SELECT
                COUNT(*) AS total_count,
                COUNT(value) AS non_null_count
            FROM nullable_data
        )
        SELECT * FROM null_stats",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_i64_by_name(&result, 0, "total_count"), 4);
    assert_eq!(get_i64_by_name(&result, 0, "non_null_count"), 2);
}

#[test]
fn test_cte_with_distinct() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH distinct_categories AS (
            SELECT DISTINCT category
            FROM equipment
        )
        SELECT COUNT(*) AS category_count
        FROM distinct_categories",
    );

    assert_row_count(&result, 1);

    assert_eq!(get_i64_by_name(&result, 0, "category_count"), 2);
}

#[test]
fn test_cte_column_aliasing() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH aliased_data AS (
            SELECT
                id AS equipment_id,
                name AS equipment_name,
                price * 1.1 AS price_with_tax
            FROM equipment
        )
        SELECT equipment_id, equipment_name, price_with_tax
        FROM aliased_data
        ORDER BY price_with_tax DESC
        LIMIT 1",
    );

    assert_row_count(&result, 1);

    assert_eq!(get_string_by_name(&result, 0, "equipment_name"), "Radar");
    assert_eq!(get_f64_by_name(&result, 0, "price_with_tax"), 2750.0);
}

#[test]
fn test_three_level_chained_ctes() {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE numbers (id INT64 PRIMARY KEY, val INT64)",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)",
    );

    let result = query(
        &mut executor,
        "WITH
            level1 AS (
                SELECT id, val * 2 AS doubled
                FROM numbers
            ),
            level2 AS (
                SELECT id, doubled + 5 AS adjusted
                FROM level1
            ),
            level3 AS (
                SELECT AVG(adjusted) AS final_avg
                FROM level2
            )
        SELECT final_avg FROM level3",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_f64_by_name(&result, 0, "final_avg"), 65.0);
}

#[test]
fn test_cte_with_self_join() {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE employees (
            id INT64 PRIMARY KEY,
            name STRING,
            manager_id INT64
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO employees VALUES
            (1, 'CEO', NULL),
            (2, 'VP Sales', 1),
            (3, 'VP Engineering', 1),
            (4, 'Sales Rep', 2),
            (5, 'Engineer', 3)",
    );

    let result = query(
        &mut executor,
        "WITH employee_managers AS (
            SELECT
                e.id AS emp_id,
                e.name AS employee,
                m.name AS manager
            FROM employees e
            INNER JOIN employees m ON e.manager_id = m.id
        )
        SELECT emp_id, employee, manager
        FROM employee_managers
        ORDER BY emp_id",
    );

    assert_row_count(&result, 4);

    assert_eq!(get_i64_by_name(&result, 0, "emp_id"), 2);
}

#[test]
fn test_cte_with_exists_via_join() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH owners_with_orders AS (
            SELECT DISTINCT owner_id
            FROM orders
        )
        SELECT y.name
        FROM yacht_owners y
        JOIN owners_with_orders owo ON y.id = owo.owner_id
        ORDER BY y.name",
    );

    assert_row_count(&result, 4);
}

#[test]
fn test_cte_with_not_exists_via_left_join() {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE customers (id INT64 PRIMARY KEY, name STRING)",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol')",
    );

    exec_ok(
        &mut executor,
        "CREATE TABLE purchases (id INT64 PRIMARY KEY, customer_id INT64)",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO purchases VALUES (1, 1), (2, 1), (3, 2)",
    );

    let result = query(
        &mut executor,
        "WITH buyers AS (
            SELECT DISTINCT customer_id
            FROM purchases
        )
        SELECT c.name
        FROM customers c
        LEFT JOIN buyers b ON c.id = b.customer_id
        WHERE b.customer_id IS NULL
        ORDER BY c.name",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Carol");
}

#[test]
fn test_cte_with_case_expression() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH categorized_orders AS (
            SELECT
                id,
                amount,
                CASE
                    WHEN amount >= 1500 THEN 'Large'
                    WHEN amount >= 500 THEN 'Medium'
                    ELSE 'Small'
                END AS size_category
            FROM orders
        )
        SELECT size_category, COUNT(*) AS count
        FROM categorized_orders
        GROUP BY size_category
        ORDER BY size_category",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_string_by_name(&result, 0, "size_category"), "Large");
    assert_eq!(get_i64_by_name(&result, 0, "count"), 2);
}

#[test]
fn test_cte_preserves_column_types() {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE typed_data (
            int_col INT64,
            float_col FLOAT64,
            str_col STRING,
            bool_col BOOL
        )",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO typed_data VALUES (42, 3.14, 'hello', TRUE)",
    );

    let result = query(
        &mut executor,
        "WITH preserved AS (
            SELECT int_col, float_col, str_col, bool_col
            FROM typed_data
        )
        SELECT * FROM preserved",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_i64_by_name(&result, 0, "int_col"), 42);
    assert_eq!(get_f64_by_name(&result, 0, "float_col"), 3.14);
    assert_eq!(get_string_by_name(&result, 0, "str_col"), "hello");
}

#[test]
fn test_cte_with_coalesce_float() {
    let mut executor = new_executor();

    exec_ok(
        &mut executor,
        "CREATE TABLE optional_values (id INT64, value FLOAT64)",
    );
    exec_ok(
        &mut executor,
        "INSERT INTO optional_values VALUES (1, NULL), (2, 100.0), (3, NULL)",
    );

    let result = query(
        &mut executor,
        "WITH defaulted AS (
            SELECT id, COALESCE(value, 0.0) AS safe_value
            FROM optional_values
        )
        SELECT SUM(safe_value) AS total
        FROM defaulted",
    );

    assert_row_count(&result, 1);

    assert_eq!(get_f64_by_name(&result, 0, "total"), 100.0);
}

#[test]
fn test_cte_with_literal_values() {
    let mut executor = new_executor();

    let result = query(
        &mut executor,
        "WITH constants AS (
            SELECT 1 AS one, 'hello' AS greeting, 3.14 AS pi
        )
        SELECT * FROM constants",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_i64_by_name(&result, 0, "one"), 1);
    assert_eq!(get_string_by_name(&result, 0, "greeting"), "hello");
    assert_eq!(get_f64_by_name(&result, 0, "pi"), 3.14);
}

#[test]
fn test_cte_simple_aggregation() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH total_sales AS (
            SELECT SUM(amount) AS grand_total
            FROM orders
        )
        SELECT grand_total FROM total_sales",
    );

    assert_row_count(&result, 1);

    assert_eq!(get_f64_by_name(&result, 0, "grand_total"), 6300.0);
}

#[test]
fn test_cte_with_between() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH medium_orders AS (
            SELECT * FROM orders
            WHERE amount BETWEEN 500 AND 1500
        )
        SELECT COUNT(*) AS count FROM medium_orders",
    );

    assert_row_count(&result, 1);

    assert_eq!(get_i64_by_name(&result, 0, "count"), 4);
}

#[test]
fn test_cte_with_like() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH smith_family AS (
            SELECT * FROM yacht_owners
            WHERE name LIKE '%Smith%'
        )
        SELECT name FROM smith_family",
    );

    assert_row_count(&result, 1);
    assert_eq!(get_string_by_name(&result, 0, "name"), "Bob Smith");
}

#[test]
fn test_cte_row_number_window() {
    let mut executor = setup_basic_cte_tables();

    let result = query(
        &mut executor,
        "WITH numbered_orders AS (
            SELECT
                id,
                amount,
                ROW_NUMBER() OVER (ORDER BY amount DESC) AS rn
            FROM orders
        )
        SELECT id, amount, rn
        FROM numbered_orders
        WHERE rn <= 3
        ORDER BY rn",
    );

    assert_row_count(&result, 3);

    assert_eq!(get_i64_by_name(&result, 0, "rn"), 1);
    assert_eq!(get_f64_by_name(&result, 0, "amount"), 2000.0);
}
