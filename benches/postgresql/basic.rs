use criterion::{BenchmarkId, Criterion, black_box};

use crate::common::{ROW_COUNTS, create_executor, setup_users_table};

pub fn bench_select_all(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_select_all");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| black_box(executor.execute_sql("SELECT * FROM users").unwrap()))
        });
    }
    group.finish();
}

pub fn bench_select_where(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_select_where");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT * FROM users WHERE age > 30")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}

pub fn bench_select_order_by(c: &mut Criterion) {
    let mut group = c.benchmark_group("pg_select_order_by");
    for &rows in &ROW_COUNTS {
        let mut executor = create_executor();
        setup_users_table(&mut executor, rows);
        group.bench_with_input(BenchmarkId::from_parameter(rows), &rows, |b, _| {
            b.iter(|| {
                black_box(
                    executor
                        .execute_sql("SELECT * FROM users ORDER BY name")
                        .unwrap(),
                )
            })
        });
    }
    group.finish();
}
