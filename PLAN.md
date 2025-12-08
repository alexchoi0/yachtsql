# Worker 2: PostgreSQL Data Types & Queries

## Objective
Implement ignored tests in PostgreSQL data types and query modules.

## Files to Work On
1. `tests/postgresql/data_types/vector.rs` (31 ignored tests)
2. `tests/postgresql/data_types/geometric.rs` (30 ignored tests)
3. `tests/postgresql/queries/mvcc.rs` (28 ignored tests)
4. `tests/postgresql/queries/information_schema.rs` (23 ignored tests)

## Total: ~112 ignored tests

## Instructions
1. For each ignored test, remove the `#[ignore = "Implement me!"]` attribute
2. Run the test to see what's missing
3. Implement the missing functionality in the executor
4. Ensure the test passes before moving to the next one

## Key Areas
- **Vector type**: pgvector extension support for similarity search
- **Geometric types**: Point, line, box, path, polygon, circle operations
- **MVCC**: Multi-version concurrency control, transaction isolation
- **Information schema**: System catalog queries, metadata access

## Running Tests
```bash
cargo test --test postgresql data_types::vector
cargo test --test postgresql data_types::geometric
cargo test --test postgresql queries::mvcc
cargo test --test postgresql queries::information_schema
```

## Notes
- Do NOT simply add `#[ignore]` to failing tests
- Implement the missing features rather than skipping tests
- Check existing implementations in `crates/executor/src/` for patterns
