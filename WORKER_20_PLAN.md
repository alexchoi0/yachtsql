# Worker 20: DML Insert and UUID Functions

## Scope
Implement advanced INSERT features and UUID utility functions

## Tests to Fix

### UUID Functions (`tests/bigquery/functions/utility.rs`)
- Line 33 - `test_generate_uuid_lowercase`
- Line 46 - `test_generate_uuid_hyphen_positions`

### DML Insert (`tests/bigquery/dml/insert.rs`)
- Line 282 - `test_insert_select_with_unnest`
- Line 315 - `test_insert_select_with_cte`
- Line 343 - `test_insert_with_nested_struct`
- Line 403 - `test_insert_values_with_subquery`
- Line 482 - `test_insert_with_range_type`

## Files to Modify
- `src/executor/functions/utility.rs` (UUID functions)
- `src/executor/dml/insert.rs` (INSERT statement handling)
- `src/planner/dml/` (if needed for INSERT planning)

## Implementation Notes

### UUID
- `GENERATE_UUID()` should return lowercase hex
- UUID format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
- Verify hyphen positions (8-4-4-4-12 pattern)

### INSERT
- INSERT ... SELECT with UNNEST in source
- INSERT ... SELECT with CTE (WITH clause)
- INSERT with nested STRUCT values
- INSERT VALUES with subquery expressions
- INSERT with RANGE type values

## Verification
```bash
cargo nextest run test_generate_uuid test_insert_select_with_unnest test_insert_select_with_cte test_insert_with_nested_struct test_insert_values_with_subquery test_insert_with_range_type
```

## Dependencies
None - independent of other workers
