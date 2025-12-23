# Worker 19: GAP_FILL Advanced Features

## Scope
Implement advanced `GAP_FILL` features: partitions, multiple columns, origin, subqueries

## Tests to Fix
- `tests/bigquery/functions/timeseries.rs:404` - `test_gap_fill_with_partitions`
- `tests/bigquery/functions/timeseries.rs:460` - `test_gap_fill_multiple_columns`
- `tests/bigquery/functions/timeseries.rs:505` - `test_gap_fill_with_origin`
- `tests/bigquery/functions/timeseries.rs:551` - `test_gap_fill_subquery`

## Files to Modify
- `src/executor/functions/timeseries.rs` (extend GAP_FILL)
- `src/planner/` (if needed for complex query handling)

## Implementation Notes
- Partition support: fill gaps within each partition independently
- Multiple columns: apply different fill strategies to different columns
- Origin: custom starting point for gap detection
- Subquery: GAP_FILL should work on subquery results
- Build on Worker 18's basic GAP_FILL implementation

## Verification
```bash
cargo nextest run test_gap_fill_with_partitions test_gap_fill_multiple_columns test_gap_fill_with_origin test_gap_fill_subquery
```

## Dependencies
Builds on Worker 18's GAP_FILL core - coordinate to avoid conflicts
