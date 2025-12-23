# Worker 14: TIMESTAMP_TRUNC Basic Units

## Scope
Implement `TIMESTAMP_TRUNC` for SECOND and WEEK units

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:1126` - `test_timestamp_trunc_second`
- `tests/bigquery/functions/datetime.rs:1163` - `test_timestamp_trunc_week`

## Files to Modify
- `src/executor/functions/datetime.rs` (extend TIMESTAMP_TRUNC)
- `src/planner/functions/` (if needed)

## Implementation Notes
- `TIMESTAMP_TRUNC(ts, SECOND)` truncates to second precision
- `TIMESTAMP_TRUNC(ts, WEEK)` truncates to start of week (Sunday)
- Follow existing TRUNC patterns for other units
- Handle subsecond precision correctly

## Verification
```bash
cargo nextest run test_timestamp_trunc_second test_timestamp_trunc_week
```

## Dependencies
None - independent of other workers
