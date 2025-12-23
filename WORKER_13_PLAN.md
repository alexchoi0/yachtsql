# Worker 13: TIMESTAMP_ADD and TIMESTAMP_DIFF Extensions

## Scope
Extend `TIMESTAMP_ADD` for milliseconds and fix `TIMESTAMP_DIFF` for negative results

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:1014` - `test_timestamp_add_millisecond`
- `tests/bigquery/functions/datetime.rs:1080` - `test_timestamp_diff_negative`

## Files to Modify
- `src/executor/functions/datetime.rs` (extend existing functions)
- `src/planner/functions/` (if needed)

## Implementation Notes
- `TIMESTAMP_ADD(ts, INTERVAL n MILLISECOND)` - add millisecond support
- `TIMESTAMP_DIFF` should correctly return negative values when first timestamp is before second
- Check existing interval unit handling and extend as needed

## Verification
```bash
cargo nextest run test_timestamp_add_millisecond test_timestamp_diff_negative
```

## Dependencies
None - independent of other workers
