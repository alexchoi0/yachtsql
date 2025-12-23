# Worker 8: EXTRACT Date/Time from Timestamp

## Scope
Implement `EXTRACT(DATE FROM timestamp)` and `EXTRACT(TIME FROM timestamp)`

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:833` - `test_extract_date_from_timestamp`
- `tests/bigquery/functions/datetime.rs:843` - `test_extract_time_from_timestamp`

## Files to Modify
- `src/executor/functions/datetime.rs` (add DATE/TIME extraction from timestamp)
- `src/planner/functions/` (if needed)

## Implementation Notes
- `EXTRACT(DATE FROM timestamp)` returns the DATE portion
- `EXTRACT(TIME FROM timestamp)` returns the TIME portion
- Handle timezone considerations if applicable
- Follow existing EXTRACT patterns in the codebase

## Verification
```bash
cargo nextest run test_extract_date_from_timestamp test_extract_time_from_timestamp
```

## Dependencies
None - independent of other workers
