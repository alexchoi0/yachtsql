# Worker 9: PARSE Time and Timestamp Functions

## Scope
Implement `PARSE_TIME` and `PARSE_TIMESTAMP` edge cases

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:599` - `test_parse_time_hour_only`
- `tests/bigquery/functions/datetime.rs:919` - `test_parse_timestamp_date_only`

## Files to Modify
- `src/executor/functions/datetime.rs` (extend PARSE functions)
- `src/planner/functions/` (if needed)

## Implementation Notes
- `PARSE_TIME` should handle hour-only format strings
- `PARSE_TIMESTAMP` should handle date-only format strings
- Extend existing format string parsing logic
- Follow BigQuery's format specifier conventions

## Verification
```bash
cargo nextest run test_parse_time_hour_only test_parse_timestamp_date_only
```

## Dependencies
None - independent of other workers
