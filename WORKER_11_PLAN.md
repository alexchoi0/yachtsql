# Worker 11: TIMESTAMP Constructor Functions

## Scope
Implement `TIMESTAMP()` constructor with various input types

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:938` - `test_timestamp_constructor_from_string`
- `tests/bigquery/functions/datetime.rs:948` - `test_timestamp_constructor_from_string_with_timezone`
- `tests/bigquery/functions/datetime.rs:958` - `test_timestamp_constructor_from_date`

## Files to Modify
- `src/executor/functions/datetime.rs` (add TIMESTAMP constructor)
- `src/planner/functions/` (if needed for function registration)

## Implementation Notes
- `TIMESTAMP(string)` parses a string to timestamp
- `TIMESTAMP(string, timezone)` parses with timezone
- `TIMESTAMP(date)` converts DATE to TIMESTAMP (at midnight)
- Handle timezone parsing and conversion
- Follow BigQuery's timestamp string format conventions

## Verification
```bash
cargo nextest run test_timestamp_constructor_from_string test_timestamp_constructor_from_string_with_timezone test_timestamp_constructor_from_date
```

## Dependencies
None - independent of other workers
