# Worker 12: STRING Function for Timestamp

## Scope
Implement `STRING(timestamp)` conversion function

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:977` - `test_string_from_timestamp`

## Files to Modify
- `src/executor/functions/datetime.rs` or `src/executor/functions/string.rs`
- `src/planner/functions/` (if needed)

## Implementation Notes
- `STRING(timestamp)` converts timestamp to string representation
- May support optional timezone parameter
- Follow BigQuery's default timestamp string format
- Handle NULL inputs appropriately

## Verification
```bash
cargo nextest run test_string_from_timestamp
```

## Dependencies
None - independent of other workers
