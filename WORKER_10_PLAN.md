# Worker 10: TIME Constructor Function

## Scope
Implement `TIME(hour, minute, second)` constructor

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:636` - `test_time_constructor_from_parts`

## Files to Modify
- `src/executor/functions/datetime.rs` (add TIME constructor)
- `src/planner/functions/` (if needed for function registration)

## Implementation Notes
- `TIME(hour, minute, second)` creates a TIME value from components
- Validate hour (0-23), minute (0-59), second (0-59)
- Handle invalid inputs appropriately
- Follow existing constructor patterns (like DATE constructor if exists)

## Verification
```bash
cargo nextest run test_time_constructor_from_parts
```

## Dependencies
None - independent of other workers
