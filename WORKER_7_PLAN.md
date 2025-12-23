# Worker 7: EXTRACT Week Variants

## Scope
Implement `EXTRACT(WEEK FROM ...)` and week-related extractions

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:446` - `test_extract_week`
- `tests/bigquery/functions/datetime.rs:853` - `test_extract_week_sunday_from_timestamp`

## Files to Modify
- `src/executor/functions/datetime.rs` (add WEEK extraction)
- `src/planner/functions/` (if needed)

## Implementation Notes
- `EXTRACT(WEEK FROM date)` returns week number (0-53)
- BigQuery WEEK starts on Sunday by default
- Handle WEEK(SUNDAY), WEEK(MONDAY) variants if applicable
- Consider ISO week vs calendar week differences

## Verification
```bash
cargo nextest run test_extract_week
```

## Dependencies
None - independent of other workers
