# Worker 15: TIMESTAMP_TRUNC ISO Week Variants

## Scope
Implement `TIMESTAMP_TRUNC` for WEEK(MONDAY), ISOWEEK, and ISOYEAR

## Tests to Fix
- `tests/bigquery/functions/datetime.rs:1173` - `test_timestamp_trunc_week_monday`
- `tests/bigquery/functions/datetime.rs:1185` - `test_timestamp_trunc_isoweek`
- `tests/bigquery/functions/datetime.rs:1195` - `test_timestamp_trunc_isoyear`

## Files to Modify
- `src/executor/functions/datetime.rs` (extend TIMESTAMP_TRUNC)
- `src/planner/functions/` (if needed)

## Implementation Notes
- `TIMESTAMP_TRUNC(ts, WEEK(MONDAY))` - week starting Monday
- `TIMESTAMP_TRUNC(ts, ISOWEEK)` - ISO 8601 week (starts Monday)
- `TIMESTAMP_TRUNC(ts, ISOYEAR)` - ISO 8601 year
- ISO week 1 is the week containing January 4th
- ISO year may differ from calendar year at year boundaries

## Verification
```bash
cargo nextest run test_timestamp_trunc_week_monday test_timestamp_trunc_isoweek test_timestamp_trunc_isoyear
```

## Dependencies
None - independent of other workers
