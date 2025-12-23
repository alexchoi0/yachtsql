# Worker 17: DATETIME_BUCKET and TIMESTAMP_BUCKET Functions

## Scope
Implement `DATETIME_BUCKET` and `TIMESTAMP_BUCKET` functions

## Tests to Fix
- `tests/bigquery/functions/timeseries.rs:107` - `test_datetime_bucket_12_hour`
- `tests/bigquery/functions/timeseries.rs:139` - `test_datetime_bucket_with_origin`
- `tests/bigquery/functions/timeseries.rs:198` - `test_timestamp_bucket_12_hour`
- `tests/bigquery/functions/timeseries.rs:230` - `test_timestamp_bucket_with_origin`

## Files to Modify
- `src/executor/functions/timeseries.rs` (create or extend)
- `src/planner/functions/` (function registration)

## Implementation Notes
- Similar to DATE_BUCKET but for DATETIME and TIMESTAMP types
- Support hour-based intervals (e.g., 12 HOUR)
- Support custom origin parameter
- Used for time series aggregation at sub-day granularity

## Verification
```bash
cargo nextest run test_datetime_bucket test_timestamp_bucket
```

## Dependencies
None - independent of other workers (coordinate with Worker 16 on shared patterns)
