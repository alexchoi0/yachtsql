# Worker 16: DATE_BUCKET Function

## Scope
Implement `DATE_BUCKET` function for date bucketing/binning

## Tests to Fix
- `tests/bigquery/functions/timeseries.rs:15` - `test_date_bucket_2_day`
- `tests/bigquery/functions/timeseries.rs:47` - `test_date_bucket_with_origin`
- `tests/bigquery/functions/timeseries.rs:88` - `test_date_bucket_month_interval`

## Files to Modify
- `src/executor/functions/timeseries.rs` (create or extend)
- `src/planner/functions/` (function registration)

## Implementation Notes
- `DATE_BUCKET(date, INTERVAL n DAY)` - bucket dates by n-day intervals
- `DATE_BUCKET(date, interval, origin)` - bucket with custom origin
- `DATE_BUCKET(date, INTERVAL n MONTH)` - month-based bucketing
- Used for time series aggregation
- Follow BigQuery DATE_BUCKET semantics

## Verification
```bash
cargo nextest run test_date_bucket
```

## Dependencies
None - independent of other workers
