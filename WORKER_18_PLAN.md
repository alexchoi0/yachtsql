# Worker 18: GAP_FILL Basic Functionality

## Scope
Implement basic `GAP_FILL` function for time series gap filling

## Tests to Fix
- `tests/bigquery/functions/timeseries.rs:280` - `test_gap_fill_locf`
- `tests/bigquery/functions/timeseries.rs:323` - `test_gap_fill_linear`
- `tests/bigquery/functions/timeseries.rs:366` - `test_gap_fill_null`

## Files to Modify
- `src/executor/functions/timeseries.rs` (create or extend)
- `src/planner/` (may need special planning for table-valued function)

## Implementation Notes
- `GAP_FILL` is a table-valued function that fills gaps in time series
- `LOCF` - Last Observation Carried Forward
- `LINEAR` - Linear interpolation between values
- `NULL` - Fill gaps with NULL
- This is a complex function that operates on result sets

## Verification
```bash
cargo nextest run test_gap_fill_locf test_gap_fill_linear test_gap_fill_null
```

## Dependencies
None - independent of other workers (coordinate with Worker 19 on GAP_FILL core)
