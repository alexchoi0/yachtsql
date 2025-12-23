# Worker 6: IEEE Division Functions

## Scope
Implement `IEEE_DIVIDE` function for IEEE 754 compliant division

## Tests to Fix
- `tests/bigquery/functions/math.rs:602` - `test_ieee_divide`
- `tests/bigquery/functions/math.rs:614` - `test_ieee_divide_by_zero`

## Files to Modify
- `src/executor/functions/math.rs` (add function implementation)
- `src/planner/functions/` (if function registration needed)

## Implementation Notes
- `IEEE_DIVIDE(x, y)` returns IEEE 754 compliant results
- Division by zero returns `+Infinity` or `-Infinity` (not error)
- `0/0` returns `NaN`
- Different from regular division which may error on divide by zero
- Follow BigQuery's IEEE_DIVIDE semantics

## Verification
```bash
cargo nextest run test_ieee_divide
```

## Dependencies
None - independent of other workers
