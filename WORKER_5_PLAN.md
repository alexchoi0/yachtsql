# Worker 5: Cube Root Function

## Scope
Implement the `CBRT` (cube root) function

## Tests to Fix
- `tests/bigquery/functions/math.rs:537` - `test_cbrt`
- `tests/bigquery/functions/math.rs:547` - `test_cbrt_negative`

## Files to Modify
- `src/executor/functions/math.rs` (add function implementation)
- `src/planner/functions/` (if function registration needed)

## Implementation Notes
- Use Rust's `f64::cbrt()` method
- Unlike square root, cube root is defined for negative numbers
- `CBRT(-8) = -2`
- Follow existing math function patterns in the codebase

## Verification
```bash
cargo nextest run test_cbrt
```

## Dependencies
None - independent of other workers
