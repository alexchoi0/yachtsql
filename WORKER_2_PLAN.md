# Worker 2: Inverse Hyperbolic Trigonometric Functions

## Scope
Implement inverse hyperbolic trigonometric functions: `ASINH`, `ACOSH`, `ATANH`

## Tests to Fix
- `tests/bigquery/functions/math.rs:321` - `test_asinh`
- `tests/bigquery/functions/math.rs:331` - `test_acosh`
- `tests/bigquery/functions/math.rs:341` - `test_atanh`

## Files to Modify
- `src/executor/functions/math.rs` (add function implementations)
- `src/planner/functions/` (if function registration needed)

## Implementation Notes
- Use Rust's `f64::asinh()`, `f64::acosh()`, `f64::atanh()` methods
- `ACOSH` domain is [1, ∞) - handle domain errors
- `ATANH` domain is (-1, 1) - handle domain errors
- Follow existing math function patterns in the codebase

## Verification
```bash
cargo nextest run test_asinh test_acosh test_atanh
```

## Dependencies
None - independent of other workers
