# Worker 4: Hyperbolic Reciprocal Trigonometric Functions

## Scope
Implement hyperbolic reciprocal trigonometric functions: `COTH`, `CSCH`, `SECH`

## Tests to Fix
- `tests/bigquery/functions/math.rs:361` - `test_coth`
- `tests/bigquery/functions/math.rs:381` - `test_csch`
- `tests/bigquery/functions/math.rs:401` - `test_sech`

## Files to Modify
- `src/executor/functions/math.rs` (add function implementations)
- `src/planner/functions/` (if function registration needed)

## Implementation Notes
- `COTH(x) = 1 / TANH(x) = COSH(x) / SINH(x)`
- `CSCH(x) = 1 / SINH(x)`
- `SECH(x) = 1 / COSH(x)`
- Handle division by zero cases (COTH and CSCH undefined at x=0)
- Follow existing math function patterns in the codebase

## Verification
```bash
cargo nextest run test_coth test_csch test_sech
```

## Dependencies
None - independent of other workers
