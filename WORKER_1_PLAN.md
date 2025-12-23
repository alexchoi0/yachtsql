# Worker 1: Hyperbolic Trigonometric Functions (Basic)

## Scope
Implement basic hyperbolic trigonometric functions: `SINH`, `COSH`, `TANH`

## Tests to Fix
- `tests/bigquery/functions/math.rs:291` - `test_sinh`
- `tests/bigquery/functions/math.rs:301` - `test_cosh`
- `tests/bigquery/functions/math.rs:311` - `test_tanh`

## Files to Modify
- `src/executor/functions/math.rs` (add function implementations)
- `src/planner/functions/` (if function registration needed)

## Implementation Notes
- Use Rust's `f64::sinh()`, `f64::cosh()`, `f64::tanh()` methods
- Follow existing math function patterns in the codebase
- Handle NULL inputs appropriately

## Verification
```bash
cargo nextest run test_sinh test_cosh test_tanh
```

## Dependencies
None - independent of other workers
