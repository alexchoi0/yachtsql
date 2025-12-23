# Worker 3: Reciprocal Trigonometric Functions

## Scope
Implement reciprocal trigonometric functions: `COT`, `CSC`, `SEC`

## Tests to Fix
- `tests/bigquery/functions/math.rs:351` - `test_cot`
- `tests/bigquery/functions/math.rs:371` - `test_csc`
- `tests/bigquery/functions/math.rs:391` - `test_sec`

## Files to Modify
- `src/executor/functions/math.rs` (add function implementations)
- `src/planner/functions/` (if function registration needed)

## Implementation Notes
- `COT(x) = 1 / TAN(x) = COS(x) / SIN(x)`
- `CSC(x) = 1 / SIN(x)`
- `SEC(x) = 1 / COS(x)`
- Handle division by zero cases appropriately
- Follow existing math function patterns in the codebase

## Verification
```bash
cargo nextest run test_cot test_csc test_sec
```

## Dependencies
None - independent of other workers
