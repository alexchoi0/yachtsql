# Worker 2: Hyperbolic & Additional Math Functions

## Objective
Implement missing math functions to remove `#[ignore]` tags from `tests/postgresql/functions/trigonometric.rs`.

## Test File
- `tests/postgresql/functions/trigonometric.rs` (26 ignored tests)

## Features to Implement

### 1. Hyperbolic Functions
- `SINH(x)` - Hyperbolic sine
- `COSH(x)` - Hyperbolic cosine
- `TANH(x)` - Hyperbolic tangent
- `ASINH(x)` - Inverse hyperbolic sine
- `ACOSH(x)` - Inverse hyperbolic cosine
- `ATANH(x)` - Inverse hyperbolic tangent

### 2. Degree-Based Trigonometric Functions
- `SIND(degrees)` - Sine of angle in degrees
- `COSD(degrees)` - Cosine of angle in degrees
- `TAND(degrees)` - Tangent of angle in degrees
- `ASIND(x)` - Arc sine returning degrees
- `ACOSD(x)` - Arc cosine returning degrees
- `ATAND(x)` - Arc tangent returning degrees
- `ATAN2D(y, x)` - Two-argument arc tangent in degrees
- `COTD(degrees)` - Cotangent of angle in degrees
- `COT(radians)` - Cotangent

### 3. Additional Math Functions
- `LOG(x)` - Base-10 logarithm (single argument)
- `CBRT(x)` - Cube root
- `FACTORIAL(n)` - Factorial
- `GCD(a, b)` - Greatest common divisor
- `LCM(a, b)` - Least common multiple
- `DIV(a, b)` - Integer division

### 4. Numeric Precision Functions
- `MIN_SCALE(numeric)` - Minimum scale needed
- `SCALE(numeric)` - Current scale
- `TRIM_SCALE(numeric)` - Remove trailing zeros
- `WIDTH_BUCKET(value, low, high, buckets)` - Histogram bucket

### 5. Random Functions
- `SETSEED(seed)` - Set random seed

## Implementation Steps

1. **Register Functions**
   - Add all function names to function registry
   - Map to appropriate implementations

2. **Implement Hyperbolic Functions**
   - Use Rust's `f64::sinh()`, `cosh()`, `tanh()`, `asinh()`, `acosh()`, `atanh()`

3. **Implement Degree Functions**
   - Convert degrees to radians: `radians = degrees * PI / 180`
   - Call standard trig functions
   - Convert result back for inverse functions

4. **Implement Math Functions**
   - `CBRT`: Use `f64::cbrt()`
   - `FACTORIAL`: Iterative or lookup table for small n
   - `GCD`/`LCM`: Euclidean algorithm
   - `DIV`: Integer division with truncation toward zero

5. **Implement Numeric Functions**
   - Work with decimal/numeric type internals
   - `SCALE`: Return decimal places
   - `TRIM_SCALE`: Remove trailing zeros
   - `WIDTH_BUCKET`: Calculate histogram bucket index

## Key Files to Modify
- `crates/executor/src/query_executor/evaluator/physical_plan/expression/` - Function implementations
- Function registry files

## Testing
```bash
cargo test --test postgresql functions::trigonometric
```
