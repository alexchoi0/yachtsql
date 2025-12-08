# PLAN_15: ClickHouse Statistical Aggregate Functions (30 tests)

## Overview
Implement ClickHouse statistical aggregate functions for descriptive statistics and analysis.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/clickhouse/functions/aggregate_statistical.rs`

---

## Functions to Implement

### Basic Descriptive Statistics

| Function | Description |
|----------|-------------|
| `stddevPop(x)` | Population standard deviation |
| `stddevSamp(x)` | Sample standard deviation |
| `varPop(x)` | Population variance |
| `varSamp(x)` | Sample variance |
| `covarPop(x, y)` | Population covariance |
| `covarSamp(x, y)` | Sample covariance |
| `corr(x, y)` | Pearson correlation coefficient |

### Moments and Skewness

| Function | Description |
|----------|-------------|
| `skewPop(x)` | Population skewness |
| `skewSamp(x)` | Sample skewness |
| `kurtPop(x)` | Population kurtosis |
| `kurtSamp(x)` | Sample kurtosis |

### Quantiles

| Function | Description |
|----------|-------------|
| `quantile(level)(x)` | Quantile at level (0 to 1) |
| `quantiles(level1, level2, ...)(x)` | Multiple quantiles |
| `quantileExact(level)(x)` | Exact quantile |
| `quantileTDigest(level)(x)` | Approximate quantile (t-digest) |
| `quantileTiming(level)(x)` | Optimized for timing data |
| `quantileDeterministic(level)(x, determinator)` | Deterministic quantile |
| `median(x)` | Median (same as quantile(0.5)) |

### Mann-Whitney U Test

| Function | Description |
|----------|-------------|
| `mannWhitneyUTest(sample_data, sample_index)` | Mann-Whitney U test |

### Simple Statistics

| Function | Description |
|----------|-------------|
| `simpleLinearRegression(y, x)` | Returns (slope, intercept) |
| `stochasticLinearRegression(...)` | Stochastic gradient descent regression |
| `stochasticLogisticRegression(...)` | Stochastic logistic regression |

### Histogram

| Function | Description |
|----------|-------------|
| `histogram(number_of_bins)(x)` | Build histogram |

### Student's t-test

| Function | Description |
|----------|-------------|
| `studentTTest(sample1, sample2)` | Student's t-test |
| `welchTTest(sample1, sample2)` | Welch's t-test |

---

## Implementation Details

### Welford's Online Algorithm for Variance

```rust
#[derive(Debug, Clone, Default)]
pub struct WelfordAccumulator {
    count: usize,
    mean: f64,
    m2: f64,  // Sum of squares of differences from mean
}

impl WelfordAccumulator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, x: f64) {
        self.count += 1;
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        let delta2 = x - self.mean;
        self.m2 += delta * delta2;
    }

    pub fn variance_pop(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.m2 / self.count as f64
    }

    pub fn variance_samp(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        self.m2 / (self.count - 1) as f64
    }

    pub fn stddev_pop(&self) -> f64 {
        self.variance_pop().sqrt()
    }

    pub fn stddev_samp(&self) -> f64 {
        self.variance_samp().sqrt()
    }
}
```

### VarPop/VarSamp Accumulators

```rust
#[derive(Debug, Default)]
pub struct VarPopAccumulator {
    welford: WelfordAccumulator,
}

impl Accumulator for VarPopAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let x = value.as_f64()
            .ok_or_else(|| Error::type_mismatch("expected numeric"))?;
        self.welford.add(x);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.welford.count == 0 {
            return Ok(Value::null());
        }
        Ok(Value::float64(self.welford.variance_pop()))
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        // Parallel Welford merge algorithm
        if let Some(o) = other.as_any().downcast_ref::<VarPopAccumulator>() {
            if o.welford.count == 0 {
                return Ok(());
            }
            if self.welford.count == 0 {
                self.welford = o.welford.clone();
                return Ok(());
            }

            let n_a = self.welford.count as f64;
            let n_b = o.welford.count as f64;
            let n = n_a + n_b;

            let delta = o.welford.mean - self.welford.mean;
            let new_mean = self.welford.mean + delta * n_b / n;
            let new_m2 = self.welford.m2 + o.welford.m2 + delta * delta * n_a * n_b / n;

            self.welford.count = (n_a + n_b) as usize;
            self.welford.mean = new_mean;
            self.welford.m2 = new_m2;
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.welford = WelfordAccumulator::default();
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}
```

### Covariance Accumulator

```rust
#[derive(Debug, Clone, Default)]
pub struct CovarAccumulator {
    count: usize,
    mean_x: f64,
    mean_y: f64,
    co_moment: f64,  // Sum of (x - mean_x) * (y - mean_y)
}

impl CovarAccumulator {
    pub fn add(&mut self, x: f64, y: f64) {
        self.count += 1;
        let n = self.count as f64;

        let delta_x = x - self.mean_x;
        let delta_y = y - self.mean_y;

        self.mean_x += delta_x / n;
        self.mean_y += delta_y / n;

        // Update co-moment using the old mean for x
        self.co_moment += delta_x * (y - self.mean_y);
    }

    pub fn covar_pop(&self) -> f64 {
        if self.count == 0 {
            return 0.0;
        }
        self.co_moment / self.count as f64
    }

    pub fn covar_samp(&self) -> f64 {
        if self.count < 2 {
            return 0.0;
        }
        self.co_moment / (self.count - 1) as f64
    }
}
```

### Quantile Accumulator

```rust
#[derive(Debug, Default)]
pub struct QuantileExactAccumulator {
    values: Vec<f64>,
    level: f64,
}

impl QuantileExactAccumulator {
    pub fn new(level: f64) -> Self {
        Self {
            values: Vec::new(),
            level: level.clamp(0.0, 1.0),
        }
    }
}

impl Accumulator for QuantileExactAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        let x = value.as_f64()
            .ok_or_else(|| Error::type_mismatch("expected numeric"))?;
        self.values.push(x);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let mut sorted = self.values.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let n = sorted.len();
        let idx = (self.level * (n - 1) as f64).round() as usize;
        let idx = idx.min(n - 1);

        Ok(Value::float64(sorted[idx]))
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<QuantileExactAccumulator>() {
            self.values.extend(&o.values);
        }
        Ok(())
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}
```

### Skewness Accumulator

```rust
#[derive(Debug, Clone, Default)]
pub struct SkewnessAccumulator {
    count: usize,
    mean: f64,
    m2: f64,
    m3: f64,
}

impl SkewnessAccumulator {
    pub fn add(&mut self, x: f64) {
        let n1 = self.count as f64;
        self.count += 1;
        let n = self.count as f64;

        let delta = x - self.mean;
        let delta_n = delta / n;
        let delta_n2 = delta_n * delta_n;
        let term1 = delta * delta_n * n1;

        self.mean += delta_n;
        self.m3 += term1 * delta_n * (n - 2.0) - 3.0 * delta_n * self.m2;
        self.m2 += term1;
    }

    pub fn skewness_pop(&self) -> f64 {
        if self.count < 3 || self.m2 == 0.0 {
            return 0.0;
        }
        let n = self.count as f64;
        (n.sqrt() * self.m3) / (self.m2.powf(1.5))
    }

    pub fn skewness_samp(&self) -> f64 {
        if self.count < 3 || self.m2 == 0.0 {
            return 0.0;
        }
        let n = self.count as f64;
        let skew_pop = self.skewness_pop();
        skew_pop * ((n * (n - 1.0)).sqrt() / (n - 2.0))
    }
}
```

### Simple Linear Regression

```rust
#[derive(Debug, Default)]
pub struct SimpleLinearRegressionAccumulator {
    n: usize,
    sum_x: f64,
    sum_y: f64,
    sum_xx: f64,
    sum_xy: f64,
}

impl Accumulator for SimpleLinearRegressionAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        // Value is [y, x]
        if let Some(arr) = value.as_array() {
            if arr.len() == 2 && !arr[0].is_null() && !arr[1].is_null() {
                let y = arr[0].as_f64().ok_or_else(|| Error::type_mismatch("expected numeric"))?;
                let x = arr[1].as_f64().ok_or_else(|| Error::type_mismatch("expected numeric"))?;

                self.n += 1;
                self.sum_x += x;
                self.sum_y += y;
                self.sum_xx += x * x;
                self.sum_xy += x * y;
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.n < 2 {
            return Ok(Value::null());
        }

        let n = self.n as f64;
        let denom = n * self.sum_xx - self.sum_x * self.sum_x;

        if denom.abs() < f64::EPSILON {
            return Ok(Value::null());
        }

        let slope = (n * self.sum_xy - self.sum_x * self.sum_y) / denom;
        let intercept = (self.sum_y - slope * self.sum_x) / n;

        // Return as tuple (slope, intercept)
        Ok(Value::array(vec![
            Value::float64(slope),
            Value::float64(intercept),
        ]))
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<SimpleLinearRegressionAccumulator>() {
            self.n += o.n;
            self.sum_x += o.sum_x;
            self.sum_y += o.sum_y;
            self.sum_xx += o.sum_xx;
            self.sum_xy += o.sum_xy;
        }
        Ok(())
    }

    fn reset(&mut self) {
        *self = Self::default();
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}
```

---

## Key Files to Modify

1. **Functions:** `crates/functions/src/aggregate/clickhouse.rs`
   - Add statistical aggregates

2. **Registry:** `crates/functions/src/registry/clickhouse_aggregates.rs`
   - Register functions

3. **Helpers:** `crates/functions/src/aggregate/stats_helpers.rs` (new)
   - Reusable statistical algorithms

---

## Implementation Order

### Phase 1: Basic Statistics
1. `varPop`, `varSamp`
2. `stddevPop`, `stddevSamp`

### Phase 2: Covariance/Correlation
1. `covarPop`, `covarSamp`
2. `corr`

### Phase 3: Quantiles
1. `quantileExact`
2. `quantile` (reservoir sampling)
3. `median`

### Phase 4: Higher Moments
1. `skewPop`, `skewSamp`
2. `kurtPop`, `kurtSamp`

### Phase 5: Regression
1. `simpleLinearRegression`

---

## Testing Pattern

```rust
#[test]
fn test_stddev_pop() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE t (val Float64)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (2), (4), (4), (4), (5), (5), (7), (9)").unwrap();

    let result = executor.execute_sql("SELECT stddevPop(val) FROM t").unwrap();
    // stddev = 2.0
    let stddev = result.get_value(0, 0).as_f64().unwrap();
    assert!((stddev - 2.0).abs() < 0.001);
}

#[test]
fn test_covar_pop() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE t (x Float64, y Float64)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1, 2), (2, 4), (3, 6)").unwrap();

    let result = executor.execute_sql("SELECT covarPop(x, y) FROM t").unwrap();
    // Perfect positive correlation, covar = 4/3
    let covar = result.get_value(0, 0).as_f64().unwrap();
    assert!((covar - 4.0/3.0).abs() < 0.001);
}

#[test]
fn test_quantile() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE t (val Float64)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1), (2), (3), (4), (5)").unwrap();

    let result = executor.execute_sql("SELECT quantileExact(0.5)(val) FROM t").unwrap();
    assert_batch_eq!(result, [[3.0]]);
}

#[test]
fn test_simple_linear_regression() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE t (x Float64, y Float64)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1, 2), (2, 4), (3, 6)").unwrap();

    let result = executor.execute_sql("SELECT simpleLinearRegression(y, x) FROM t").unwrap();
    // y = 2x + 0, so slope=2, intercept=0
    let arr = result.get_value(0, 0).as_array().unwrap();
    let slope = arr[0].as_f64().unwrap();
    let intercept = arr[1].as_f64().unwrap();
    assert!((slope - 2.0).abs() < 0.001);
    assert!(intercept.abs() < 0.001);
}
```

---

## Verification Steps

1. Run: `cargo test --test clickhouse -- functions::aggregate_statistical --ignored`
2. Implement variance/stddev first
3. Add covariance/correlation
4. Add quantile functions
5. Remove `#[ignore = "Implement me!"]` as tests pass
