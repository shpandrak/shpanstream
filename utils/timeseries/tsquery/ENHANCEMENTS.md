# TSQuery Gap Analysis: Features Worth Adding

## Context

The tsquery framework is a composable, type-safe time-series query system with a rich filter pipeline model. It already covers: alignment/bucketing (calendar + custom), fill modes (linear, forward), reductions (sum/avg/min/max/count), delta & rate with counter support, full arithmetic + unary math, comparisons, logical ops, selector/NVL/cast, timestamp extraction, multi-datasource reduction, report joins (inner/left/full), field projection/drop/append/replace, condition filtering, and a plugin system for extensions.

This analysis compares against PromQL, Flux (InfluxDB), TimescaleDB, Graphite, OpenTSDB, and M3Query to identify gaps worth closing.

---

## Part 1: Features That Fit Our Model Naturally

These map cleanly to our composable filter/reduction architecture.

### A. Sliding Window Functions (HIGH priority, MODERATE effort)

**What:** Moving average, moving sum, moving min/max over a time-based sliding window. Supported by 5/5 frameworks. PromQL has `avg_over_time`, `max_over_time`, etc. Graphite has `movingAverage()`, Flux has `movingAverage(n:)`.

**Why it matters:** Smoothing noisy data, spike detection, and composing with rate/delta are among the most common time-series operations. Without this, users must pre-aggregate externally.

**How it fits:** A `SlidingWindowFilter` parameterized by (windowDuration, reductionType). Our `stream.Window()` is count-based; we need a time-duration variant. The reduction functions already exist in `reductions.go`. One new primitive unlocks: moving avg/sum/min/max/median, windowed stddev, windowed percentile, subqueries, predict_linear, `changes()`/`resets()` counting.

**Files:** New filter in `datasource/`, new stream primitive in `timeseries/`, new OpenAPI types in `queryopenapi/`.

---

### B. Additional Reduction Types (HIGH priority, LOW effort)

**What:** Extend `ReductionType` with:
- `first` / `last` - first or last value in bucket (4/5 frameworks)
- `spread` - max minus min (Flux `spread()`)
- `stddev` / `variance` - standard deviation and variance (5/5 frameworks)
- `percentile(phi)` - parameterized quantile (5/5 frameworks, e.g. PromQL `quantile`, OpenTSDB `p95`)

**Why it matters:** stddev and percentile are standard SLI/SLO metrics. first/last are essential for downsampling discrete events. Spread is a one-liner convenience.

**How it fits:** Direct additions to the existing `ReductionType` enum and `GetReducerFunc` in `reductions.go`. Percentile needs a parameter (the phi value), making it the only non-trivial one.

**Files:** `tsquery/reductions.go`, `queryopenapi/openapi_parser_filter.go`, `queryopenapi/tsquery-swagger.yaml`.

---

### C. Additional Fill Modes (HIGH priority, VERY LOW effort)

**What:**
- `zero` fill - replace gaps with 0 (5/5 frameworks)
- `null` fill - emit explicit null for gaps (OpenTSDB, Flux)
- `constant(value)` fill - user-specified fill value (Flux, Graphite)

**Why it matters:** Linear and forward fill don't cover all use cases. Counter-derived data often needs zero fill. Explicit null fill is needed for "I want to see the gap" UIs.

**How it fits:** New `FillMode` enum values in the existing gap-filler stream infrastructure. The plumbing already exists; just new enum cases.

**Files:** `timeseries/gap_filler.go` (or equivalent), `queryopenapi/openapi_parser_filter.go`, swagger.

---

### D. Time Shift (HIGH priority, LOW effort)

**What:** Shift timestamps by a duration. PromQL `offset`, Graphite `timeShift()`, Flux `timeShift()`. Supported by 4/5 frameworks.

**Why it matters:** Week-over-week and day-over-day comparisons are fundamental monitoring patterns. `rate(x[5m]) / rate(x[5m] offset 1w)` is a canonical query. Without time shift, users can't do historical comparison within the query layer.

**How it fits:** A `TimeShiftDatasource` wrapper that adjusts `from`/`to` before delegating, then maps output timestamps back. Composes naturally with report joins for side-by-side comparison.

**Files:** New datasource wrapper in `datasource/`, new OpenAPI type.

---

### E. Clamp (MEDIUM priority, VERY LOW effort)

**What:** `clamp(min, max)`, `clamp_min(min)`, `clamp_max(max)` - bound values to a range. PromQL and others.

**Why it matters:** Sanitizing values (CPU 0-100%, no negative rates). Can be expressed today with nested selectors, but a dedicated operator is far more ergonomic.

**How it fits:** New `UnaryNumericOperatorType` values (or a small parameterized filter). Trivial implementation.

**Files:** `queryopenapi/openapi_parser_field.go`, swagger.

---

### F. Cumulative Sum (MEDIUM priority, LOW effort)

**What:** Running total of all values seen so far. Flux `cumulativeSum()`, Graphite `integral()`.

**Why it matters:** Useful for "total requests since start of window" and similar accumulation patterns.

**How it fits:** A stateful filter identical in pattern to `DeltaFilter` - maintains an accumulator, emits running sum.

**Files:** New filter in `datasource/`.

---

### G. Limit / Offset on Streams (MEDIUM priority, VERY LOW effort)

**What:** Restrict output to first/last N records. Flux `limit(n:, offset:)`, `tail(n:)`.

**How it fits:** Counter-based stream filter. Trivial.

---

### H. Absent Detection (MEDIUM priority, LOW effort)

**What:** Returns a synthetic value (e.g. 1 or true) when a datasource produces no data. PromQL `absent()`.

**Why it matters:** Critical for alerting: "alert when this metric disappears."

**How it fits:** A datasource wrapper that checks if the inner stream is empty. If empty, emit a single synthetic record. Clean and composable.

---

### I. Configurable Alignment Aggregation (MEDIUM priority, MODERATE effort)

**What:** When aligning/bucketing, choose the aggregation method (sum, avg, min, max, first, last, percentile) per bucket instead of always using time-weighted average.

**Why it matters:** OpenTSDB, Flux, Graphite, and TimescaleDB all support this. Different use cases need different bucket aggregations - e.g., sum for request counts, max for peak detection.

**How it fits:** Extend `AlignerFilter` to accept an optional `ReductionType`. Falls back to current behavior (time-weighted avg) when not specified.

**Files:** Alignment filter implementation, swagger.

---

### J. EMA / Exponential Moving Average (LOW priority, LOW effort)

**What:** Exponentially weighted moving average. Flux `exponentialMovingAverage()`.

**How it fits:** Stateful filter with one float64 accumulator. Formula: `ema = alpha * current + (1-alpha) * prev_ema`.

---

### K. Power Operator (`^`) (LOW priority, VERY LOW effort)

**What:** Exponentiation binary operator. `math.Pow(a, b)`.

**How it fits:** New `BinaryNumericOperatorType`. One-liner.

---

### L. Sign Function (`sgn`) (LOW priority, VERY LOW effort)

**What:** Returns -1, 0, or 1 based on value sign. PromQL `sgn()`.

**How it fits:** New `UnaryNumericOperatorType`. One-liner.

---

### M. Regex Matching for Conditions (LOW priority, LOW effort)

**What:** `=~` and `!~` operators for string fields. PromQL uses this everywhere.

**How it fits:** New condition operator types alongside existing eq/neq/gt/lt. Apply `regexp.MatchString` to string field values.

---

### N. State Tracking (LOW priority, MODERATE effort)

**What:** Track consecutive duration or count in a given state. Flux `stateCount()`, `stateDuration()`.

**How it fits:** Stateful filter with a predicate condition. Resets counter on state change.

---

## Part 2: Features That Don't Fit Our Model Well

These are worth understanding as model exercises.

### X1. GroupBy / Label-Based Aggregation

**What it does:** Groups multiple time series by a tag/label dimension and aggregates per group. Fundamental to PromQL (`sum by (instance)`), OpenTSDB, Flux, Graphite.

**Why it doesn't fit:** Our model operates on explicitly named datasources, not on a labeled metric namespace. We don't have a "select all metrics matching X, then group by label Y" concept. Our `MultiDatasource` is a static list, not a dynamic query over a label space.

**Should we change the model?** This is the biggest philosophical question. Label-based selection + groupBy is the dominant pattern in monitoring systems. However, our model has a different strength: explicit, predictable query construction where the caller knows exactly which datasources they're combining. Adding dynamic label-based selection would mean adding a query-over-metadata layer, which is a significant architectural expansion. **Verdict: Not worth adding unless we move into general-purpose metric storage. Our model's explicitness is a feature, not a bug, for our use case.**

### X2. Histogram Data Model

**What it does:** PromQL `histogram_quantile()` operates on pre-bucketed histogram data (series with `le` bucket boundaries). This requires a special data model where related series (buckets, sum, count) are treated as a unit.

**Why it doesn't fit:** Our data types are scalar (integer, decimal, string, boolean, timestamp). A histogram is a composite structure. Supporting it would require either a new composite data type or conventions around multi-field reports where fields represent bucket boundaries.

**Should we change the model?** The report model (multi-field per timestamp) could represent histograms if we add a `histogram_quantile` report field value that reads bucket boundary fields. This is doable but niche. **Verdict: Only add if we have concrete histogram data to query. Can be built as a plugin without core model changes.**

### X3. TopK / BottomK Across Dynamic Series

**What it does:** PromQL `topk(5, rate(http_requests_total[5m]))` - select the 5 series with highest values out of potentially thousands.

**Why it doesn't fit:** Requires evaluating ALL series to rank them, then selecting a subset. In our model, the caller explicitly lists datasources. There's no "evaluate all, keep top 5" pattern.

**Fits partially:** Could work as a multi-datasource filter that evaluates all, sorts by a point-in-time or aggregate value, and yields only top/bottom K. This fits within `FilteredMultiDatasource`. **Verdict: Moderate fit. Worth adding if we have use cases with many datasources.**

### X4. Holt-Winters / Seasonal Forecasting

**What it does:** Triple exponential smoothing accounting for level, trend, and seasonality. Graphite `holtWintersForecast()`.

**Why it doesn't fit:** Requires a bootstrap period (typically one full seasonal cycle, e.g., one week) of historical data before producing meaningful output. This is at odds with the streaming filter model where each record is processed independently or with a small window.

**Should we change the model?** No. Forecasting is better handled by dedicated ML/analytics systems. Simple `predict_linear` (linear regression over a window) fits fine as a sliding window filter and covers 90% of use cases. **Verdict: Implement predict_linear, skip Holt-Winters.**

### X5. Cross-Series Correlation

**What it does:** Flux `pearsonr()` - compute correlation coefficient between two series over time.

**Why it doesn't fit partially:** It's a two-input operation producing a scalar or rolling series. Our filters are single-stream transformations. This would need a specialized join-then-compute pattern.

**Fits via reports:** Two series joined in a report, then a `CorrelationReportFieldValue` that reads both fields and computes rolling correlation. This is doable within the report model. **Verdict: Moderate fit. Add when needed, via report field values.**

---

## Part 3: Recommended Implementation Order

### Wave 1: Quick Wins (each 1-2 days)
1. **Additional fill modes** (zero, null, constant) - extends existing infra
2. **Additional reductions** (first, last, spread, stddev, variance) - extends existing enum
3. **Clamp** (min/max bounding) - new unary operators
4. **Power operator** + **sign function** - trivial additions
5. **Time shift** datasource wrapper

### Wave 2: High-Value Features (each 3-5 days)
6. **Sliding window filter** - the big one, unlocks moving avg/sum/min/max
7. **Percentile reduction** (parameterized) - needs phi parameter
8. **Configurable alignment aggregation** - extend AlignerFilter
9. **Cumulative sum** filter

### Wave 3: Nice-to-Haves
10. **Absent detection** wrapper
11. **EMA** filter
12. **Regex condition operators**
13. **Limit/offset** stream filter
14. **Predict linear** (depends on sliding window)
15. **State tracking** filter

---

## Verification

For each added feature:
- Unit tests following existing patterns (see `e2e_delta_filter_test.go`, `e2e_reduction_datasource_test.go` for style)
- OpenAPI spec update in `tsquery-swagger.yaml`
- Parser update in `queryopenapi/openapi_parser_*.go`
- Regenerate with `generate-openapi.sh`
- Run `go test ./...` in `utils/timeseries/tsquery/...`
