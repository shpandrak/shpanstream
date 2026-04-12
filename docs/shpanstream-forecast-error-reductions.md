# Proposal: Forecast Error Reduction Types for shpanstream

## Motivation

We're building forecast accuracy reports (PV forecast vs actual, load forecast vs actual) and need standardized error metrics on scorecards. Today we can approximate some of these by chaining computed fields + single-field reductions, but it's verbose, error-prone, and some metrics (correlation, R²) simply can't be expressed that way.

### Current workaround (MAE example)

```
Chart computed fields:
  error        = actual - predicted           [hidden]
  absError     = abs(error)                   [hidden]
  squaredError = error * error                [hidden]

Scorecard aggregation:
  avgAbsError     = avg(absError)             → MAE
  avgSquaredError = avg(squaredError)
  rmse            = sqrt(avgSquaredError)      → RMSE (expression layer)
```

This works but has downsides:
- 3 hidden computed fields pollute the chart query
- Each metric requires a multi-step manual wiring
- Correlation and R² cannot be decomposed into single-field reductions at all
- No streaming efficiency — intermediate fields are materialized

## Proposed: Paired Reduction Types

A new category of reduction that operates on **two fields** simultaneously (predicted vs actual). These are standard forecast verification metrics used across energy, weather, and ML domains.

### Core error metrics

| Reduction | Formula | What it tells you |
|-----------|---------|-------------------|
| `mae` | `mean(abs(actual - predicted))` | Average error magnitude (in original units, e.g. Wh) |
| `rmse` | `sqrt(mean((actual - predicted)²))` | Error magnitude, penalizes large errors more |
| `mbe` | `mean(actual - predicted)` | Systematic bias — positive = under-predicting, negative = over-predicting |
| `mape` | `mean(abs((actual - predicted) / actual)) * 100` | Percentage error — scale-independent, useful for comparing across sites |

### Correlation / fit metrics

| Reduction | Formula | What it tells you |
|-----------|---------|-------------------|
| `pearson` | Pearson correlation coefficient | Does the forecast track the shape of actuals? (1.0 = perfect tracking) |
| `r_squared` | Coefficient of determination | How much variance in actuals is explained by the forecast? |

### Suggested API shape

```go
// New reduction category: paired/comparative
type PairedReductionType string

const (
    PairedReductionMAE      PairedReductionType = "mae"
    PairedReductionRMSE     PairedReductionType = "rmse"
    PairedReductionMBE      PairedReductionType = "mbe"
    PairedReductionMAPE     PairedReductionType = "mape"
    PairedReductionPearson  PairedReductionType = "pearson"
    PairedReductionRSquared PairedReductionType = "r_squared"
)
```

These could live as:
- A new `PairedReductionAggregation` type in the aggregation layer (takes two source URNs)
- Or as a new aggregation field value type: `ApiPairedReductionAggregationFieldValue { actual: urn, predicted: urn, reductionType: ... }`

### Streaming implementation

All of these can be computed in O(1) space with streaming accumulators (same pattern as existing Welford-based stddev/variance):

- **MAE/MBE**: running sum of errors + count
- **RMSE**: running sum of squared errors + count
- **MAPE**: running sum of percentage errors + count (skip when actual=0)
- **Pearson/R²**: running sums of x, y, x², y², xy + count (textbook online formula)

### Priority

From our immediate needs:
1. **MAE + RMSE** — highest value, most universally understood error metrics
2. **MBE** — quick win alongside MAE (same accumulator, just skip the abs)
3. **Pearson / R²** — requires paired streaming, more architectural work
4. **MAPE** — useful but has the division-by-zero edge case (actual=0) that needs a policy decision (skip? clamp?)

## Bonus: Single-field enhancements (lower priority)

While you're in this area, a few single-field reductions from ENHANCEMENTS.md that would also help:

| Reduction | Use case |
|-----------|----------|
| `ema` (exponential moving average) | Smoothed trend lines on charts |
| `cumsum` (cumulative sum) | Running totals (e.g., cumulative energy over a day) |
| `clamp(min, max)` | Bounding values for display (e.g., cap accuracy % at 200%) |

These are independent of the paired reduction work and can be done separately.
