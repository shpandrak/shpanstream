# Proposal: `includePartial` support in ReductionDatasource

## Problem

`ReductionDatasource` combines multiple datasource streams using `InnerJoinStreams` (`reduction_datasource.go:188`):

```go
outputDataStream = timeseries.InnerJoinStreams(streams, reducerFunc)
```

This means **if any datasource has no data at timestamp T, the entire reduced row at T is dropped** — even when other datasources have valid data.

### When this breaks

Consider 3 datasources being summed:

```
Source A:  [100, 100, 100, 100, 100, 100]    timestamps: T1–T6
Source B:  [50,  50,  50,  ---,  ---,  50]    timestamps: T1–T3, T6 (gap at T4–T5)
Source C:  [30,  30,  30,  30,  30,  30]      timestamps: T1–T6
```

**Expected Sum:** `[180, 180, 180, 130, 130, 180]` — at T4 and T5, Source B is missing but A+C still produce a valid sum.

**Actual (InnerJoin):** `[180, 180, 180, ___, ___, 180]` — T4 and T5 are **dropped** because Source B has no data. The consumer sees a gap.

### Real-world scenarios

This is not an edge case. It manifests whenever:
- Equipment goes offline temporarily (maintenance, power outage, communication failure)
- Different data sources report at different intervals or schedules
- One source starts or stops before/after others
- Time series have natural gaps (e.g., sensors that power down at night)

In all these cases, the reduction of _available_ data is more useful than no data at all.

## Proposed solution

Add an `includePartial` option to `ReductionDatasource` and `ApiReductionQueryDatasource`. When enabled, timestamps where only some datasources have data still produce a reduced result from the available values.

### Go API

```go
// Builder method on ReductionDatasource
func (r *ReductionDatasource) WithIncludePartial() *ReductionDatasource
```

Usage:
```go
// Default (unchanged — requires all datasources):
ds := datasource.NewReductionDatasource(Sum, aligner, multiDs, meta)

// Opt-in partial (produces results from available datasources):
ds := datasource.NewReductionDatasource(Sum, aligner, multiDs, meta).WithIncludePartial()
```

### OpenAPI (tsquery-swagger.yaml)

Add `includePartial` to `ApiReductionQueryDatasource` (new field only — existing fields unchanged):

```yaml
    # Add to ApiReductionQueryDatasource.properties:
    includePartial:
      type: boolean
      description: |
        When true, timestamps where only some datasources have data still produce
        a reduced result from the available values. Missing datasources are skipped
        (not treated as a gap that drops the row).
        When false (default), a reduced row is only produced when ALL datasources
        have data at that timestamp (inner-join semantics).
```

### Implementation

**1. Add `includePartial` field to `ReductionDatasource`:**

```go
type ReductionDatasource struct {
    reductionType        tsquery.ReductionType
    alignerFilter        *AlignerFilter
    multiDataSource      MultiDataSource
    addFieldMeta         tsquery.AddFieldMeta
    emptyDatasourceValue Value
    includePartial       bool  // NEW
}

func (r *ReductionDatasource) WithIncludePartial() *ReductionDatasource {
    r.includePartial = true
    return r
}
```

**2. Branch in `Execute()` based on `includePartial`:**

The existing single-datasource identity optimization takes precedence — partial join of one stream is always identity, so no special handling needed there.

```go
// reducerFunc already obtained via r.reductionType.GetReducerFunc(dataType)

if len(streams) == 1 && r.reductionType.UseIdentityWhenSingleValue() {
    outputDataStream = streams[0]
} else if r.includePartial {
    // Wrap the existing reducer to skip nil values from FullJoinStreams.
    // FullJoinStreams[any, any] passes []*any to the joiner — nil where a
    // datasource has no data at that timestamp.
    partialReducer := wrapReducerForPartialJoin(reducerFunc)
    outputDataStream = timeseries.FullJoinStreams(streams, partialReducer)
} else {
    outputDataStream = timeseries.InnerJoinStreams(streams, reducerFunc)
}
```

**3. `wrapReducerForPartialJoin` — nil-aware wrapper around existing reducers:**

Reuses the already-computed `reducerFunc` (no double call to `GetReducerFunc`). The `[]*any` type comes from `FullJoinStreams[any, any]`'s generic instantiation — each element is nil when that datasource has no data at the timestamp.

```go
func wrapReducerForPartialJoin(innerReducer func([]any) any) func([]*any) any {
    return func(values []*any) any {
        nonNil := make([]any, 0, len(values))
        for _, v := range values {
            if v != nil {
                nonNil = append(nonNil, *v)
            }
        }
        if len(nonNil) == 0 {
            return nil
        }
        return innerReducer(nonNil)
    }
}
```

**4. Wire in the OpenAPI parser** (`openapi_parser_datasource.go`):

When parsing `ApiReductionQueryDatasource`, if `includePartial` is true, call `.WithIncludePartial()` on the constructed `ReductionDatasource`.

**Note:** `WithIncludePartial` uses a pointer receiver (builder pattern on the `*ReductionDatasource` returned by `New...`). The existing `Execute` method keeps its value receiver, consistent with `var _ DataSource = ReductionDatasource{}`.

## Why this is correct for all reduction types

The wrapper **filters out nil values** (datasources with no data at that timestamp) and passes the remaining values to the existing, unchanged reducer:

| Reduction | With `includePartial` | Correct? |
|-----------|----------------------|----------|
| **Sum** | Sum of available values | Yes — missing source contributes nothing |
| **Avg** | Average of available values (denominator = present count) | Yes — more accurate than including artificial zeros |
| **Min/Max** | Min/max of available values | Yes — absent source can't affect the extremes |
| **Count** | Count of present values | Yes — counts what actually reported |
| **First/Last** | From available values | Yes — picks from what's present |
| **Stddev/Variance** | Over available values | Yes — statistics of actual observations |
| **Spread** | Max-min of available values | Yes |
| **Percentiles** | Of available values | Yes |

Key properties:
- **No new reducer logic** — all existing reducers are reused unchanged
- **Default unchanged** — inner join remains the default; `includePartial` is opt-in
- **`FullJoinStreams` already exists** — no new stream infrastructure needed
- **Edge case handled** — if ALL streams are nil at a timestamp, returns nil (shouldn't happen with FullJoin since at least one stream triggered the row)

## Why not alternative approaches

**Why not change InnerJoin to FullJoin globally?**
InnerJoin is the safe conservative default — "only produce results when all data is present." Some consumers may depend on this guarantee. Making it opt-in preserves backward compatibility.

**Why not use `emptyDatasourceValue` for this?**
`emptyDatasourceValue` handles a different case: when `MultiDataSource` yields **zero datasources** (no sources match the query). It does not help when sources exist but have temporal gaps. These are orthogonal scenarios.

**Why not fill gaps in individual streams before the join?**
Gap-filling (interpolation, zero-fill) requires knowing the semantic meaning of the data. Filling a power metric with zeros may be correct; filling a temperature metric with zeros is wrong. `includePartial` is semantics-agnostic — it skips missing values without inventing data.
