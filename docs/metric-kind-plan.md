# MetricKind: Implementation Plan

## Motivation

Today, `FieldMeta` carries `DataType` (what shape values are) and `Unit` (what they measure).
But it doesn't carry **MetricKind** -- the temporal semantics of the values:

- **Gauge**: instantaneous measurement (temperature, CPU%, current power)
- **Delta**: change since last data point (energy consumed in interval, requests in interval)
- **Cumulative**: monotonically increasing total (lifetime energy, total request count)
- **Rate**: derived rate of change (power in kW, requests/second)

Users must manually configure filters knowing the kind of their data:
- Applying `DeltaFilter` before `AlignerFilter` for cumulative counters
- Choosing `Sum` bucket reduction for delta metrics vs `Avg` for gauges
- Setting `nonNegative: true` on DeltaFilter for cumulative counters

These are decisions the system can make automatically if metrics carry their kind.

### Example: The Cumulative-to-Aligned-Delta Problem

Today, to get 15-minute energy deltas from a cumulative lifetime energy counter:

```json
{
  "type": "filtered",
  "datasource": { "type": "static", "fieldMeta": { "uri": "lifetimeEnergy", "dataType": "decimal", "required": true, "unit": "kWh" }, "data": [...] },
  "filters": [
    { "type": "delta", "nonNegative": true },
    { "type": "aligner", "alignerPeriod": { "type": "custom", "durationInMillis": 900000, "zoneId": "UTC" }, "bucketReduction": "sum" }
  ]
}
```

The user must:
1. Know the data is cumulative (not visible in the query)
2. Know to apply delta first
3. Know to use `sum` reduction (not `avg` -- `avg` of deltas is meaningless for totals)

With MetricKind, the same query becomes self-describing. In Phase 2, the aligner auto-selects `sum` for delta data, so the user can omit `bucketReduction` entirely and get the correct result:

```json
{
  "type": "filtered",
  "datasource": { "type": "static", "fieldMeta": { "uri": "lifetimeEnergy", "dataType": "decimal", "required": true, "unit": "kWh", "metricKind": "cumulative" }, "data": [...] },
  "filters": [
    { "type": "delta", "nonNegative": true },
    { "type": "aligner", "alignerPeriod": { "type": "custom", "durationInMillis": 900000, "zoneId": "UTC" } }
  ]
}
```

What happens under the hood:
```
lifetimeEnergy (cumulative, kWh)
  --> DeltaFilter: input=cumulative, output kind=delta (values are now per-interval changes)
  --> AlignerFilter(15m, no explicit reduction):
        sees kind=delta --> auto-picks Sum
        result: 15-minute energy consumption per bucket
```

Compare with a gauge going through the same aligner:
```
temperature (gauge, celsius)
  --> AlignerFilter(15m, no explicit reduction):
        sees kind=gauge --> auto-picks time-weighted average (existing default behavior)
        result: average temperature per 15-minute bucket
```

## Design Decisions (Agreed)

### 1. MetricKind is a first-class field on `FieldMeta`

Like `DataType` and `Unit`, not stuffed into `CustomMeta`. Reasons:
- The filters that consume it (`DeltaFilter`, `RateFilter`, `AlignerFilter`) live in this library
- Type-safe enum, not stringly-typed convention
- Filters can rely on it being present (with a default)

### 2. Default is Gauge

The zero value (`""`) is treated as Gauge. The public getter `MetricKind()` always returns `Gauge` when unset. This means:
- All existing metrics are implicitly gauge (backward compatible)
- No "unspecified" concept exposed to users
- If you want kind-aware behavior, set the kind in your datasource

```go
type MetricKind string

const (
    MetricKindGauge      MetricKind = "gauge"
    MetricKindDelta      MetricKind = "delta"
    MetricKindCumulative MetricKind = "cumulative"
    MetricKindRate       MetricKind = "rate"
)

func (fm FieldMeta) MetricKind() MetricKind {
    if fm.metricKind == "" {
        return MetricKindGauge
    }
    return fm.metricKind
}
```

`MetricKindUnspecified` exists internally as the zero value but is not exported -- users see gauge, delta, cumulative, or rate. Rate behaves identically to gauge in all filter paths today but is semantically distinct and future-proofs the enum.

### 3. Where users declare MetricKind

On the **datasource's FieldMeta** -- the same place they declare `DataType` and `Unit`. The datasource is the authority on what the raw data represents.

- `StaticDatasource`: via `ApiQueryFieldMeta.metricKind` in JSON, or `FieldMeta.WithMetricKind()` in Go
- Plugin/external datasources: set when constructing `FieldMeta` in `Execute()`
- Programmatic use: via constructor or `WithMetricKind()` builder

### 4. Filter kind transformations

| Filter | Input Kind | Output Kind | Notes |
|--------|-----------|-------------|-------|
| DeltaFilter | Cumulative | Delta | Phase 2: error if input is Gauge, Rate, or Delta |
| RateFilter | Any numeric | Rate | A computed rate is a rate |
| AlignerFilter | Any | Preserved | Alignment changes granularity, not semantic meaning |
| OverrideFieldMetadataFilter | Any | User-specified | Escape hatch for incorrect datasource metadata |
| All other filters | Any | Preserved | Kind passes through unchanged |

### 5. AlignerFilter: explicit always wins

When `bucketReduction` is explicitly set by the user, the aligner uses it regardless of kind. Kind only influences the **default** when no reduction is specified:

| MetricKind | Default Bucket Reduction (when none specified) |
|------------|-----------------------------------------------|
| Gauge | Time-weighted average (existing behavior, unchanged) |
| Rate | Time-weighted average (same as gauge) |
| Delta | Sum |
| Cumulative | Last |

### 6. DeltaFilter validation (Phase 2)

- Input `Cumulative` --> valid, output `Delta`
- Input `Delta` --> error ("data is already delta, DeltaFilter is not needed")
- Input `Gauge` --> error ("DeltaFilter is for cumulative data; gauge metrics represent instantaneous values")
- Input `Rate` --> error ("DeltaFilter cannot be applied to rate metrics")
- These validations are added as commented-out code with TODO in Phase 1

### 7. No separate "smart aligner"

The existing `AlignerFilter` gains kind-awareness. No new filter type. The change is small -- just a kind-based switch in the default (no explicit `bucketReduction`) code path.

### 8. Separate releases for Phase 1 and Phase 2

Phase 1 and Phase 2 are shipped as **separate tagged releases**. Phase 1 is pure plumbing with zero behavior changes -- safe to upgrade immediately. Phase 2 introduces validation and smart defaults that require metrics to be correctly tagged. Customers control when they upgrade to Phase 2, after they have populated `metricKind` on their cumulative/delta datasources.

There is no transition flag or "unspecified" escape hatch. If a metric reaches DeltaFilter without an explicit `metricKind: "cumulative"` in Phase 2, it defaults to Gauge and errors. This is intentional -- it surfaces untagged metrics that need attention. The migration path:

1. Upgrade to Phase 1 release -- nothing breaks, no behavior changes
2. Tag cumulative/delta metrics with `metricKind` in datasources -- nothing breaks
3. Upgrade to Phase 2 release -- any untagged metrics hitting DeltaFilter will error with a clear message

### 9. Optional samplePeriod on FieldMeta

An optional `samplePeriod` (`*time.Duration`) on `FieldMeta` declares the expected reporting interval for a metric (e.g., "this delta reports every 5m"). This is orthogonal to MetricKind -- useful for all kinds, not just delta -- but particularly valuable for delta metrics where missing samples cause silent undercounting during alignment.

**How it works:**
- Datasources set `samplePeriod` via `WithSamplePeriod(5 * time.Minute)` in Go or `"samplePeriod": "5m"` in JSON
- All field values and filters propagate it (same pattern as MetricKind)
- RateFilter clears it (rate output has no meaningful sample period)
- Overridable via `OverrideFieldMetadataFilter` and `AddFieldMeta.overrideSamplePeriod`
- Phase 1: plumbing only, no behavior changes
- Phase 2: AlignerFilter uses `samplePeriod` + `MetricKind=Delta` for gap detection and proration

**Industry context:** OpenTelemetry carries explicit start/end timestamps per delta point. Prometheus extrapolates to cover gaps. Google Cloud Monitoring interpolates missing periods. Our approach is lighter -- metadata on the field rather than per-point timestamps -- but enables the same downstream decisions.

---

## Phase 1: Types, Plumbing, Propagation

**Goal**: Add MetricKind to the type system and data flow. No behavior changes. Customers can start populating kind in their datasources.

### 1.1 Add `MetricKind` type

**File**: `utils/timeseries/tsquery/metric_kind.go` (new)

```go
package tsquery

import "fmt"

type MetricKind string

const (
    MetricKindGauge      MetricKind = "gauge"
    MetricKindDelta      MetricKind = "delta"
    MetricKindCumulative MetricKind = "cumulative"
    MetricKindRate       MetricKind = "rate"
)

func (mk MetricKind) Validate() error {
    switch mk {
    case "", MetricKindGauge, MetricKindDelta, MetricKindCumulative, MetricKindRate:
        return nil
    }
    return fmt.Errorf("invalid metric kind: %q", mk)
}
```

### 1.2 Add `metricKind` field to `FieldMeta`

**File**: `utils/timeseries/tsquery/time_series_query.go`

- Add `metricKind MetricKind` field to `FieldMeta` struct
- Add `MetricKind()` getter (returns `MetricKindGauge` if empty)
- Add `WithMetricKind(kind MetricKind) FieldMeta` builder -- returns `FieldMeta` by value (not pointer), consistent with `WithRequired()`
- Add full constructor with `metricKind` positioned after `dataType` (both are fundamental classifiers):
  ```go
  func NewFieldMetaFull(urn string, dataType DataType, metricKind MetricKind, required bool, unit string, customMeta map[string]any) (*FieldMeta, error)
  ```
- Update `NewFieldMetaWithCustomData` to accept kind (or keep signature, set empty -- backward compat)
- Add `MetricKind` field to `ValueMeta` struct
- Add `MetricKind` field to `AddFieldMeta` struct
- Validate `MetricKind` in constructors

Decision on constructor backward compatibility:
- `NewFieldMeta(urn, dataType, required)` -- unchanged, kind defaults to gauge
- `NewFieldMetaWithCustomData(urn, dataType, required, unit, customMeta)` -- unchanged, kind defaults to gauge
- `NewFieldMetaFull(urn, dataType, metricKind, required, unit, customMeta)` -- new, explicit kind

### 1.3 Update OpenAPI spec

**File**: `utils/timeseries/tsquery/queryopenapi/tsquery-swagger.yaml`

Add `ApiMetricKind` enum:
```yaml
ApiMetricKind:
  type: string
  x-go-type: tsquery.MetricKind
  x-go-import: "github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
  enum:
    - gauge
    - delta
    - cumulative
    - rate
```

Add `metricKind` field to `ApiQueryFieldMeta`:
```yaml
metricKind:
  $ref: '#/components/schemas/ApiMetricKind'
```

Add `overrideMetricKind` field to `ApiOverrideFieldMetadataFilter`:
```yaml
overrideMetricKind:
  $ref: '#/components/schemas/ApiMetricKind'
```

### 1.4 Regenerate OpenAPI code

Follow the release skill pipeline:
1. Run `build.sh` from `utils/timeseries/tsquery/queryopenapi/cmd/tsquery-parser-codegen/`
2. Run `generate.sh` from `utils/timeseries/tsquery/queryopenapi/codegen/testdata/`
3. Run `generate-openapi.sh` from `utils/timeseries/tsquery/queryopenapi/`

### 1.5 Update OpenAPI parsers

**File**: `utils/timeseries/tsquery/queryopenapi/openapi_parser_datasource.go`

- `parseStaticDatasource`: pass `metricKind` from `ApiQueryFieldMeta` to `NewFieldMetaFull`
- `ParseAddFieldMeta`: include `MetricKind` in `AddFieldMeta`

**File**: `utils/timeseries/tsquery/queryopenapi/openapi_parser_filter.go`

- `parseOverrideFieldMetadataFilter`: handle `overrideMetricKind` field

### 1.6 Update filters to propagate kind

**DeltaFilter** (`delta_filter.go`):
- Output metadata: set kind to `MetricKindDelta`
- Add commented-out input validation with TODO:
```go
// TODO Phase 2: validate input kind
// inputKind := result.meta.MetricKind()
// if inputKind == tsquery.MetricKindGauge {
//     return ..., fmt.Errorf("delta filter cannot be applied to gauge metrics")
// }
// if inputKind == tsquery.MetricKindDelta {
//     return ..., fmt.Errorf("delta filter cannot be applied to data that is already delta")
// }
// if inputKind == tsquery.MetricKindRate {
//     return ..., fmt.Errorf("delta filter cannot be applied to rate metrics")
// }
```
- Currently preserves `result.meta` directly -- needs to create new meta with `MetricKindDelta`

**RateFilter** (`rate_filter.go`):
- Already creates new meta (line 50-56) -- add `MetricKindRate` to the new meta

**AlignerFilter** (`aligner_filter.go`):
- Preserves kind (no change to kind in output metadata)
- Add commented-out smart default with TODO:
```go
// TODO Phase 2: kind-aware default bucket reduction
// if af.bucketReduction == nil {
//     switch result.meta.MetricKind() {
//     case tsquery.MetricKindDelta:
//         // auto-select Sum for delta metrics
//     case tsquery.MetricKindCumulative:
//         // auto-select Last for cumulative metrics
//     case tsquery.MetricKindGauge, tsquery.MetricKindRate:
//         // existing time-weighted average behavior
//     }
// }
```

**OverrideFieldMetadataFilter** (`override_field_metadata_filter.go`):
- Add `optUpdatedMetricKind *MetricKind` parameter
- Apply override when present, preserve original when nil

**All other filters**: No changes needed -- they either preserve `result.meta` as-is (kind flows through) or create new meta via `NewFieldMetaWithCustomData` which defaults to gauge.

### 1.6b Update report-side metadata propagation

MetricKind must flow through the report (multi-field) path identically to the datasource (single-field) path.

**`ValueMeta`** (`time_series_query.go`):
- Add `MetricKind MetricKind` field -- this is the central carrier for report field values

**`RefFieldValue`** (`report/ref_report_field_value.go`):
- Extract `fm.MetricKind()` into `ValueMeta.MetricKind` (like it already does for DataType, Unit, Required, CustomMeta)

**`PrepareField()`** (`report/report_filter.go`):
- Pass `valueMeta.MetricKind` through when creating FieldMeta via `NewFieldMetaWithCustomData` (or use `WithMetricKind` on the result)
- Handle `AddFieldMeta.OverrideMetricKind` if present

**`parseStaticReportDatasource`** (`queryopenapi/openapi_parser_report_datasource.go`):
- Pass `metricKind` from `ApiQueryFieldMeta` when constructing FieldMeta

**Report `OverrideFieldMetadataFilter`** (`report/override_field_metadata_report_filter.go`):
- Add `optUpdatedMetricKind *MetricKind` -- same pattern as datasource-side override

**Report `OverrideFieldMetadataReportFilter` parser** (`queryopenapi/openapi_parser_report_filter.go`):
- Parse `overrideMetricKind` field

**Other report field values** (constant, cast, nvl, numeric_expression, unary, selector, etc.):
- Most propagate ValueMeta fields automatically -- once MetricKind is on ValueMeta, it flows through
- `constant_field_report_value.go`: defaults to gauge (no source metadata)
- `condition/logical_expression`: boolean output, kind is irrelevant (defaults to gauge)

**`StaticStructDatasource`** (`report/static_struct_report_datassource.go`):
- Uses `NewFieldMeta` which defaults kind to gauge -- no change needed

### 1.7 Update tests

- Add unit tests for `MetricKind.Validate()`
- Add test for `FieldMeta.MetricKind()` default behavior (empty -> gauge)
- Add test for `WithMetricKind()` builder
- Add test for `NewFieldMetaFull` constructor
- Test DeltaFilter sets output kind to Delta
- Test RateFilter sets output kind to Rate
- Test AlignerFilter preserves kind (including Rate)
- Test OverrideFieldMetadataFilter can override kind
- Test kind propagation through filter chains

### 1.8 Build and verify

- `go build ./...`
- `go test ./...`
- Run codegen testdata tests: `cd utils/timeseries/tsquery/queryopenapi/codegen/testdata/ && go test ./...`

---

## Phase 2: Kind-Aware Behavior

**Goal**: Filters use MetricKind to make smart decisions. This is where the ergonomic payoff lands.

### 2.1 AlignerFilter smart defaults

When `bucketReduction` is nil, choose default based on kind:

```go
func (af AlignerFilter) Filter(_ context.Context, result Result) (Result, error) {
    // ... existing validation ...

    if af.bucketReduction != nil {
        // Explicit reduction: use it (unchanged)
        // ... existing bucket reduction code path ...
    } else {
        switch result.meta.MetricKind() {
        case tsquery.MetricKindDelta:
            // Delta metrics: sum within bucket
            // Use the existing bucket reduction code path with ReductionTypeSum
        case tsquery.MetricKindCumulative:
            // Cumulative metrics: last value in bucket
            // Use the existing bucket reduction code path with ReductionTypeLast
        case tsquery.MetricKindGauge, tsquery.MetricKindRate:
            // Gauge/Rate: time-weighted interpolation (existing default, unchanged)
            // ... existing interpolation code path ...
        }
    }
}
```

### 2.2 DeltaFilter input validation

Uncomment the Phase 1 TODOs:
- Error on `MetricKindGauge` input
- Error on `MetricKindRate` input
- Error on `MetricKindDelta` input (already delta)
- Only accept `MetricKindCumulative` (and unspecified for backward compat -- but unspecified returns gauge, so this effectively requires explicit cumulative)

### 2.3 Selector field value strict kind validation

Selector field values (`if condition then trueField else falseField`) currently validate that the true and false branches have matching `DataType` and `Required` status (`selector_field_value.go:83-88`). Phase 2 extends this validation to require matching `MetricKind`:

- If `trueBranch.MetricKind != falseBranch.MetricKind` --> error at query-build time
- Error message: `"selector requires matching metric kinds: true=<X>, false=<Y>"`

**Why:** Selecting between a cumulative counter and a delta metric is almost always a programmer mistake. The resulting stream would have ambiguous semantics -- downstream filters like AlignerFilter couldn't pick a correct default reduction.

**Phase 1 behavior (unchanged):** Selector propagates `MetricKind` from the true branch. This is backward-compatible: existing untagged code keeps working because both branches default to Gauge.

**Phase 2 behavior (breaking for mis-tagged metrics):** Strict matching required. Customers upgrading to Phase 2 must ensure selectors over tagged metrics have matching kinds on both branches, or use `OverrideFieldMetadataFilter` to align kinds first.

Apply to both datasource-side (`selector_field_value.go`) and report-side (`selector_report_field_value.go`) selectors.

### 2.4 Test: cumulative-to-aligned-delta (the golden test case)

```go
func TestCumulativeToAlignedDelta(t *testing.T) {
    // Simulate a cumulative energy counter reporting every 5 minutes
    // Values: 100, 110, 125, 140, 155, 170, 190, 215 (over 40 minutes)
    //
    // DeltaFilter produces deltas: 10, 15, 15, 15, 15, 20, 25
    //   (kind changes from Cumulative to Delta)
    //
    // AlignerFilter(15m) with no explicit bucketReduction:
    //   Sees kind=Delta --> auto-selects Sum
    //   Bucket [0:00-0:15): delta 10 + 15 = 25
    //   Bucket [0:15-0:30): delta 15 + 15 = 30
    //   Bucket [0:30-0:45): delta 15 + 20 + 25 = 60 (if 3 samples in bucket)
    //
    // Without MetricKind, user would need explicit bucketReduction: "sum"
    // With MetricKind, it just works.

    fieldMeta, _ := tsquery.NewFieldMetaFull(
        "lifetimeEnergy",
        tsquery.DataTypeDecimal,
        tsquery.MetricKindCumulative,
        true,
        "kWh",
        nil,
    )
    // ... build datasource, apply DeltaFilter, apply AlignerFilter(15m) ...
    // ... assert output values are sums per bucket ...
    // ... assert output kind is Delta ...
}
```

### 2.5 Test: gauge through same aligner (contrast test)

```go
func TestGaugeAlignedDefaultAvg(t *testing.T) {
    // Temperature gauge at irregular intervals through a 15-min bucket
    // AlignerFilter(15m) with no explicit bucketReduction:
    //   Sees kind=Gauge --> time-weighted average (existing behavior)
    //
    // This test ensures gauge behavior is unchanged from pre-MetricKind.
}
```

### 2.6 Test: explicit bucketReduction overrides kind

```go
func TestExplicitReductionOverridesKind(t *testing.T) {
    // Delta metric + AlignerFilter with explicit bucketReduction="avg"
    // Even though kind=Delta suggests Sum, user said Avg --> use Avg
}
```

---

## Files Changed (Summary)

### Phase 1
| File | Change |
|------|--------|
| `utils/timeseries/tsquery/metric_kind.go` | **New** -- MetricKind type, constants, Validate() |
| `utils/timeseries/tsquery/time_series_query.go` | Add field, getter, builder, full constructor; add MetricKind to ValueMeta and AddFieldMeta |
| `utils/timeseries/tsquery/queryopenapi/tsquery-swagger.yaml` | Add ApiMetricKind enum, add to ApiQueryFieldMeta, ApiAddFieldMeta, and both ApiOverrideFieldMetadataFilters |
| `utils/timeseries/tsquery/queryopenapi/openapi_generated.gen.go` | Regenerated |
| `utils/timeseries/tsquery/queryopenapi/openapi_parser_datasource.go` | Parse metricKind from ApiQueryFieldMeta |
| `utils/timeseries/tsquery/queryopenapi/openapi_parser_filter.go` | Parse overrideMetricKind in override filter; pass MetricKind in ParseAddFieldMeta |
| `utils/timeseries/tsquery/queryopenapi/openapi_parser_report_datasource.go` | Parse metricKind for static report datasource |
| `utils/timeseries/tsquery/queryopenapi/openapi_parser_report_filter.go` | Parse overrideMetricKind in report override filter |
| `utils/timeseries/tsquery/datasource/delta_filter.go` | Set output kind=Delta, add commented-out validation |
| `utils/timeseries/tsquery/datasource/rate_filter.go` | Set output kind=Rate |
| `utils/timeseries/tsquery/datasource/aligner_filter.go` | Add commented-out smart defaults |
| `utils/timeseries/tsquery/datasource/override_field_metadata_filter.go` | Add kind override support |
| `utils/timeseries/tsquery/report/ref_report_field_value.go` | Extract MetricKind into ValueMeta |
| `utils/timeseries/tsquery/report/report_filter.go` | Pass MetricKind through PrepareField |
| `utils/timeseries/tsquery/report/override_field_metadata_report_filter.go` | Add kind override support |
| Test files | New tests for kind propagation |

### Phase 2
| File | Change |
|------|--------|
| `utils/timeseries/tsquery/datasource/aligner_filter.go` | Uncomment smart defaults, implement kind-aware path |
| `utils/timeseries/tsquery/datasource/delta_filter.go` | Uncomment input validation |
| `utils/timeseries/tsquery/datasource/selector_field_value.go` | Add strict MetricKind matching validation between true/false branches |
| `utils/timeseries/tsquery/report/selector_report_field_value.go` | Add strict MetricKind matching validation between true/false branches |
| Test files | Golden test cases (cumulative->aligned-delta, gauge contrast, explicit override, selector kind mismatch) |
