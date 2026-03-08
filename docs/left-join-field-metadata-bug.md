# Bug: Left/Full Join Does Not Mark Right-Side Fields as Optional

## Summary

`join_datasource.go` appends field metadata from all datasources without updating `Required` for outer join types. This causes panics when computed fields or condition filters reference right-side (nil-able) fields after a left or full join.

## Root Cause

`join_datasource.go:54` simply appends field metadata:

```go
joinedFieldMeta = append(joinedFieldMeta, currFieldsMeta...)
```

For `LeftJoin`, right-side datasource fields can be nil at runtime, but their metadata still says `Required=true`. Downstream code uses `Required` to decide whether to add nil-wrapping.

## Affected Downstream Consumers

- **NumericExpressionFieldValue** (`numeric_expression_report_field_value.go:83`): computes `Required: op1Meta.Required && op2Meta.Required` — remains `true` when both operands come from "Required" fields, so nil-wrapping at lines 88-96 is skipped. Raw arithmetic `v1.(float64) + v2.(float64)` panics when v1 or v2 is nil.

- **ConditionFilter** (`condition_report_filter.go:54`): `conditionValue.(bool)` panics on nil.

- **SelectorFieldValue** (`selector_report_field_value.go:105`): `selectorValue.(bool)` panics on nil.

- **LogicalExpressionFieldValue** (`logical_expressions.go:201-206`): `v1.(bool) && v2.(bool)` panics on nil.

- **Aligner filter interpolation** via `DataType.ToFloat64()` (`datatype.go:84-99`).

## Reproduction

The existing test `TestJoinDatasource_LeftJoin_ThreeStreams` already produces nil values in joined rows (verified at lines 172-175). Adding a `NumericExpressionFieldValue` (e.g., `cpuUsage + memoryUsed`) as an `AppendFieldFilter` after the left join would panic at Hour 0 where `memoryUsed` is nil.

## Suggested Fix

### Step 1: Add `WithRequired` method to `FieldMeta`

**File:** `time_series_query.go`

```go
func (fm FieldMeta) WithRequired(required bool) FieldMeta {
    fm.required = required
    return fm
}
```

### Step 2: Mark right-side fields as optional after join

**File:** `join_datasource.go`, after the loop building `joinedFieldMeta` (after line ~61):

```go
switch mds.joinType {
case LeftJoin:
    // Right-side fields can be nil — mark as optional
    leftFieldCount := idxToNumberOfExpectedFields[0]
    for i := leftFieldCount; i < len(joinedFieldMeta); i++ {
        joinedFieldMeta[i] = joinedFieldMeta[i].WithRequired(false)
    }
case FullJoin:
    // All fields can be nil — mark all as optional
    for i := range joinedFieldMeta {
        joinedFieldMeta[i] = joinedFieldMeta[i].WithRequired(false)
    }
}
```

### Step 3: Add test

Left join + `AppendFieldFilter` with `NumericExpressionFieldValue` referencing a right-side field. Verify it produces nil (not panic) when the right-side row is missing.

## Impact

- **EnergyRuntimeHub current usage:** Safe — builtin power/energy breakdown charts use left join but have no computed fields or post-join filters.
- **Future usage:** Any query combining left join with computed fields or condition filters will panic until this is fixed.
