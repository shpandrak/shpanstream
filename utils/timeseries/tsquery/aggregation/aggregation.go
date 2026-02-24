package aggregation

import (
	"context"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"time"
)

// AggregatedValue holds a single aggregated scalar result.
// Metadata lives in Result.FieldsMeta() (parallel array, same index).
type AggregatedValue struct {
	Value     any
	Timestamp *time.Time // nil when reduction has no meaningful timestamp (sum, avg, count)
}

// Result contains aggregation output: eager metadata + lazy computed values.
// Mirrors the datasource.Result{meta, data} pattern.
type Result struct {
	fieldsMeta []tsquery.FieldMeta
	fields     lazy.Lazy[[]AggregatedValue]
}

// NewResult creates a new aggregation Result with eager metadata and lazy fields.
func NewResult(fieldsMeta []tsquery.FieldMeta, fields lazy.Lazy[[]AggregatedValue]) Result {
	return Result{fieldsMeta: fieldsMeta, fields: fields}
}

// FieldsMeta returns the metadata for each aggregated field (available immediately after Execute).
func (r Result) FieldsMeta() []tsquery.FieldMeta { return r.fieldsMeta }

// Fields returns a lazy handle to the aggregated values. Computation is deferred until Get() is called.
func (r Result) Fields() lazy.Lazy[[]AggregatedValue] { return r.fields }

// Aggregator is the common interface for all aggregation types.
type Aggregator interface {
	Execute(ctx context.Context, from time.Time, to time.Time) (Result, error)
}

// AggregationFieldDef defines one aggregation to compute.
// AddFieldMeta is optional — when nil, metadata is derived from the source field.
type AggregationFieldDef struct {
	ReductionType tsquery.ReductionType
	AddFieldMeta  *tsquery.AddFieldMeta  // nil = auto-derive from source
	EmptyValue    datasource.StaticValue // nil = null when stream is empty
}
