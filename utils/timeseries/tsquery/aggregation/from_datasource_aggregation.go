package aggregation

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"time"
)

// FromDatasourceAggregation aggregates a single datasource stream into scalar values.
type FromDatasourceAggregation struct {
	datasource datasource.DataSource
	fields     []AggregationFieldDef
}

func NewFromDatasourceAggregation(ds datasource.DataSource, fields []AggregationFieldDef) *FromDatasourceAggregation {
	return &FromDatasourceAggregation{
		datasource: ds,
		fields:     fields,
	}
}

func (a *FromDatasourceAggregation) Execute(ctx context.Context, from time.Time, to time.Time) (Result, error) {
	// EAGER: execute datasource, validate, create accumulators, build metadata
	dsResult, err := a.datasource.Execute(ctx, from, to)
	if err != nil {
		return Result{}, fmt.Errorf("failed to execute datasource for aggregation: %w", err)
	}

	sourceMeta := dsResult.Meta()
	sourceDataType := sourceMeta.DataType()

	// Validate fields and create accumulators
	accumulators := make([]tsquery.Accumulator, len(a.fields))
	for i, field := range a.fields {
		// Validate numeric for reductions that require it (sum, avg, min, max)
		if field.ReductionType.RequiresNumeric() {
			if !sourceDataType.IsNumeric() {
				return Result{}, fmt.Errorf("cannot aggregate non-numeric field %q (type %s) with reduction %s",
					sourceMeta.Urn(), sourceDataType, field.ReductionType)
			}
		}

		acc, err := field.ReductionType.NewAccumulator(sourceDataType)
		if err != nil {
			return Result{}, fmt.Errorf("failed to create accumulator for field %d (reduction %s): %w",
				i, field.ReductionType, err)
		}
		accumulators[i] = acc
	}

	// Validate URN uniqueness
	multipleFields := len(a.fields) > 1
	if err := validateFieldUrns(a.fields, sourceMeta, multipleFields); err != nil {
		return Result{}, err
	}

	// Build field metadata eagerly
	resultFieldsMeta := make([]tsquery.FieldMeta, len(a.fields))
	for i, field := range a.fields {
		resultDataType := field.ReductionType.GetResultDataType(sourceDataType)
		meta, err := buildFieldMeta(field, sourceMeta, resultDataType, multipleFields)
		if err != nil {
			return Result{}, fmt.Errorf("failed to build metadata for aggregation field %d: %w", i, err)
		}
		resultFieldsMeta[i] = *meta
	}

	// LAZY: stream consumption + result building
	lazyFields := lazy.NewLazy(func(ctx context.Context) ([]AggregatedValue, error) {
		// Consume the stream in a single pass, feeding all accumulators per record
		err := dsResult.Data().Consume(ctx, func(record timeseries.TsRecord[any]) {
			for _, acc := range accumulators {
				acc.Add(record.Value, record.Timestamp)
			}
		})
		if err != nil {
			return nil, fmt.Errorf("failed to consume datasource stream for aggregation: %w", err)
		}

		// Build result fields
		resultFields := make([]AggregatedValue, len(a.fields))
		for i, field := range a.fields {
			acc := accumulators[i]
			resultValue := acc.Result()

			// Handle empty stream with fallback
			if resultValue == nil && field.EmptyValue != nil {
				fallbackValue, err := executeStaticFallback(ctx, field.EmptyValue)
				if err != nil {
					return nil, fmt.Errorf("failed to execute emptyValue for aggregation field %d: %w", i, err)
				}
				resultValue = fallbackValue
			}

			resultFields[i] = AggregatedValue{
				Value:     resultValue,
				Timestamp: acc.ResultTimestamp(),
			}
		}

		return resultFields, nil
	})

	return NewResult(resultFieldsMeta, lazyFields), nil
}

func executeStaticFallback(ctx context.Context, sv datasource.StaticValue) (any, error) {
	_, supplier, err := sv.ExecuteStatic(ctx)
	if err != nil {
		return nil, err
	}
	return supplier(ctx, timeseries.TsRecord[any]{})
}

func buildFieldMeta(field AggregationFieldDef, sourceMeta tsquery.FieldMeta, resultDataType tsquery.DataType, multipleFields bool) (*tsquery.FieldMeta, error) {
	// Determine URN
	urn := resolveFieldUrn(field, sourceMeta, multipleFields)

	// Determine unit
	unit := sourceMeta.Unit()
	if field.ReductionType == tsquery.ReductionTypeCount {
		unit = "" // count has no unit
	}
	if field.AddFieldMeta != nil && field.AddFieldMeta.OverrideUnit != "" {
		unit = field.AddFieldMeta.OverrideUnit
	}

	// Determine custom metadata
	customMeta := sourceMeta.CustomMeta()
	if field.AddFieldMeta != nil && field.AddFieldMeta.CustomMeta != nil {
		customMeta = field.AddFieldMeta.CustomMeta
	}

	// Determine metric kind: aggregation produces a scalar; default gauge, respect override
	metricKind := tsquery.MetricKindGauge
	if field.AddFieldMeta != nil && field.AddFieldMeta.OverrideMetricKind != "" {
		metricKind = field.AddFieldMeta.OverrideMetricKind
	}

	return tsquery.NewFieldMetaFull(urn, resultDataType, metricKind, sourceMeta.Required(), unit, customMeta)
}

func validateFieldUrns(fields []AggregationFieldDef, sourceMeta tsquery.FieldMeta, multipleFields bool) error {
	urnSet := make(map[string]bool)
	for i, field := range fields {
		urn := resolveFieldUrn(field, sourceMeta, multipleFields)
		if urnSet[urn] {
			return fmt.Errorf("duplicate URN %q for aggregation field %d: provide explicit fieldMeta.uri to disambiguate", urn, i)
		}
		urnSet[urn] = true
	}
	return nil
}

func resolveFieldUrn(field AggregationFieldDef, sourceMeta tsquery.FieldMeta, multipleFields bool) string {
	if field.AddFieldMeta != nil && field.AddFieldMeta.Urn != "" {
		return field.AddFieldMeta.Urn
	}
	if multipleFields {
		return sourceMeta.Urn() + "_" + string(field.ReductionType)
	}
	return sourceMeta.Urn()
}
