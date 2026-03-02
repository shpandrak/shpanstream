package aggregation

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

// ExpressionAggregationFieldDef defines one computed field in an expression aggregation.
type ExpressionAggregationFieldDef struct {
	AddFieldMeta tsquery.AddFieldMeta
	Value        AggregationValue
}

// ExpressionAggregation computes new scalar fields from the results of a source aggregation
// using arithmetic expressions. Only the explicitly defined expression fields are output;
// source fields are not passed through unless referenced via a ref expression.
type ExpressionAggregation struct {
	source Aggregator
	fields []ExpressionAggregationFieldDef
}

func NewExpressionAggregation(source Aggregator, fields []ExpressionAggregationFieldDef) *ExpressionAggregation {
	return &ExpressionAggregation{source: source, fields: fields}
}

func (e *ExpressionAggregation) Execute(ctx context.Context, from, to time.Time) (Result, error) {
	// EAGER: execute source aggregation
	sourceResult, err := e.source.Execute(ctx, from, to)
	if err != nil {
		return Result{}, fmt.Errorf("expression aggregation: failed to execute source: %w", err)
	}

	// Build source type map from eager metadata
	sourceMeta := sourceResult.FieldsMeta()
	sourceTypes := make(map[string]tsquery.DataType, len(sourceMeta))
	for _, meta := range sourceMeta {
		sourceTypes[meta.Urn()] = meta.DataType()
	}

	// Build eager metadata for expression fields with correct resolved types.
	resultFieldsMeta := make([]tsquery.FieldMeta, len(e.fields))
	for i, field := range e.fields {
		urn := field.AddFieldMeta.Urn
		if urn == "" {
			return Result{}, fmt.Errorf("expression aggregation field %d: urn is required", i)
		}

		resolvedType, err := field.Value.ResolveType(sourceTypes)
		if err != nil {
			return Result{}, fmt.Errorf("expression aggregation field %d (%s): failed to resolve type: %w", i, urn, err)
		}

		meta, err := tsquery.NewFieldMetaWithCustomData(urn, resolvedType, true, field.AddFieldMeta.OverrideUnit, field.AddFieldMeta.CustomMeta)
		if err != nil {
			return Result{}, fmt.Errorf("expression aggregation field %d: failed to build metadata: %w", i, err)
		}
		resultFieldsMeta[i] = *meta
	}

	// LAZY: resolve source values, evaluate expressions
	lazyFields := lazy.NewLazy(func(ctx context.Context) ([]AggregatedValue, error) {
		// Get source aggregation values
		sourceValues, err := sourceResult.Fields().Get(ctx)
		if err != nil {
			return nil, fmt.Errorf("expression aggregation: failed to get source values: %w", err)
		}

		// Build resolved field map keyed by URN
		sourceMeta := sourceResult.FieldsMeta()
		resolved := make(map[string]ResolvedField, len(sourceMeta))
		for i, meta := range sourceMeta {
			resolved[meta.Urn()] = ResolvedField{
				Value:    sourceValues[i].Value,
				DataType: meta.DataType(),
			}
		}

		// Evaluate each expression field
		resultFields := make([]AggregatedValue, len(e.fields))
		for i, field := range e.fields {
			value, _, err := field.Value.Evaluate(resolved)
			if err != nil {
				return nil, fmt.Errorf("expression aggregation field %d (%s): evaluation failed: %w", i, field.AddFieldMeta.Urn, err)
			}
			resultFields[i] = AggregatedValue{Value: value}
		}

		return resultFields, nil
	})

	return NewResult(resultFieldsMeta, lazyFields), nil
}
