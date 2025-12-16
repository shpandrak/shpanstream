package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = CastFieldValue{}

type CastFieldValue struct {
	source     Value
	targetType tsquery.DataType
}

// Conversion functions for each type pair

func NewCastFieldValue(
	source Value,
	targetType tsquery.DataType,
) CastFieldValue {
	return CastFieldValue{
		source:     source,
		targetType: targetType,
	}
}

func (cf CastFieldValue) Execute(ctx context.Context, fieldMeta tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	// Execute source to get metadata (lazy validation)
	sourceMeta, sourceValueSupplier, err := cf.source.Execute(ctx, fieldMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing source field: %w", err)
	}
	sourceType := sourceMeta.DataType

	// Get the cast function
	castFunc, err := tsquery.GetCastFuncForDataType(sourceType, cf.targetType)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed to get cast function: %w", err)
	}

	// Create field value metadata for the cast result
	fieldValueMeta := tsquery.ValueMeta{
		DataType:   cf.targetType,
		Unit:       sourceMeta.Unit,
		Required:   sourceMeta.Required,
		CustomMeta: sourceMeta.CustomMeta,
	}

	// Wrap cast function to handle nil for optional fields
	if !fieldValueMeta.Required {
		originalFunc := castFunc
		castFunc = func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}
			return originalFunc(v)
		}
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[any]) (any, error) {
		value, err := sourceValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value from source field: %w", err)
		}

		result, err := castFunc(value)
		if err != nil {
			return nil, fmt.Errorf("failed casting value: %w", err)
		}

		return result, nil
	}

	return fieldValueMeta, valueSupplier, nil
}
