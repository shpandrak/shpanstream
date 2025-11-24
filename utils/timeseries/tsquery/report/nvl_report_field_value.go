package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = NvlFieldValue{}

type NvlFieldValue struct {
	source   Value
	altField Value
}

func NewNvlFieldValue(
	source Value,
	altField Value,
) NvlFieldValue {
	return NvlFieldValue{
		source:   source,
		altField: altField,
	}
}

func (nf NvlFieldValue) Execute(ctx context.Context, fieldsMeta []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	// Execute both fields to get metadata (lazy validation)
	sourceMeta, sourceValueSupplier, err := nf.source.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing source field: %w", err)
	}
	altMeta, altValueSupplier, err := nf.altField.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing alternative field: %w", err)
	}

	sourceType := sourceMeta.DataType
	altType := altMeta.DataType

	// Check that both fields have the same data type
	if sourceType != altType {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"incompatible datatypes for fields for nvl field: %s vs %s",
			sourceType,
			altType,
		)
	}

	// Alternative field must be required
	if !altMeta.Required {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("alternative field must be required")
	}

	// Create field metadata - NVL field is always required since we have a fallback
	fvm := tsquery.ValueMeta{
		DataType: sourceType,
		Unit:     sourceMeta.Unit,
		Required: true,
	}
	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		// If the source is required, it can't be null, so just return its value
		if sourceMeta.Required {
			return sourceValueSupplier(ctx, currRow)
		}

		// Get value from the source
		value, err := sourceValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value from source field: %w", err)
		}

		// If the source value is nil, use an alternative field
		if value == nil {
			altValue, err := altValueSupplier(ctx, currRow)
			if err != nil {
				return nil, fmt.Errorf("failed getting value from alternative field: %w", err)
			}
			return altValue, nil
		}

		return value, nil
	}

	return fvm, valueSupplier, nil
}
