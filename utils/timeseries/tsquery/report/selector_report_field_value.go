package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = SelectorFieldValue{}

type SelectorFieldValue struct {
	selectorBooleanField Value
	trueField            Value
	falseField           Value
}

func NewSelectorFieldValue(
	selectorBooleanField Value,
	trueField Value,
	falseField Value,
) SelectorFieldValue {
	return SelectorFieldValue{
		selectorBooleanField: selectorBooleanField,
		trueField:            trueField,
		falseField:           falseField,
	}
}

func (cf SelectorFieldValue) Execute(ctx context.Context, fieldsMeta []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	// Execute all three fields to get metadata
	selectorMeta, selectorValueSupplier, err := cf.selectorBooleanField.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing selector boolean field: %w", err)
	}

	trueMeta, trueValueSupplier, err := cf.trueField.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing true field: %w", err)
	}

	falseMeta, falseValueSupplier, err := cf.falseField.Execute(ctx, fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed executing false field: %w", err)
	}

	// Validate selector field is boolean and required
	if selectorMeta.DataType != tsquery.DataTypeBoolean {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"selector field must be boolean, got %s",
			selectorMeta.DataType,
		)
	}

	if !selectorMeta.Required {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"selector field must be required",
		)
	}

	// Validate true and false fields have matching datatype
	trueType := trueMeta.DataType
	falseType := falseMeta.DataType
	if trueType != falseType {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"incompatible datatypes for true field and false field: %s vs %s",
			trueType,
			falseType,
		)
	}

	// Validate true and false fields have matching unit
	if trueMeta.Unit != falseMeta.Unit {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"incompatible units for true field and false field: %s vs %s",
			trueMeta.Unit,
			falseMeta.Unit,
		)
	}

	// Validate true and false fields have matching required status
	if trueMeta.Required != falseMeta.Required {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"incompatible required status for true field and false field: %v vs %v",
			trueMeta.Required,
			falseMeta.Required,
		)
	}

	// Create field metadata
	fvm := tsquery.ValueMeta{
		DataType: trueType,
		Unit:     trueMeta.Unit,
		Required: trueMeta.Required,
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		// Get the boolean selector value
		selectorValue, err := selectorValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value from selector field: %w", err)
		}

		boolValue := selectorValue.(bool)

		// Return value based on a selector
		if boolValue {
			value, err := trueValueSupplier(ctx, currRow)
			if err != nil {
				return nil, fmt.Errorf("failed getting value from true field: %w", err)
			}
			return value, nil
		} else {
			value, err := falseValueSupplier(ctx, currRow)
			if err != nil {
				return nil, fmt.Errorf("failed getting value from false field: %w", err)
			}
			return value, nil
		}
	}

	return fvm, valueSupplier, nil
}
