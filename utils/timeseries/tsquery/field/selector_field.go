package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"maps"
)

var _ Field = SelectorField{}

type SelectorField struct {
	targetFieldUrn       string
	selectorBooleanField Field
	trueField            Field
	falseField           Field
}

func NewSelectorField(targetFieldUrn string, selectorBooleanField Field, trueField Field, falseField Field) SelectorField {
	return SelectorField{targetFieldUrn: targetFieldUrn, selectorBooleanField: selectorBooleanField, trueField: trueField, falseField: falseField}
}

func (cf SelectorField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	// Execute all three fields to get metadata
	selectorMeta, selectorValueSupplier, err := cf.selectorBooleanField.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing selector boolean field: %w", err)
	}

	trueMeta, trueValueSupplier, err := cf.trueField.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing true field: %w", err)
	}

	falseMeta, falseValueSupplier, err := cf.falseField.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing false field: %w", err)
	}

	// Validate selector field is boolean and required
	if selectorMeta.DataType() != tsquery.DataTypeBoolean {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"selector field %s must be boolean, got %s",
			selectorMeta.Urn(),
			selectorMeta.DataType(),
		)
	}

	if !selectorMeta.Required() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"selector field %s must be required",
			selectorMeta.Urn(),
		)
	}

	// Validate true and false fields have matching datatype
	trueType := trueMeta.DataType()
	falseType := falseMeta.DataType()
	if trueType != falseType {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"incompatible datatypes for true field %s and false field %s: %s vs %s",
			trueMeta.Urn(),
			falseMeta.Urn(),
			trueType,
			falseType,
		)
	}

	// Validate true and false fields have matching unit
	if trueMeta.Unit() != falseMeta.Unit() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"incompatible units for true field %s and false field %s: %s vs %s",
			trueMeta.Urn(),
			falseMeta.Urn(),
			trueMeta.Unit(),
			falseMeta.Unit(),
		)
	}

	// Validate true and false fields have matching required status
	if trueMeta.Required() != falseMeta.Required() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"incompatible required status for true field %s and false field %s: %v vs %v",
			trueMeta.Urn(),
			falseMeta.Urn(),
			trueMeta.Required(),
			falseMeta.Required(),
		)
	}

	// Merge custom metadata from both true and false fields
	var newFieldCustomMeta map[string]any
	if trueMeta.CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(trueMeta.CustomMeta())
		if falseMeta.CustomMeta() != nil {
			maps.Copy(newFieldCustomMeta, falseMeta.CustomMeta())
		}
	} else if falseMeta.CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(falseMeta.CustomMeta())
	}

	// Create field metadata
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		cf.targetFieldUrn,
		trueType,
		trueMeta.Required(),
		trueMeta.Unit(),
		newFieldCustomMeta,
	)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to create field meta for selector field: %w", err)
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		// Get the boolean selector value
		selectorValue, err := selectorValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value from selector field: %w", err)
		}

		boolValue := selectorValue.(bool)

		// Return value based on selector
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

	return *fieldMeta, valueSupplier, nil
}
