package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"maps"
)

type NvlField struct {
	source    Field
	altField  Field
	fieldMeta tsquery.FieldMeta
}

func NewNvlField(
	fieldUrn string,
	source Field,
	altField Field,
) (*NvlField, error) {
	sourceType := source.Meta().DataType()
	altType := altField.Meta().DataType()

	// Check that both fields have the same data type
	if sourceType != altType {
		return nil, fmt.Errorf(
			"incompatible datatypes for fields %s and %s: %s vs %s",
			source.Meta().Urn(),
			altField.Meta().Urn(),
			sourceType,
			altType,
		)
	}

	// Alternative field must be required
	if !altField.Meta().Required() {
		return nil, fmt.Errorf("alternative field %s must be required", altField.Meta().Urn())
	}

	// Merge custom metadata from both fields
	var newFieldCustomMeta map[string]any
	if source.Meta().CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(source.Meta().CustomMeta())
		if altField.Meta().CustomMeta() != nil {
			maps.Copy(newFieldCustomMeta, altField.Meta().CustomMeta())
		}
	} else if altField.Meta().CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(altField.Meta().CustomMeta())
	}

	// Create field metadata - NVL field is always required since we have a fallback
	newFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		fieldUrn,
		sourceType,
		true, // Always required because we have a non-null fallback
		newFieldCustomMeta,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create field meta for nvl field: %w", err)
	}

	return &NvlField{
		source:    source,
		altField:  altField,
		fieldMeta: *newFieldMeta,
	}, nil
}

func (nf *NvlField) Meta() tsquery.FieldMeta {
	return nf.fieldMeta
}

func (nf *NvlField) GetValue(ctx context.Context) (any, error) {
	// If source is required, it can't be null, so just return its value
	if nf.source.Meta().Required() {
		return nf.source.GetValue(ctx)
	}

	// Get value from source
	value, err := nf.source.GetValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting value from source field: %w", err)
	}

	// If source value is nil, use alternative field
	if value == nil {
		altValue, err := nf.altField.GetValue(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed getting value from alternative field: %w", err)
		}
		return altValue, nil
	}

	return value, nil
}
