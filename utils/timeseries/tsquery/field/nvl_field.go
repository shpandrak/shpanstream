package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"maps"
)

var _ Field = NvlField{}

type NvlField struct {
	source   Field
	altField Field
	fieldUrn string
}

func NewNvlField(
	fieldUrn string,
	source Field,
	altField Field,
) NvlField {
	return NvlField{
		source:   source,
		altField: altField,
		fieldUrn: fieldUrn,
	}
}

func (nf NvlField) Execute(ctx context.Context) (tsquery.FieldMeta, ValueSupplier, error) {
	// Execute both fields to get metadata (lazy validation)
	sourceMeta, sourceValueSupplier, err := nf.source.Execute(ctx)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing source field: %w", err)
	}
	altMeta, altValueSupplier, err := nf.altField.Execute(ctx)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing alternative field: %w", err)
	}

	sourceType := sourceMeta.DataType()
	altType := altMeta.DataType()

	// Check that both fields have the same data type
	if sourceType != altType {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf(
			"incompatible datatypes for fields %s and %s: %s vs %s",
			sourceMeta.Urn(),
			altMeta.Urn(),
			sourceType,
			altType,
		)
	}

	// Alternative field must be required
	if !altMeta.Required() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("alternative field %s must be required", altMeta.Urn())
	}

	// Merge custom metadata from both fields
	var newFieldCustomMeta map[string]any
	if sourceMeta.CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(sourceMeta.CustomMeta())
		if altMeta.CustomMeta() != nil {
			maps.Copy(newFieldCustomMeta, altMeta.CustomMeta())
		}
	} else if altMeta.CustomMeta() != nil {
		newFieldCustomMeta = maps.Clone(altMeta.CustomMeta())
	}

	// Create field metadata - NVL field is always required since we have a fallback
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		nf.fieldUrn,
		sourceType,
		true, // Always required because we have a non-null fallback
		sourceMeta.Unit(),
		newFieldCustomMeta,
	)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to create field meta for nvl field: %w", err)
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		// If source is required, it can't be null, so just return its value
		if sourceMeta.Required() {
			return sourceValueSupplier(ctx, currRow)
		}

		// Get value from source
		value, err := sourceValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value from source field: %w", err)
		}

		// If source value is nil, use alternative field
		if value == nil {
			altValue, err := altValueSupplier(ctx, currRow)
			if err != nil {
				return nil, fmt.Errorf("failed getting value from alternative field: %w", err)
			}
			return altValue, nil
		}

		return value, nil
	}

	return *fieldMeta, valueSupplier, nil
}
