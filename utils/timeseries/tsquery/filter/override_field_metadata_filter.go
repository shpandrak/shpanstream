package filter

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Filter = OverrideFieldMetadataFilter{}

// OverrideFieldMetadataFilter replaces the result stream with a single field extracted using the provided field.
type OverrideFieldMetadataFilter struct {
	fieldUrn             string
	optUpdatedUrn        *string
	optUpdatedUnit       *string
	optUpdatedCustomMeta map[string]any
}

func NewOverrideFieldMetadataFilter(fieldUrn string, optUpdatedUrn *string, optUpdatedUnit *string, optUpdatedCustomMeta map[string]any) *OverrideFieldMetadataFilter {
	return &OverrideFieldMetadataFilter{
		fieldUrn:             fieldUrn,
		optUpdatedUrn:        optUpdatedUrn,
		optUpdatedUnit:       optUpdatedUnit,
		optUpdatedCustomMeta: optUpdatedCustomMeta,
	}
}

func (ofm OverrideFieldMetadataFilter) Filter(result tsquery.Result) (tsquery.Result, error) {
	// Find the field metadata for the specified URN
	fieldsMeta := result.FieldsMeta()

	originalFieldMeta, fieldIndex, err := tsquery.FieldAndIdxByUrn(fieldsMeta, ofm.fieldUrn)
	if err != nil {
		return util.DefaultValue[tsquery.Result](), err
	}

	if fieldIndex == -1 {
		return util.DefaultValue[tsquery.Result](), fmt.Errorf("field with urn %s not found in result", ofm.fieldUrn)
	}

	// Determine the new URN (use updated or keep original)
	newUrn := originalFieldMeta.Urn()
	if ofm.optUpdatedUrn != nil {
		if *ofm.optUpdatedUrn != originalFieldMeta.Urn() {
			// Check for URN conflict
			if result.HasField(*ofm.optUpdatedUrn) {
				return util.DefaultValue[tsquery.Result](), fmt.Errorf("cannot update field URN to %s, since it already exists in the result", *ofm.optUpdatedUrn)
			}
			newUrn = *ofm.optUpdatedUrn
		}
	}

	// Determine the new unit (use updated or keep original)
	newUnit := originalFieldMeta.Unit()
	if ofm.optUpdatedUnit != nil {
		newUnit = *ofm.optUpdatedUnit
	}

	// Merge custom metadata
	var newCustomMeta map[string]any
	if ofm.optUpdatedCustomMeta != nil {
		if len(ofm.optUpdatedCustomMeta) > 0 {
			newCustomMeta = ofm.optUpdatedCustomMeta
		}
	}

	// Create new field metadata with overridden values
	// Keep dataType and required unchanged as per requirements
	updatedFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		newUrn,
		originalFieldMeta.DataType(),
		originalFieldMeta.Required(),
		newUnit,
		newCustomMeta,
	)
	if err != nil {
		return util.DefaultValue[tsquery.Result](), fmt.Errorf("failed to create updated field metadata: %w", err)
	}

	// Create new fields metadata slice with the updated field
	newFieldsMeta := make([]tsquery.FieldMeta, len(fieldsMeta))
	copy(newFieldsMeta, fieldsMeta)
	newFieldsMeta[fieldIndex] = *updatedFieldMeta

	// Return a new result with updated metadata but the same stream
	return tsquery.NewResult(newFieldsMeta, result.Stream()), nil
}
