package datasource

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Filter = OverrideFieldMetadataFilter{}

// OverrideFieldMetadataFilter allows overriding specific metadata properties
// of a datasource result. It can selectively modify URN, Unit, and CustomMeta
// while preserving DataType and Required status (which are immutable).
type OverrideFieldMetadataFilter struct {
	optUpdatedUrn        *string
	optUpdatedUnit       *string
	optUpdatedCustomMeta map[string]any
}

// NewOverrideFieldMetadataFilter creates a new filter that overrides field metadata.
// All parameters are optional (can be nil). If provided:
// - optUpdatedUrn: overrides the field's URN
// - optUpdatedUnit: overrides the field's unit
// - optUpdatedCustomMeta: replaces the field's custom metadata entirely
func NewOverrideFieldMetadataFilter(
	optUpdatedUrn *string,
	optUpdatedUnit *string,
	optUpdatedCustomMeta map[string]any,
) OverrideFieldMetadataFilter {
	return OverrideFieldMetadataFilter{
		optUpdatedUrn:        optUpdatedUrn,
		optUpdatedUnit:       optUpdatedUnit,
		optUpdatedCustomMeta: optUpdatedCustomMeta,
	}
}

func (ofm OverrideFieldMetadataFilter) Filter(result Result) (Result, error) {
	// Get the field metadata from the single-field result
	fieldMeta := result.Meta()

	// Determine final values: use override if provided, otherwise keep original
	finalUrn := fieldMeta.Urn()
	if ofm.optUpdatedUrn != nil {
		finalUrn = *ofm.optUpdatedUrn
	}

	finalUnit := fieldMeta.Unit()
	if ofm.optUpdatedUnit != nil {
		finalUnit = *ofm.optUpdatedUnit
	}

	finalCustomMeta := fieldMeta.CustomMeta()
	if ofm.optUpdatedCustomMeta != nil {
		// Replace custom metadata entirely (not merge)
		finalCustomMeta = ofm.optUpdatedCustomMeta
	}

	// Create new field metadata with updated values
	// DataType and Required remain immutable
	updatedFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		finalUrn,
		fieldMeta.DataType(),
		fieldMeta.Required(),
		finalUnit,
		finalCustomMeta,
	)
	if err != nil {
		return Result{}, fmt.Errorf("failed to create updated field metadata: %w", err)
	}

	// Return a new result with updated metadata but same data stream
	return Result{
		meta: *updatedFieldMeta,
		data: result.Data(),
	}, nil
}
