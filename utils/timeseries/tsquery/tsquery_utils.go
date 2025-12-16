package tsquery

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
)

// MergeCustomMeta merges base and override custom metadata maps.
// Values from override take precedence on key conflicts.
// Returns nil only if both inputs are nil.
func MergeCustomMeta(base, override map[string]any) map[string]any {
	if base == nil && override == nil {
		return nil
	}
	if base == nil {
		return override
	}
	if override == nil {
		return base
	}
	result := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		result[k] = v
	}
	for k, v := range override {
		result[k] = v
	}
	return result
}

func FieldAndIdxByUrn(fieldsMeta []FieldMeta, urn string) (FieldMeta, int, error) {
	// Find the field in fieldsMeta by URN
	for i, meta := range fieldsMeta {
		if meta.Urn() == urn {
			return meta, i, nil
		}
	}

	// If field not found, return error
	return util.DefaultValue[FieldMeta](), -1, fmt.Errorf("field with URN %s not found time series", urn)
}
