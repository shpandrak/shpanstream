package tsquery

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
)

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
