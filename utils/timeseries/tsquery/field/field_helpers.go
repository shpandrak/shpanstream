package field

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

func fieldAndIdxByUrn(fieldsMeta []tsquery.FieldMeta, urn string) (tsquery.FieldMeta, int, error) {
	// Find the field in fieldsMeta by URN
	for i, meta := range fieldsMeta {
		if meta.Urn() == urn {
			return meta, i, nil
		}
	}

	// If field not found, return error
	return util.DefaultValue[tsquery.FieldMeta](), -1, fmt.Errorf("field with URN %s not found time series", urn)
}
