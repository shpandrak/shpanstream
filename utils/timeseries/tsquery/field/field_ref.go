package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Field = RefField{}

type RefField struct {
	urn string
}

func NewRefField(urn string) RefField {
	return RefField{urn: urn}
}

func (rf RefField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	// Find the field in fieldsMeta by URN
	fieldIndex := -1
	var foundMeta tsquery.FieldMeta
	for i, meta := range fieldsMeta {
		if meta.Urn() == rf.urn {
			fieldIndex = i
			foundMeta = meta
			break
		}
	}

	// If field not found, return error
	if fieldIndex == -1 {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("field reference not found: %s", rf.urn)
	}

	// Create a value supplier that extracts the value from the current row at the correct index
	valueSupplier := func(_ context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		return currRow.Value[fieldIndex], nil
	}

	return foundMeta, valueSupplier, nil
}
