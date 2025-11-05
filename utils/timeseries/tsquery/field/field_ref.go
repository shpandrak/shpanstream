package field

import (
	"context"
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
	fm, idx, err := tsquery.FieldAndIdxByUrn(fieldsMeta, rf.urn)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, err
	}

	// Create a value supplier that extracts the value from the current row at the correct index
	valueSupplier := func(_ context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		return currRow.Value[idx], nil
	}

	return fm, valueSupplier, nil
}
