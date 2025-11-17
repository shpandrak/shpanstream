package field

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = RefFieldValue{}

type RefFieldValue struct {
	urn string
}

func NewRefFieldValue(urn string) RefFieldValue {
	return RefFieldValue{urn: urn}
}

func (rf RefFieldValue) Execute(fieldsMeta []tsquery.FieldMeta) (ValueMeta, ValueSupplier, error) {
	fm, idx, err := tsquery.FieldAndIdxByUrn(fieldsMeta, rf.urn)
	if err != nil {
		return util.DefaultValue[ValueMeta](), nil, err
	}

	// Create a value supplier that extracts the value from the current row at the correct index
	valueSupplier := func(_ context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		return currRow.Value[idx], nil
	}

	return ValueMeta{
		DataType: fm.DataType(),
		Unit:     fm.Unit(),
		Required: fm.Required(),
	}, valueSupplier, nil
}
