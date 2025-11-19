package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = RefFieldValue{}

type RefFieldValue struct {
}

func NewRefFieldValue() RefFieldValue {
	return RefFieldValue{}
}

func (rf RefFieldValue) Execute(fieldsMeta tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	// Create a value supplier that extracts the value from the current row at the correct index
	valueSupplier := func(_ context.Context, currRow timeseries.TsRecord[any]) (any, error) {
		return currRow.Value, nil
	}

	return tsquery.ValueMeta{
		DataType: fieldsMeta.DataType(),
		Unit:     fieldsMeta.Unit(),
		Required: fieldsMeta.Required(),
	}, valueSupplier, nil
}
