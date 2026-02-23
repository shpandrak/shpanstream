package report

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = RowTimestampFieldValue{}

type RowTimestampFieldValue struct {
}

func NewRowTimestampFieldValue() RowTimestampFieldValue {
	return RowTimestampFieldValue{}
}

func (rf RowTimestampFieldValue) Execute(_ context.Context, _ []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	valueSupplier := func(_ context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		return currRow.Timestamp, nil
	}

	return tsquery.ValueMeta{
		DataType: tsquery.DataTypeTimestamp,
		Required: true,
	}, valueSupplier, nil
}
