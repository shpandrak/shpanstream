package field

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type ValueSupplier func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error)

type ValueMeta struct {
	DataType tsquery.DataType
	Unit     string
	Required bool
}

type Value interface {
	Execute(fieldsMeta []tsquery.FieldMeta) (ValueMeta, ValueSupplier, error)
}
