package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type ValueSupplier func(ctx context.Context, currRow timeseries.TsRecord[any]) (any, error)

type Value interface {
	Execute(meta tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error)
}
