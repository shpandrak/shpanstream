package field

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type ValueSupplier func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error)

// Claude: this was the old interface
//type Field interface {
//	Meta() tsquery.FieldMeta
//	GetValue(ctx context.Context) (any, error)
//}

type Field interface {
	Execute(ctx context.Context) (tsquery.FieldMeta, ValueSupplier, error)
}
