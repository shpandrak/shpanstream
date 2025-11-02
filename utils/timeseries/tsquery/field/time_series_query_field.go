package field

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type ValueSupplier func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error)

type Field interface {
	Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error)
}
