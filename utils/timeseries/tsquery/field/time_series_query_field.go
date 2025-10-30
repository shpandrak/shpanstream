package field

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type Field interface {
	Meta() tsquery.FieldMeta
	GetValue(ctx context.Context) (any, error)
}
