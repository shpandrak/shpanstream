package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

type DataSource interface {
	Execute(ctx context.Context, from time.Time, to time.Time) (tsquery.Result, error)
}
