package filter

import (
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type Filter interface {
	Filter(result tsquery.Result) (tsquery.Result, error)
}
