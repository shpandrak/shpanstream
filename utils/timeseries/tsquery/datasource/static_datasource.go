package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

type StaticDatasource struct {
	result Result
}

func NewStaticDatasource(fieldMeta tsquery.FieldMeta, data stream.Stream[timeseries.TsRecord[any]]) (StaticDatasource, error) {
	// Validate no collisions in field URNs
	return StaticDatasource{
		result: Result{
			meta: fieldMeta,
			data: data,
		},
	}, nil
}

func (s StaticDatasource) Execute(
	_ context.Context,
	from time.Time,
	to time.Time,
) (Result, error) {
	return Result{
		meta: s.result.meta,
		data: s.result.data.Filter(func(src timeseries.TsRecord[any]) bool {
			return !src.Timestamp.Before(from) && src.Timestamp.Before(to)
		}),
	}, nil
}
