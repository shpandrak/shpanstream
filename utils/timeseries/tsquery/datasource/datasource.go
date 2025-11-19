package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

type Result struct {
	meta tsquery.FieldMeta
	data stream.Stream[timeseries.TsRecord[any]]
}

func NewResult(meta tsquery.FieldMeta, data stream.Stream[timeseries.TsRecord[any]]) Result {
	return Result{meta: meta, data: data}
}

func (r Result) Meta() tsquery.FieldMeta {
	return r.meta
}

func (r Result) Data() stream.Stream[timeseries.TsRecord[any]] {
	return r.data
}

type DataSource interface {
	Execute(ctx context.Context, from time.Time, to time.Time) (Result, error)
}

type MultiDataSource interface {
	GetDatasources(ctx context.Context) stream.Stream[DataSource]
}
