package report

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"time"
)

type DataSource interface {
	Execute(ctx context.Context, from time.Time, to time.Time) (Result, error)
}

type MultiDataSource interface {
	GetDatasources(ctx context.Context) stream.Stream[DataSource]
}
