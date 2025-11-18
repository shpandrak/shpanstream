package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
)

type ListMultiDatasource struct {
	datasources []DataSource
}

func NewListMultiDatasource(datasources []DataSource) *ListMultiDatasource {
	return &ListMultiDatasource{datasources: datasources}
}

func (l ListMultiDatasource) GetDatasources(_ context.Context) stream.Stream[DataSource] {
	return stream.FromSlice(l.datasources)
}
