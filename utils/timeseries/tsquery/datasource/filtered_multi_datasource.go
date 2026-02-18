package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
)

type FilteredMultiDatasource struct {
	inner   MultiDataSource
	filters []Filter
}

func NewFilteredMultiDatasource(inner MultiDataSource, filters []Filter) *FilteredMultiDatasource {
	return &FilteredMultiDatasource{inner: inner, filters: filters}
}

func (f FilteredMultiDatasource) GetDatasources(ctx context.Context) stream.Stream[DataSource] {
	return stream.Map(f.inner.GetDatasources(ctx), func(ds DataSource) DataSource {
		return NewFilteredDataSource(ds, f.filters...)
	})
}
