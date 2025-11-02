package filter

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"time"
)

type Filter interface {
	Filter(result tsquery.Result) (tsquery.Result, error)
}

func ApplyFilters(result tsquery.Result, filters ...Filter) (tsquery.Result, error) {
	var err error
	for _, filter := range filters {
		result, err = filter.Filter(result)
		if err != nil {
			return tsquery.Result{}, err
		}
	}
	return result, nil
}

type filteredDataSource struct {
	dataSource datasource.DataSource
	filters    []Filter
}

func (f filteredDataSource) Execute(ctx context.Context, from time.Time, to time.Time) (tsquery.Result, error) {
	result, err := f.dataSource.Execute(ctx, from, to)
	if err != nil {
		return util.DefaultValue[tsquery.Result](), err
	}
	return ApplyFilters(result, f.filters...)
}

func NewFilteredDataSource(dataSource datasource.DataSource, filters ...Filter) datasource.DataSource {
	return &filteredDataSource{
		dataSource: dataSource,
		filters:    filters,
	}
}
