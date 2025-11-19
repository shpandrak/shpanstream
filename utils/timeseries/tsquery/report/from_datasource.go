package report

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"time"
)

type datasourceReport struct {
	datasource.DataSource
}

func FromDatasource(ds datasource.DataSource) DataSource {
	return datasourceReport{DataSource: ds}
}

func (d datasourceReport) Execute(ctx context.Context, from time.Time, to time.Time) (Result, error) {
	dsResult, err := d.DataSource.Execute(ctx, from, to)
	if err != nil {
		return util.DefaultValue[Result](), err
	}
	return NewResult(
		[]tsquery.FieldMeta{dsResult.Meta()},
		stream.Map(
			dsResult.Data(),
			func(record timeseries.TsRecord[any]) timeseries.TsRecord[[]any] {
				return timeseries.TsRecord[[]any]{
					Timestamp: record.Timestamp,
					Value:     []any{record.Value},
				}
			},
		),
	), nil
}

type multiReportFromMultiDatasource struct {
	datasource.MultiDataSource
}

func (m multiReportFromMultiDatasource) GetDatasources(ctx context.Context) stream.Stream[DataSource] {
	return stream.Map(m.MultiDataSource.GetDatasources(ctx), FromDatasource)
}

func FromMultiDatasource(multiDs datasource.MultiDataSource) MultiDataSource {
	return multiReportFromMultiDatasource{MultiDataSource: multiDs}
}
