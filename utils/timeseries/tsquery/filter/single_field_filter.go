package filter

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
)

type SingleFieldFilter struct {
	field field.Field
}

func NewSingleFieldFilter(field field.Field) AppendFieldFilter {
	return AppendFieldFilter{field: field}
}

func (sff SingleFieldFilter) Filter(result tsquery.Result) (tsquery.Result, error) {
	// Execute field to get metadata and value supplier
	ctx := context.Background()
	fieldMeta, valueSupplier, err := sff.field.Execute(ctx)
	if err != nil {
		return tsquery.Result{}, err
	}

	return tsquery.NewResult(
		[]tsquery.FieldMeta{fieldMeta},
		stream.MapWithErrAndCtx(result.Stream(), func(ctx context.Context, record timeseries.TsRecord[[]any]) (timeseries.TsRecord[[]any], error) {
			value, err := valueSupplier(ctx, record)
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[[]any]](), err
			}
			return timeseries.TsRecord[[]any]{
				Timestamp: record.Timestamp,
				Value:     []any{value},
			}, nil
		})), nil
}
