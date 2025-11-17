package filter

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
)

var _ Filter = SingleFieldFilter{}

// SingleFieldFilter replaces the result stream with a single field extracted using the provided field.
type SingleFieldFilter struct {
	fieldValue field.Value
	meta       AddFieldMeta
}

func NewSingleFieldFilter(fieldValue field.Value, meta AddFieldMeta) SingleFieldFilter {
	return SingleFieldFilter{fieldValue: fieldValue, meta: meta}
}

func (sff SingleFieldFilter) Filter(result tsquery.Result) (tsquery.Result, error) {
	// Prepare field to get metadata and value supplier
	fieldMeta, valueSupplier, err := PrepareField(sff.meta, sff.fieldValue, result.FieldsMeta())
	if err != nil {
		return tsquery.Result{}, err
	}

	return tsquery.NewResult(
		[]tsquery.FieldMeta{*fieldMeta},
		stream.MapWithErrAndCtx(
			result.Stream(),
			func(ctx context.Context, record timeseries.TsRecord[[]any]) (timeseries.TsRecord[[]any], error) {
				value, err := valueSupplier(ctx, record)
				if err != nil {
					return util.DefaultValue[timeseries.TsRecord[[]any]](), err
				}
				return timeseries.TsRecord[[]any]{
					Timestamp: record.Timestamp,
					Value:     []any{value},
				}, nil
			},
		)), nil
}
