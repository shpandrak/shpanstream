package report

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Filter = SingleFieldFilter{}

// SingleFieldFilter replaces the result stream with a single field extracted using the provided field.
type SingleFieldFilter struct {
	fieldValue Value
	meta       tsquery.AddFieldMeta
}

func NewSingleFieldFilter(fieldValue Value, meta tsquery.AddFieldMeta) SingleFieldFilter {
	return SingleFieldFilter{fieldValue: fieldValue, meta: meta}
}

func (sff SingleFieldFilter) Filter(ctx context.Context, result Result) (Result, error) {
	// Prepare field to get metadata and value supplier
	fieldMeta, valueSupplier, err := PrepareField(ctx, sff.meta, sff.fieldValue, result.FieldsMeta())
	if err != nil {
		return Result{}, err
	}

	return NewResult(
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
