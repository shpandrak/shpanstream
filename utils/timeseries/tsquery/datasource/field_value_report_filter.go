package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

// FieldValueFilter replaces an existing field with a new field value.
type FieldValueFilter struct {
	fieldValue  Value
	updatedMeta tsquery.AddFieldMeta
}

func NewFieldValueFilter(updatedValue Value, updatedMeta tsquery.AddFieldMeta) FieldValueFilter {
	return FieldValueFilter{
		fieldValue:  updatedValue,
		updatedMeta: updatedMeta,
	}
}

func (r FieldValueFilter) Filter(ctx context.Context, result Result) (Result, error) {

	// Prepare field to get metadata and value supplier
	fieldMeta, valueSupplier, err := PrepareFieldValue(ctx, r.updatedMeta, r.fieldValue, result.meta)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed preparing field for filter value: %w", err)
	}

	// Create new fieldsMeta with the field replaced
	return Result{
		meta: *fieldMeta,
		data: stream.MapWithErrAndCtx(
			result.data,
			func(ctx context.Context, record timeseries.TsRecord[any]) (timeseries.TsRecord[any], error) {
				value, err := valueSupplier(ctx, record)
				if err != nil {
					return util.DefaultValue[timeseries.TsRecord[any]](), err
				}
				return timeseries.TsRecord[any]{
					Timestamp: record.Timestamp,
					Value:     value,
				}, nil
			},
		),
	}, nil
}
