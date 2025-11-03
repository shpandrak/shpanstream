package filter

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
)

// AppendFieldFilter appends a new field to the existing result using the provided field.
type AppendFieldFilter struct {
	field field.Field
}

func NewAppendFieldFilter(field field.Field) AppendFieldFilter {
	return AppendFieldFilter{field: field}
}

func (a AppendFieldFilter) Filter(result tsquery.Result) (tsquery.Result, error) {
	// Execute field to get metadata and value supplier
	fieldMeta, valueSupplier, err := a.field.Execute(result.FieldsMeta())
	if err != nil {
		return util.DefaultValue[tsquery.Result](), err
	}

	// Verify there are no duplicates
	if result.HasField(fieldMeta.Urn()) {
		return util.DefaultValue[tsquery.Result](), fmt.Errorf("cannot append field, since it is duplicate %s", fieldMeta.Urn())
	}

	return tsquery.NewResult(
		append(result.FieldsMeta(), fieldMeta),
		stream.MapWithErrAndCtx(result.Stream(), func(ctx context.Context, record timeseries.TsRecord[[]any]) (timeseries.TsRecord[[]any], error) {
			value, err := valueSupplier(ctx, record)
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[[]any]](), err
			}
			return timeseries.TsRecord[[]any]{
				Timestamp: record.Timestamp,
				Value:     append(record.Value, value),
			}, nil

		})), nil
}
