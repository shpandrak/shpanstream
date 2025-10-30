package filter

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
)

type AppendFieldFilter struct {
	field field.Field
}

func NewAppendFieldFilter(field field.Field) AppendFieldFilter {
	return AppendFieldFilter{field: field}
}

func (a AppendFieldFilter) Filter(result tsquery.Result) (tsquery.Result, error) {
	// Verify there are no duplicates
	if result.HasField(a.field.Meta().Urn()) {
		return util.DefaultValue[tsquery.Result](), fmt.Errorf("cannot append field, since it is duplicate %s", a.field.Meta().Urn())
	}

	return *tsquery.NewResult(
		append(result.FieldsMeta(), a.field.Meta()),
		stream.MapWithErrAndCtx(result.Stream(), func(ctx context.Context, record tsquery.Record) (tsquery.Record, error) {
			value, err := a.field.GetValue(ctx)
			if err != nil {
				return util.DefaultValue[tsquery.Record](), err
			}
			return tsquery.Record{
				Timestamp: record.Timestamp,
				Value:     append(record.Value, value),
			}, nil

		})), nil
}
