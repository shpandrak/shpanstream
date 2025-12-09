package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Filter = DeltaFilter{}

type DeltaFilter struct{}

func NewDeltaFilter() DeltaFilter {
	return DeltaFilter{}
}

func (df DeltaFilter) Filter(_ context.Context, result Result) (Result, error) {
	dataType := result.meta.DataType()
	if !dataType.IsNumeric() {
		return util.DefaultValue[Result](), fmt.Errorf(
			"delta filter can only be applied to numeric data types, got: %s",
			dataType,
		)
	}
	if !result.meta.Required() {
		return util.DefaultValue[Result](), fmt.Errorf(
			"delta filter can only be applied to required fields (no nils allowed)",
		)
	}

	subFunc, err := tsquery.BinaryNumericOperatorSub.GetFuncImpl(dataType)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to get subtraction function: %w", err)
	}

	var prevItem *timeseries.TsRecord[any]
	return Result{
		meta: result.meta,
		data: stream.MapWhileFilteringWithErr(
			result.data,
			func(item timeseries.TsRecord[any]) (*timeseries.TsRecord[any], error) {
				// Skipping the first item, just storing the reference
				if prevItem == nil {
					prevItem = &item
					return nil, nil
				}
				delta := subFunc(item.Value, prevItem.Value)
				prevItem = &item
				return &timeseries.TsRecord[any]{
					Value:     delta,
					Timestamp: item.Timestamp,
				}, nil
			},
		),
	}, nil
}
