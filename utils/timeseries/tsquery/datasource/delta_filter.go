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

type DeltaFilter struct {
	nonNegative    bool
	maxCounterValue float64
}

func NewDeltaFilter(nonNegative bool, maxCounterValue float64) DeltaFilter {
	return DeltaFilter{nonNegative: nonNegative, maxCounterValue: maxCounterValue}
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

				if df.nonNegative {
					currVal, err := dataType.ToFloat64(item.Value)
					if err != nil {
						return nil, fmt.Errorf("failed to convert current value to float64: %w", err)
					}
					prevVal, err := dataType.ToFloat64(prevItem.Value)
					if err != nil {
						return nil, fmt.Errorf("failed to convert previous value to float64: %w", err)
					}

					// Negative current value: drop the point entirely (don't update prevItem)
					if currVal < 0 {
						return nil, nil
					}

					// Reset detected: current < previous
					if currVal < prevVal {
						prevItem = &item
						var delta float64
						if df.maxCounterValue > 0 {
							delta = (df.maxCounterValue - prevVal) + currVal
						} else {
							delta = currVal
						}
						converted, err := dataType.FromFloat64(delta)
						if err != nil {
							return nil, fmt.Errorf("failed to convert delta from float64: %w", err)
						}
						return &timeseries.TsRecord[any]{
							Value:     converted,
							Timestamp: item.Timestamp,
						}, nil
					}
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
