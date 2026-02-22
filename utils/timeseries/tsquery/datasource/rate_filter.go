package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Filter = RateFilter{}

type RateFilter struct {
	overrideUnit    string
	perSeconds      int
	nonNegative     bool
	maxCounterValue float64
}

func NewRateFilter(overrideUnit string, perSeconds int, nonNegative bool, maxCounterValue float64) RateFilter {
	return RateFilter{
		overrideUnit:    overrideUnit,
		perSeconds:      perSeconds,
		nonNegative:     nonNegative,
		maxCounterValue: maxCounterValue,
	}
}

func (rf RateFilter) Filter(_ context.Context, result Result) (Result, error) {
	dataType := result.meta.DataType()
	if !dataType.IsNumeric() {
		return util.DefaultValue[Result](), fmt.Errorf(
			"rate filter can only be applied to numeric data types, got: %s",
			dataType,
		)
	}
	if !result.meta.Required() {
		return util.DefaultValue[Result](), fmt.Errorf(
			"rate filter can only be applied to required fields (no nils allowed)",
		)
	}

	newMeta, err := tsquery.NewFieldMetaWithCustomData(
		result.meta.Urn(),
		tsquery.DataTypeDecimal,
		true,
		rf.overrideUnit,
		result.meta.CustomMeta(),
	)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to create new field meta: %w", err)
	}

	perSeconds := rf.perSeconds
	if perSeconds <= 0 {
		perSeconds = 1
	}

	var computeDelta counterDeltaFunc
	if rf.nonNegative {
		computeDelta = newNonNegativeCounterDeltaFunc(rf.maxCounterValue)
	} else {
		computeDelta = func(currVal, prevVal float64) (float64, bool) {
			return currVal - prevVal, true
		}
	}

	var prevItem *timeseries.TsRecord[any]
	return Result{
		meta: *newMeta,
		data: stream.MapWhileFilteringWithErr(
			result.data,
			func(item timeseries.TsRecord[any]) (*timeseries.TsRecord[any], error) {
				// Skipping the first item, just storing the reference
				if prevItem == nil {
					prevItem = &item
					return nil, nil
				}

				currVal, err := dataType.ToFloat64(item.Value)
				if err != nil {
					return nil, fmt.Errorf("failed to convert current value to float64: %w", err)
				}
				prevVal, err := dataType.ToFloat64(prevItem.Value)
				if err != nil {
					return nil, fmt.Errorf("failed to convert previous value to float64: %w", err)
				}

				delta, emit := computeDelta(currVal, prevVal)
				if !emit {
					return nil, nil
				}

				timeDiff := item.Timestamp.Sub(prevItem.Timestamp).Seconds()
				if timeDiff == 0 {
					return nil, fmt.Errorf("time difference is zero between %s and %s", prevItem.Timestamp, item.Timestamp)
				}

				prevItem = &item
				return &timeseries.TsRecord[any]{
					Value:     (delta / timeDiff) * float64(perSeconds),
					Timestamp: item.Timestamp,
				}, nil
			},
		),
	}, nil
}
