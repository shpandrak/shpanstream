package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

var _ Filter = AlignerFilter{}

type AlignerFilter struct {
	alignmentPeriod timeseries.AlignmentPeriod
	fillMode        *timeseries.FillMode
}

func NewAlignerFilter(alignmentPeriod timeseries.AlignmentPeriod) AlignerFilter {
	return AlignerFilter{alignmentPeriod: alignmentPeriod}
}

func NewInterpolatingAlignerFilter(alignmentPeriod timeseries.AlignmentPeriod, fillMode timeseries.FillMode) AlignerFilter {
	return AlignerFilter{alignmentPeriod: alignmentPeriod, fillMode: &fillMode}
}

func (af AlignerFilter) Filter(_ context.Context, result Result) (Result, error) {
	if !result.meta.DataType().IsNumeric() {
		return util.DefaultValue[Result](), fmt.Errorf(
			"aligner filter can only be applied to numeric data types, got: %s",
			result.meta.DataType(),
		)
	}
	// Using ClusterSortedStreamComparable to group the items by the duration slot
	alignedStream := stream.ClusterSortedStreamComparable[timeseries.TsRecord[any], timeseries.TsRecord[any], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream stream.Stream[timeseries.TsRecord[any]],
			lastItemOnPreviousCluster *timeseries.TsRecord[any],
		) (timeseries.TsRecord[any], error) {

			firstItem, err := clusterStream.FindFirst().Get(ctx)
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[any]](), err
			}

			// If this is the first cluster, smudge the first item to the start of the cluster
			if lastItemOnPreviousCluster == nil {
				return timeseries.TsRecord[any]{
					Value:     firstItem.Value,
					Timestamp: clusterTimestampClassifier,
				}, nil
			} else {
				// If this is not the first cluster

				// Check if the first item is magically aligned to the slot, return it
				if firstItem.Timestamp == clusterTimestampClassifier {
					return timeseries.TsRecord[any]{
						Value:     firstItem.Value,
						Timestamp: clusterTimestampClassifier,
					}, nil
				} else {
					// If not, we need to calculate the time-weighted average
					avgItem, err := timeWeightedAverage(
						result.meta,
						clusterTimestampClassifier,
						lastItemOnPreviousCluster.Timestamp,
						lastItemOnPreviousCluster.Value,
						firstItem.Timestamp,
						firstItem.Value,
					)
					if err != nil {
						return util.DefaultValue[timeseries.TsRecord[any]](), fmt.Errorf("error calculating time weighted average while aliging streams: %w", err)
					}
					return timeseries.TsRecord[any]{
						Value:     avgItem,
						Timestamp: clusterTimestampClassifier,
					}, nil
				}
			}
		},
		alignmentPeriodClassifierFunc[any](af.alignmentPeriod),
		result.data,
	)

	// If fillMode is set, wrap the sparse aligned stream with a gap-filler
	if af.fillMode != nil {
		fieldMeta := result.meta
		alignedStream = timeseries.NewTsGapFillerStream[any](
			alignedStream,
			af.alignmentPeriod,
			*af.fillMode,
			func(targetTime, v1Time time.Time, v1 any, v2Time time.Time, v2 any) (any, error) {
				return timeWeightedAverage(fieldMeta, targetTime, v1Time, v1, v2Time, v2)
			},
			func(v any) any { return v },
		)
	}

	return Result{
		meta: result.meta,
		data: alignedStream,
	}, nil

}

func alignmentPeriodClassifierFunc[T any](ap timeseries.AlignmentPeriod) func(a timeseries.TsRecord[T]) time.Time {
	return func(a timeseries.TsRecord[T]) time.Time { return ap.GetStartTime(a.Timestamp) }
}

// timeWeightedAverageArr computes the time-weighted average of two values (v1Arr and v2Arr) erroring if the values are not numeric
func timeWeightedAverage(fieldMeta tsquery.FieldMeta, targetTime, v1Time time.Time, v1 any, v2Time time.Time, v2 any) (any, error) {
	if v1Time.Equal(v2Time) {
		if v1Time == targetTime {
			return v1, nil
		}
		return nil, fmt.Errorf("v1Time and v2Time are the same: %s. targetTime:%s", v1Time, targetTime)
	}

	// Ensure the targetTime is between v1Time and v2Time
	if targetTime.Before(v1Time) || targetTime.After(v2Time) {
		return nil, fmt.Errorf("targetTime %s is out of bounds (%s to %s)", targetTime, v1Time, v2Time)
	}

	// Calculate the interpolation factor
	totalDuration := v2Time.Sub(v1Time).Seconds()
	interpolatedDuration := targetTime.Sub(v1Time).Seconds()
	weight := interpolatedDuration / totalDuration

	dt := fieldMeta.DataType()
	v1Float, err := dt.ToFloat64(v1)
	if err != nil {
		return nil, fmt.Errorf("error converting v1 to float64 for weighted average: %w", err)
	}
	v2Float, err := dt.ToFloat64(v2)
	if err != nil {
		return nil, fmt.Errorf("error converting v2 to float64 for weighted average: %w", err)
	}
	// Perform the weighted average
	retVal, err := dt.FromFloat64(v1Float + (v2Float-v1Float)*weight)
	if err != nil {
		return nil, fmt.Errorf("failed to convert cast weighted average value back to datatype values when calculating weighted average: %w", err)
	}
	return retVal, nil

}
