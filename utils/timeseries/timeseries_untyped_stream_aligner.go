package timeseries

import (
	"context"
	"errors"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"time"
)

// AlignStreamUntyped aligns a stream of sorted timeseries numeric records to a fixed duration.
// this can be used to take a dense timeseries stream and produce a stream with fewer data points
// aligned points use the time-weighted average to calculate the value of the aligned point
func AlignStreamUntyped(
	s stream.Stream[TsRecord[[]any]],
	alignmentPeriod AlignmentPeriod,
) stream.Stream[TsRecord[[]any]] {
	// Using ClusterSortedStreamComparable to group the items by the duration slot
	return stream.ClusterSortedStreamComparable[TsRecord[[]any], TsRecord[[]any], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream stream.Stream[TsRecord[[]any]],
			lastItemOnPreviousCluster *TsRecord[[]any],
		) (TsRecord[[]any], error) {

			firstItem, found, err := clusterStream.FindFirst(ctx)
			if err != nil {
				return util.DefaultValue[TsRecord[[]any]](), err
			}
			if !found {
				// A cluster always holds at least one element; this is defensive.
				return util.DefaultValue[TsRecord[[]any]](), errors.New("no \"first element\" in an empty stream")
			}

			// If this is the first cluster, smudge the first item to the start of the cluster
			if lastItemOnPreviousCluster == nil {
				return TsRecord[[]any]{
					Value:     firstItem.Value,
					Timestamp: clusterTimestampClassifier,
				}, nil
			} else {
				// If this is not the first cluster

				// Check if  the first item is magically aligned to the slot, return it
				if firstItem.Timestamp == clusterTimestampClassifier {
					return TsRecord[[]any]{
						Value:     firstItem.Value,
						Timestamp: clusterTimestampClassifier,
					}, nil
				} else {
					// If not, we need to calculate the time weighted average
					avgItem, err := timeWeightedAverageArr(
						clusterTimestampClassifier,
						lastItemOnPreviousCluster.Timestamp,
						lastItemOnPreviousCluster.Value,
						firstItem.Timestamp,
						firstItem.Value,
					)
					if err != nil {
						return util.DefaultValue[TsRecord[[]any]](), fmt.Errorf("error calculating time weighted average while aliging streams: %w", err)
					}
					return TsRecord[[]any]{
						Value:     avgItem,
						Timestamp: clusterTimestampClassifier,
					}, nil
				}
			}
		},
		AlignmentPeriodClassifierFunc[[]any](alignmentPeriod),
		s,
	)
}

// timeWeightedAverageArr computes the time-weighted average of two values (v1Arr and v2Arr) erroring if the values are not numeric
func timeWeightedAverageArr(targetTime, v1Time time.Time, v1Arr []any, v2Time time.Time, v2Arr []any) ([]any, error) {
	if v1Time.Equal(v2Time) {
		if v1Time == targetTime {
			return v1Arr, nil
		}
		return nil, fmt.Errorf("v1Time and v2Time are the same: %s. targetTime:%s", v1Time, targetTime)
	}

	// Ensure targetTime is between v1Time and v2Time
	if targetTime.Before(v1Time) || targetTime.After(v2Time) {
		return nil, fmt.Errorf("targetTime %s is out of bounds (%s to %s)", targetTime, v1Time, v2Time)
	}

	// Calculate the interpolation factor
	totalDuration := v2Time.Sub(v1Time).Seconds()
	interpolatedDuration := targetTime.Sub(v1Time).Seconds()
	weight := interpolatedDuration / totalDuration

	res := make([]any, len(v1Arr))
	for i := range v1Arr {
		v1, err := util.AnyToFloat64(v1Arr[i])
		if err != nil {
			return nil, err
		}
		v2, err := util.AnyToFloat64(v2Arr[i])
		if err != nil {
			return nil, err
		}
		// Perform the weighted average
		res[i] = v1 + (v2-v1)*weight
	}

	return res, nil
}
