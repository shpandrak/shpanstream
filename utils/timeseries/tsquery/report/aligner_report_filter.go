package report

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
}

func NewAlignerFilter(alignmentPeriod timeseries.AlignmentPeriod) AlignerFilter {
	return AlignerFilter{alignmentPeriod: alignmentPeriod}
}

func (af AlignerFilter) Filter(ctx context.Context, result Result) (Result, error) {
	// Using ClusterSortedStreamComparable to group the items by the duration slot
	fieldsMeta := result.FieldsMeta()
	s := stream.ClusterSortedStreamComparable[timeseries.TsRecord[[]any], timeseries.TsRecord[[]any], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream stream.Stream[timeseries.TsRecord[[]any]],
			lastItemOnPreviousCluster *timeseries.TsRecord[[]any],
		) (timeseries.TsRecord[[]any], error) {

			firstItem, err := clusterStream.FindFirst().Get(ctx)
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[[]any]](), err
			}

			// If this is the first cluster, smudge the first item to the start of the cluster
			if lastItemOnPreviousCluster == nil {
				return timeseries.TsRecord[[]any]{
					Value:     firstItem.Value,
					Timestamp: clusterTimestampClassifier,
				}, nil
			} else {
				// If this is not the first cluster

				// Check if the first item is magically aligned to the slot, return it
				if firstItem.Timestamp == clusterTimestampClassifier {
					return timeseries.TsRecord[[]any]{
						Value:     firstItem.Value,
						Timestamp: clusterTimestampClassifier,
					}, nil
				} else {
					// If not, we need to calculate the time-weighted average
					avgItem, err := timeWeightedAverageArr(
						fieldsMeta,
						clusterTimestampClassifier,
						lastItemOnPreviousCluster.Timestamp,
						lastItemOnPreviousCluster.Value,
						firstItem.Timestamp,
						firstItem.Value,
					)
					if err != nil {
						return util.DefaultValue[timeseries.TsRecord[[]any]](), fmt.Errorf("error calculating time weighted average while aliging streams: %w", err)
					}
					return timeseries.TsRecord[[]any]{
						Value:     avgItem,
						Timestamp: clusterTimestampClassifier,
					}, nil
				}
			}
		},
		recordAlignmentPeriodClassifierFunc(af.alignmentPeriod),
		result.Stream(),
	)

	return NewResult(
		fieldsMeta,
		s,
	), nil

}

func recordAlignmentPeriodClassifierFunc(ap timeseries.AlignmentPeriod) func(a timeseries.TsRecord[[]any]) time.Time {
	return func(a timeseries.TsRecord[[]any]) time.Time { return ap.GetStartTime(a.Timestamp) }
}

// timeWeightedAverageArr computes the time-weighted average of two values (v1Arr and v2Arr) erroring if the values are not numeric
func timeWeightedAverageArr(fieldsMeta []tsquery.FieldMeta, targetTime, v1Time time.Time, v1Arr []any, v2Time time.Time, v2Arr []any) ([]any, error) {
	if v1Time.Equal(v2Time) {
		if v1Time == targetTime {
			return v1Arr, nil
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

	res := make([]any, len(v1Arr))
	for i := range v1Arr {
		dt := fieldsMeta[i].DataType()
		v1, err := dt.ToFloat64(v1Arr[i])
		if err != nil {
			return nil, fmt.Errorf("error converting v1 array to float64 for weighted array: %w", err)
		}
		v2, err := dt.ToFloat64(v2Arr[i])
		if err != nil {
			return nil, fmt.Errorf("error converting v2 array to float64 for weighted array: %w", err)
		}
		// Perform the weighted average
		res[i], err = dt.FromFloat64(v1 + (v2-v1)*weight)
		if err != nil {
			return nil, fmt.Errorf("failed to convert cast weighted average value back to datatype values: %w", err)
		}
	}

	return res, nil
}
