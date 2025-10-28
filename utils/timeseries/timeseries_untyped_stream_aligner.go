package timeseries

import (
	"context"
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

			firstItem, err := clusterStream.FindFirst().Get(ctx)
			if err != nil {
				return util.DefaultValue[TsRecord[[]any]](), err
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
		alignmentPeriodClassifierFunc[[]any](alignmentPeriod),
		s,
	)
}
