package timeseries

import (
	"context"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"time"
)

// AlignStream aligns a stream of sorted timeseries numeric records to a fixed duration.
// this can be used to take a dense timeseries stream and produce a stream with fewer data points
// aligned points use the time weighted average to calculate the value of the aligned point
func AlignStream[N Number](
	s shpanstream.Stream[TsRecord[N]],
	alignmentPeriod AlignmentPeriod,
) shpanstream.Stream[TsRecord[N]] {
	// Using ClusterSortedStreamComparable to group the items by the duration slot
	return shpanstream.ClusterSortedStreamComparable[TsRecord[N], TsRecord[N], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream shpanstream.Stream[TsRecord[N]],
			lastItemOnPreviousCluster *TsRecord[N],
		) (TsRecord[N], error) {

			firstItem, err := clusterStream.FindFirst().Get(ctx)
			if err != nil {
				return util.DefaultValue[TsRecord[N]](), err
			}

			// If this is the first cluster, smudge the first item to the start of the cluster
			if lastItemOnPreviousCluster == nil {
				return TsRecord[N]{
					Value:     firstItem.Value,
					Timestamp: clusterTimestampClassifier,
				}, nil
			} else {
				// If this is not the first cluster

				// Check if  the first item is magically aligned to the slot, return it
				if firstItem.Timestamp == clusterTimestampClassifier {
					return TsRecord[N]{
						Value:     firstItem.Value,
						Timestamp: clusterTimestampClassifier,
					}, nil
				} else {
					// If not, we need to calculate the time weighted average
					avgItem, err := timeWeightedAverage[N](
						clusterTimestampClassifier,
						lastItemOnPreviousCluster.Timestamp,
						lastItemOnPreviousCluster.Value,
						firstItem.Timestamp,
						firstItem.Value,
					)
					if err != nil {
						return util.DefaultValue[TsRecord[N]](), err
					}
					return TsRecord[N]{
						Value:     avgItem,
						Timestamp: clusterTimestampClassifier,
					}, nil
				}
			}
		},
		alignmentPeriodClassifierFunc[N](alignmentPeriod),
		s,
	)
}
