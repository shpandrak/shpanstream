package timeseries

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"time"
)

// AlignStream aligns a stream of sorted timeseries numeric records to a fixed duration.
// this can be used to take a dense timeseries stream and produce a stream with fewer data points
// aligned points use the time weighted average to calculate the value of the aligned point
func AlignStream[N Number](
	s shpanstream.Stream[TsRecord[N]],
	fixedDuration time.Duration,
) shpanstream.Stream[TsRecord[N]] {
	// Check if the fixed duration is valid
	if fixedDuration <= 0 {
		return shpanstream.ErrorStream[TsRecord[N]](
			fmt.Errorf("invalid fixed duration for alignment of timeseries stream: %s", fixedDuration),
		)
	}

	// Using ClusterSortedStream to group the items by the duration slot
	return shpanstream.ClusterSortedStream[TsRecord[N], TsRecord[N], int64](
		func(
			ctx context.Context,
			clusterClassifier int64,
			clusterStream shpanstream.Stream[TsRecord[N]],
			lastItemOnPreviousCluster *TsRecord[N],
		) (TsRecord[N], error) {

			clusterTimestamp := time.UnixMilli(clusterClassifier)
			firstItem, err := clusterStream.FindFirst().Get(ctx)
			if err != nil {
				return defaultValue[TsRecord[N]](), err
			}

			// If this is the first cluster, smudge the first item to the start of the cluster
			if lastItemOnPreviousCluster == nil {
				return TsRecord[N]{
					Value:     firstItem.Value,
					Timestamp: clusterTimestamp,
				}, nil
			} else {
				// If this is not the first cluster

				// Check if  the first item is magically aligned to the slot, return it
				if firstItem.Timestamp == clusterTimestamp {
					return TsRecord[N]{
						Value:     firstItem.Value,
						Timestamp: clusterTimestamp,
					}, nil
				} else {
					// If not, we need to calculate the time weighted average
					avgItem, err := timeWeightedAverage[N](
						clusterTimestamp,
						lastItemOnPreviousCluster.Timestamp,
						lastItemOnPreviousCluster.Value,
						firstItem.Timestamp,
						firstItem.Value,
					)
					if err != nil {
						return defaultValue[TsRecord[N]](), err
					}
					return TsRecord[N]{
						Value:     avgItem,
						Timestamp: clusterTimestamp,
					}, nil
				}
			}
		},
		func(a *TsRecord[N]) int64 {
			return a.Timestamp.Truncate(fixedDuration).UnixMilli()
		},
		s,
	)
}
