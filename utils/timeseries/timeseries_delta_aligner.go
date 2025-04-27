package timeseries

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"time"
)

// AlignDeltaStream aligns a stream of sorted timeseries numeric records to a provided alignment period.
// and calculates the delta between the current and previous record.
func AlignDeltaStream[N Number](s shpanstream.Stream[TsRecord[N]], ap AlignmentPeriod) shpanstream.Stream[TsRecord[N]] {
	var globalFirstItem *TsRecord[N]
	var globalLastItem *TsRecord[N]

	// Using ClusterSortedStreamComparable to group the items by the duration slot
	alignedStream := shpanstream.ClusterSortedStreamComparable[TsRecord[N], TsRecord[N], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream shpanstream.Stream[TsRecord[N]],
			lastItemOnPreviousCluster *TsRecord[N],
		) (TsRecord[N], error) {
			var localFirstItem *TsRecord[N]
			err := clusterStream.Consume(ctx, func(t TsRecord[N]) {
				if localFirstItem == nil {
					localFirstItem = &t
				}
				globalLastItem = &t
			})
			if err != nil {
				return util.DefaultValue[TsRecord[N]](), err
			}
			if localFirstItem == nil {
				return util.DefaultValue[TsRecord[N]](), fmt.Errorf("cluster stream for cluster %s is empty", clusterTimestampClassifier)
			}
			if globalFirstItem == nil {
				globalFirstItem = localFirstItem
			}

			// If this is the first cluster, smudge the first item to the start of the cluster
			if lastItemOnPreviousCluster == nil {
				return TsRecord[N]{
					Value:     localFirstItem.Value,
					Timestamp: clusterTimestampClassifier,
				}, nil
			} else {
				// If this is not the first cluster

				// Check if  the first item is magically aligned to the slot, return it
				if localFirstItem.Timestamp == clusterTimestampClassifier {
					return TsRecord[N]{
						Value:     localFirstItem.Value,
						Timestamp: clusterTimestampClassifier,
					}, nil
				} else {
					// If not, we need to calculate the time weighted average
					avgItem, err := timeWeightedAverage[N](
						clusterTimestampClassifier,
						lastItemOnPreviousCluster.Timestamp,
						lastItemOnPreviousCluster.Value,
						localFirstItem.Timestamp,
						localFirstItem.Value,
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
		alignmentPeriodClassifierFunc[N](ap),
		s,
	)

	// Now, because with delta we want to add the last slot as it might be "ongoing" and we need to add the last item
	// to the aligned stream, so that delta will be calculated correctly for that last slot
	alignedStream = shpanstream.ConcatStreams(alignedStream, shpanstream.NewLazyOptional(func(ctx context.Context) (*TsRecord[N], error) {

		// Unless the last item magically aligns to a slot, append it to the stream
		if globalLastItem != nil &&
			globalLastItem.Timestamp != ap.GetStartTime(globalLastItem.Timestamp) &&
			// This handles the case of a single item stream, we should emit nothing...
			globalLastItem.Timestamp != globalFirstItem.Timestamp {
			// If the last item is not aligned to the slot, we add it so it will be counted in delta
			globalLastItem.Timestamp = ap.GetEndTime(globalLastItem.Timestamp)
			return globalLastItem, nil
		}
		return nil, nil
	}).AsStream())
	return DeltaStream(alignedStream)
}
