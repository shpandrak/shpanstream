package timeseries

import (
	"context"
	"github.com/shpandrak/shpanstream"
	"time"
)

// AlignReduceStream aligns a stream of sorted timeseries numeric records to a fixed duration.
// Reduce items in the same duration slot using the provided reducer.
func AlignReduceStream[N Number](
	s shpanstream.Stream[TsRecord[N]],
	alignmentPeriod AlignmentPeriod,
	reducer Reducer[N],
) shpanstream.Stream[TsRecord[N]] {

	// Using ClusterSortedStreamComparable to group the items by the duration slot
	return shpanstream.ClusterSortedStreamComparable[TsRecord[N], TsRecord[N], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream shpanstream.Stream[TsRecord[N]],
			_ *TsRecord[N],
		) (TsRecord[N], error) {
			return reducer(clusterTimestampClassifier, clusterStream).Get(ctx)
		},
		alignmentPeriodClassifierFunc[N](alignmentPeriod),
		s,
	)
}
