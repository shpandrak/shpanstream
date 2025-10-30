package timeseries

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"time"
)

// AlignReduceStream aligns a stream of sorted timeseries numeric records to a fixed duration.
// Reduce items in the same duration slot using the provided reducer.
func AlignReduceStream[N Number](
	s stream.Stream[TsRecord[N]],
	alignmentPeriod AlignmentPeriod,
	reducer Reducer[N],
) stream.Stream[TsRecord[N]] {

	// Using ClusterSortedStreamComparable to group the items by the duration slot
	return stream.ClusterSortedStreamComparable[TsRecord[N], TsRecord[N], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream stream.Stream[TsRecord[N]],
			_ *TsRecord[N],
		) (TsRecord[N], error) {
			return reducer(clusterTimestampClassifier, clusterStream).Get(ctx)
		},
		AlignmentPeriodClassifierFunc[N](alignmentPeriod),
		s,
	)
}
