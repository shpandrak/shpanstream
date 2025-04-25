package timeseries

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"time"
)

// AlignReduceStream aligns a stream of sorted timeseries numeric records to a fixed duration.
// Reduce items in the same duration slot using the provided reducer.
func AlignReduceStream[N Number](
	s shpanstream.Stream[TsRecord[N]],
	fixedDuration time.Duration,
	reducer Reducer[N],
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
			_ *TsRecord[N],
		) (TsRecord[N], error) {
			return reducer(time.UnixMilli(clusterClassifier), clusterStream).Get(ctx)
		},
		func(a *TsRecord[N]) int64 {
			return a.Timestamp.Truncate(fixedDuration).UnixMilli()
		},
		s,
	)
}
