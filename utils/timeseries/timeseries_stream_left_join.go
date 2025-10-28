package timeseries

import (
	"github.com/shpandrak/shpanstream/stream"
)

func LeftJoinStreams[S any, T any](
	s []stream.Stream[TsRecord[S]],
	joiner func(left S, others []*S) T,
) stream.Stream[TsRecord[T]] {
	if len(s) == 0 {
		return stream.Empty[TsRecord[T]]()
	}

	// Comparator that compares records based on their timestamp
	comparator := func(a, b TsRecord[S]) int {
		return a.Timestamp.Compare(b.Timestamp)
	}

	// Joiner that unwraps TsRecord values, calls the user's joiner, and wraps the result
	tsJoiner := func(left TsRecord[S], others []*TsRecord[S]) TsRecord[T] {
		// Extract value pointers from other streams (nil TsRecord becomes nil pointer)
		otherValues := make([]*S, len(others))
		for i, other := range others {
			if other != nil {
				otherValues[i] = &other.Value
			}
			// If nil, otherValues[i] remains nil
		}

		// Call the user's joiner
		result := joiner(left.Value, otherValues)

		// Return a TsRecord with the left's timestamp
		return TsRecord[T]{
			Timestamp: left.Timestamp,
			Value:     result,
		}
	}

	return stream.LeftJoinMultipleSortedStreams(s, comparator, tsJoiner)
}
