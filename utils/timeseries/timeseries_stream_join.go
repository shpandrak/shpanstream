package timeseries

import (
	"github.com/shpandrak/shpanstream/stream"
	"time"
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

func InnerJoinStreams[S any, T any](
	s []stream.Stream[TsRecord[S]],
	joiner func(values []S) T,
) stream.Stream[TsRecord[T]] {
	if len(s) == 0 {
		return stream.Empty[TsRecord[T]]()
	}

	// Comparator that compares records based on their timestamp
	comparator := func(a, b TsRecord[S]) int {
		return a.Timestamp.Compare(b.Timestamp)
	}

	// Joiner that unwraps TsRecord values, calls the user's joiner, and wraps the result
	tsJoiner := func(records []TsRecord[S]) TsRecord[T] {
		// Extract values from all records (in inner join, all records are non-nil)
		values := make([]S, len(records))
		for i, record := range records {
			values[i] = record.Value
		}

		// Call the user's joiner
		result := joiner(values)

		// Return a TsRecord with the timestamp (all records have the same timestamp)
		return TsRecord[T]{
			Timestamp: records[0].Timestamp,
			Value:     result,
		}
	}

	return stream.JoinMultipleSortedStreams(s, comparator, tsJoiner)
}

func FullJoinStreams[S any, T any](
	s []stream.Stream[TsRecord[S]],
	joiner func(values []*S) T,
) stream.Stream[TsRecord[T]] {
	if len(s) == 0 {
		return stream.Empty[TsRecord[T]]()
	}

	// Comparator that compares records based on their timestamp
	comparator := func(a, b TsRecord[S]) int {
		return a.Timestamp.Compare(b.Timestamp)
	}

	// Joiner that unwraps TsRecord values, calls the user's joiner, and wraps the result
	tsJoiner := func(records []*TsRecord[S]) TsRecord[T] {
		// Extract value pointers from all streams (nil TsRecord becomes nil pointer)
		values := make([]*S, len(records))
		var timestamp time.Time
		timestampSet := false

		for i, record := range records {
			if record != nil {
				values[i] = &record.Value
				// Use the first non-nil timestamp
				if !timestampSet {
					timestamp = record.Timestamp
					timestampSet = true
				}
			}
			// If nil, values[i] remains nil
		}

		// Call the user's joiner
		result := joiner(values)

		// Return a TsRecord with the timestamp from the first non-nil record
		return TsRecord[T]{
			Timestamp: timestamp,
			Value:     result,
		}
	}

	return stream.FullJoinMultipleSortedStreams(s, comparator, tsJoiner)
}
