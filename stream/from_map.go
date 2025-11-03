package stream

import (
	"github.com/shpandrak/shpanstream"
	"maps"
)

// FromMapValues creates a stream from the values of the provided map.
func FromMapValues[K comparable, V any](mp map[K]V) Stream[V] {
	return FromIterator(maps.Values(mp))
}

// FromMapKeys creates a stream from the keys of the provided map.
func FromMapKeys[K comparable, V any](mp map[K]V) Stream[K] {
	return FromIterator(maps.Keys(mp))
}

// FromMapEntries creates a stream from the entries of the provided map.
func FromMapEntries[K comparable, V any](mp map[K]V) Stream[shpanstream.Entry[K, V]] {
	return FromIterator2[K, V](maps.All(mp))
}
