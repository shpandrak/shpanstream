package stream

func (s Stream[T]) Iterator(yield func(T) bool) {
	s.Filter(func(v T) bool {
		// Yield return false if we need to stop (e.g. break within the loop)
		return !yield(v)
	}).FindFirst().
		MustGetOptional()
}

func (s Stream[T]) IndexedIterator(yield func(int, T) bool) {
	// Use a counter to keep track of the index
	index := -1
	s.Filter(func(v T) bool {
		index++
		// Yield return false if we need to stop (e.g. break within the loop)
		return !yield(index, v)
	}).FindFirst().
		MustGetOptional()
}
