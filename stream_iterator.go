package shpanstream

func (s Stream[T]) Iterator(yield func(T) bool) {
	s.Filter(func(v T) bool {
		// Yield return false if we need to stop (e.g. break within the loop)
		return !yield(v)
	}).FindFirst().
		MustGetOptional()
}
