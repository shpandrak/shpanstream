package stream

func (s Stream[T]) Untyped() Stream[any] {
	return Map(s, func(v T) any {
		return v
	})
}
