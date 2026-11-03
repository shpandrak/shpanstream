package stream

// Untyped returns the stream with its elements boxed as `any`, keeping the lifecycle untouched.
// It is deliberately not written as Map(s, func(v T) any { ... }): that would instantiate the whole
// Map machinery for a [T, any] pair per element type (see docs/compile-size-per-element-type.md).
// boxProvider is the one-line boxing closure, already compiled once per element type.
func (s Stream[T]) Untyped() Stream[any] {
	return newStream(ProviderFunc[any](boxProvider(s.provider)), s.allLifecycleElement)
}
