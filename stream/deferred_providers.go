package stream

import (
	"context"
	"io"

	"github.com/shpandrak/shpanstream/internal/util"
)

// The deferred sources below, together with FromSliceProvider, build a stream from a value that is
// not computed until the stream runs: each takes a function that is invoked only when the stream
// is materialized, once per materialization, with the materialization context. An error the
// function returns is the stream's error.
//
//	deferred slice          FromSliceProvider(func(ctx) ([]T, error))
//	deferred single value   FromValueProvider(func(ctx) (T, error))
//	deferred optional value FromOptionalProvider(func(ctx) (T, bool, error))
//	deferred stream         FromStreamProvider(func(ctx) (Stream[T], error))

// FromValueProvider creates a single-element Stream whose element is fetched by provider when the
// stream is materialized. If provider fails, the stream fails with its error.
func FromValueProvider[T any](provider func(ctx context.Context) (T, error)) Stream[T] {
	return NewStream(&optionalProviderStream[T]{provider: func(ctx context.Context) (T, bool, error) {
		v, err := provider(ctx)
		return v, true, err
	}})
}

// FromOptionalProvider creates a Stream of at most one element, fetched by provider when the
// stream is materialized: the stream emits the value when provider reports it present, and is
// empty when it reports it absent. If provider fails, the stream fails with its error. The
// provider shape matches Stream.FindFirst, so FromOptionalProvider(s.FindFirst) is a stream of s's
// first element.
func FromOptionalProvider[T any](provider func(ctx context.Context) (T, bool, error)) Stream[T] {
	return NewStream(&optionalProviderStream[T]{provider: provider})
}

// FromStreamProvider creates a Stream whose source stream is obtained from provider when the
// stream is materialized, then streamed through (opened, drained and closed with the outer
// stream). The provider runs when the stream is opened (or, inside a combinator, when the
// combinator reaches it), and its error reaches the consumer wrapped (match it with errors.Is).
func FromStreamProvider[T any](provider func(ctx context.Context) (Stream[T], error)) Stream[T] {
	return Concat(FromValueProvider(provider))
}

// optionalProviderStream emits the provider's value, if present, once per materialization. The
// provider is invoked from Emit (not Open) so its error reaches the consumer as is, like any
// element error.
type optionalProviderStream[T any] struct {
	provider func(ctx context.Context) (T, bool, error)
	done     bool
}

func (o *optionalProviderStream[T]) Open(_ context.Context) error {
	o.done = false
	return nil
}

func (o *optionalProviderStream[T]) Close() {
	o.done = true
}

func (o *optionalProviderStream[T]) Emit(ctx context.Context) (T, error) {
	if o.done {
		return util.DefaultValue[T](), io.EOF
	}
	o.done = true
	v, present, err := o.provider(ctx)
	if err != nil {
		return util.DefaultValue[T](), err
	}
	if !present {
		return util.DefaultValue[T](), io.EOF
	}
	return v, nil
}
