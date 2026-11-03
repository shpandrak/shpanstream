package stream

import (
	"context"
	"io"

	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/lazy"
)

type Stream[T any] struct {
	provider            ProviderFunc[T]
	allLifecycleElement []Lifecycle
}

func NewStream[T any](provider Provider[T]) Stream[T] {
	return newStream(provider.Emit, []Lifecycle{provider})
}

func newStream[T any](streamProviderFunc ProviderFunc[T], allLifecycleElement []Lifecycle) Stream[T] {
	return Stream[T]{provider: streamProviderFunc, allLifecycleElement: allLifecycleElement}
}

type CreateStreamOption struct {
	openFunc  func(ctx context.Context) error
	closeFunc func()
}

func WithOpenFuncOption(openFunc func(ctx context.Context) error) CreateStreamOption {
	return CreateStreamOption{openFunc: openFunc}
}

func WithCloseFuncOption(closeFunc func()) CreateStreamOption {
	return CreateStreamOption{closeFunc: closeFunc}
}

func NewSimpleStream[T any](streamProviderFunc ProviderFunc[T], options ...CreateStreamOption) Stream[T] {
	return Stream[T]{provider: streamProviderFunc, allLifecycleElement: lifecyclesFromOptions(options)}
}

type ProviderFunc[T any] func(ctx context.Context) (T, error)

// Consume consumes the entire stream and applies the provided function to each element (sometimes named ForEach)
// It returns an error if the stream materialization fails in any stage of the pipeline
// For empty streams, it returns immediately with no error
// For infinite streams, it will block until the stream either ctx is cancelled, stream is done or an error occurs
//
//go:noinline
func (s Stream[T]) Consume(ctx context.Context, f func(T), options ...ConsumeOption) error {
	if len(options) > 0 {
		return s.ConsumeWithErr(ctx, func(v T) error {
			f(v)
			return nil
		}, options...)
	}
	return consumeLoop(ctx, s.allLifecycleElement, func(ctx context.Context) (bool, error) {
		v, err := s.provider(ctx)
		if err != nil {
			return false, err
		}
		f(v)
		return true, nil
	})
}

// MustConsume is a convenience method that panics if the stream errors
func (s Stream[T]) MustConsume(f func(T)) {
	err := s.Consume(context.Background(), f)
	if err != nil {
		panic(err)
	}
}

// ConsumeWithErr consumes the entire stream and applies the provided function to each element (sometimes named ForEach).
// Allow returning an error from the function to stop the pipeline
// It returns an error if the stream materialization fails in any stage of the pipeline
//
//go:noinline
func (s Stream[T]) ConsumeWithErr(ctx context.Context, f func(T) error, options ...ConsumeOption) error {
	if len(options) > 0 {
		return s.ConsumeWithErrAndCtx(ctx, func(_ context.Context, v T) error {
			return f(v)
		}, options...)
	}
	return consumeLoop(ctx, s.allLifecycleElement, func(ctx context.Context) (bool, error) {
		v, err := s.provider(ctx)
		if err != nil {
			return false, err
		}
		return true, f(v)
	})
}

// ConsumeWithErrAndCtx consumes the entire stream and applies the provided function to each element (sometimes named ForEach).
// Allow returning an error from the function to stop the pipeline,
// passing through the context allowing the function to gracefully cancel
// It returns an error if the stream materialization fails in any stage of the pipeline
//
//go:noinline
func (s Stream[T]) ConsumeWithErrAndCtx(ctx context.Context, f func(ctx context.Context, value T) error, options ...ConsumeOption) error {
	// Check for the concurrent option
	if concurrency, concurrent := concurrentConsumeOptionOf(options); concurrent {
		return consumeConcurrently(ctx, concurrency, s.allLifecycleElement, boxProvider(s.provider), boxConsumer(f))
	}
	return consumeLoop(ctx, s.allLifecycleElement, func(ctx context.Context) (bool, error) {
		v, err := s.provider(ctx)
		if err != nil {
			return false, err
		}
		return true, f(ctx, v)
	})
}

//go:noinline
func (s Stream[T]) FindFirst() lazy.Lazy[T] {
	return lazy.NewLazyOptional[T](func(ctx context.Context) (*T, error) {
		itemArr, err := s.Limit(1).Collect(ctx)
		if err != nil {
			return nil, err
		}
		if len(itemArr) > 0 {
			// If we have at least one item, we return the first one
			return &itemArr[0], nil
		}
		// If we reach here, it means the stream is empty, so we need to return nil
		return nil, nil

	}).OrElseThrow(errNoFirstElement)
}

//go:noinline
func (s Stream[T]) FindLast() lazy.Lazy[T] {
	return lazy.NewLazyOptional[T](func(ctx context.Context) (*T, error) {
		var result *T
		err := s.Consume(ctx, func(v T) {
			result = &v
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	}).OrElseThrow(errNoLastElement)

}

// FindFirstAndLast returns the first and last element of the stream, unless the stream is empty
// if the stream contains only one element, the first and last element are the same
func FindFirstAndLast[T any](s Stream[T]) lazy.Lazy[shpanstream.Tuple2[T, T]] {
	return lazy.NewLazyOptional[shpanstream.Tuple2[T, T]](func(ctx context.Context) (*shpanstream.Tuple2[T, T], error) {
		var first *T
		var last *T
		err := s.Consume(ctx, func(v T) {
			if first == nil {
				first = &v
			}
			last = &v
		})
		if err != nil {
			return nil, err
		}
		if first == nil {
			return nil, nil
		}
		return &shpanstream.Tuple2[T, T]{
			A: *first,
			B: *last,
		}, nil
	}).OrElseThrow(errNoFirstAndLastElement)

}

// Collect materializes the stream, and collects all elements of the stream into a slice
// It returns an error if the stream materialization fails in any stage of the pipeline
//
//go:noinline
func (s Stream[T]) Collect(ctx context.Context) ([]T, error) {
	var result []T
	err := s.Consume(ctx, func(v T) {
		result = append(result, v)
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MustCollect is a convenience method that panics if the stream errors
// should be used for testing purpose or when streams are static (e.g. slice sourced streams)
func (s Stream[T]) MustCollect() []T {
	result, err := s.Collect(context.Background())
	if err != nil {
		panic(err)
	}
	return result
}

func (s Stream[T]) Filter(predicate shpanstream.Predicate[T]) Stream[T] {
	return s.FilterWithErAndCtx(predicate.ToErrCtx())
}

func (s Stream[T]) FilterWithErr(predicate shpanstream.PredicateWithErr[T]) Stream[T] {
	return s.FilterWithErAndCtx(predicate.ToErrCtx())
}

//go:noinline
func (s Stream[T]) FilterWithErAndCtx(predicate shpanstream.PredicateWithErrAndCtx[T]) Stream[T] {
	return newStream[T](func(ctx context.Context) (T, error) {
		for {
			v, err := s.provider(ctx)
			if err != nil {
				return v, err
			}
			shouldKeep, err := predicate(ctx, v)
			if err != nil {
				// Wrapping errors, e.g. we don't want EOF accidentally returned from here
				return util.DefaultValue[T](), wrapFilterError(err)
			}
			if shouldKeep {
				return v, nil
			}
		}
	}, s.allLifecycleElement)
}

// Count counts the number of elements in the stream (materializes the stream)
//
//go:noinline
func (s Stream[T]) Count(ctx context.Context) (int, error) {
	count := 0
	err := s.Consume(ctx, func(v T) {
		count++
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}

// MustCount is a convenience method that panics if the stream errors.
// Should be used for testing purpose or when streams are static (e.g. slice sourced streams)
func (s Stream[T]) MustCount() int {
	count, err := s.Count(context.Background())
	if err != nil {
		panic(err)
	}
	return count
}

func (s Stream[T]) IsEmpty(ctx context.Context) (bool, error) {
	return s.FindFirst().IsEmpty(ctx)
}

func (s Stream[T]) WithAdditionalLifecycle(lch Lifecycle) Stream[T] {
	return newStream(s.provider, appendLifecycle(s.allLifecycleElement, lch))
}

// Peek allows to peek at the elements of the stream without consuming them
// Peek will not materialize the stream, and will be invoked only (and if) the stream is materialized
//
//go:noinline
func (s Stream[T]) Peek(f func(v T)) Stream[T] {
	// Written directly rather than as Map(s, ...) so a Stream shape does not instantiate the whole
	// Map machinery for its own [T, T] pair.
	return newStream[T](func(ctx context.Context) (T, error) {
		v, err := s.provider(ctx)
		if err != nil {
			return util.DefaultValue[T](), err
		}
		f(v)
		return v, nil
	}, s.allLifecycleElement)
}

// FromLazy converts the Lazy to a Stream (or either a single element, empty stream, or an error stream)
func FromLazy[T any](l lazy.Lazy[T]) Stream[T] {
	alreadyFetched := false
	return NewSimpleStream(func(ctx context.Context) (T, error) {
		if alreadyFetched {
			return util.DefaultValue[T](), io.EOF
		} else {
			alreadyFetched = true
		}

		lazyValue, err := l.GetOptional(ctx)
		if err != nil {
			return util.DefaultValue[T](), err
		}
		if lazyValue == nil {
			return util.DefaultValue[T](), io.EOF
		}
		return *lazyValue, nil
	})
}
