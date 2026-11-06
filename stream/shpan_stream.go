package stream

import (
	"context"
	"io"

	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
)

// Stream is a lazily evaluated sequence of T: nothing runs until a terminal operation
// materializes it, and every materialization opens the stream's resources, pulls its elements and
// closes the resources again.
//
// The operations that take a function are generic methods, so they chain: Filter, Map, Peek,
// Consume and the rest. Every other operation is a package function that takes the stream:
// terminal operations take ctx first (Collect(ctx, s), FindFirst(ctx, s), Count(ctx, s)),
// transforms take the stream first (Limit(s, n), Skip(s, n), Page(s, page, size)), and MustValues(s)
// returns an iter.Seq. Stream has no plain methods, so a program compiles only the operations it
// calls for each element type (see docs/stream-api-generic-methods.md).
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
func (s Stream[T]) Consume[F ~func(T)](ctx context.Context, f F, options ...ConsumeOption) error {
	return consume(s, ctx, (func(T))(f), options)
}

//go:noinline
func consume[T any](s Stream[T], ctx context.Context, f func(T), options []ConsumeOption) error {
	if len(options) > 0 {
		return consumeWithErrAndCtx(s, ctx, func(_ context.Context, v T) error {
			f(v)
			return nil
		}, options)
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

// MustConsume is Consume with context.Background(), panicking if the stream fails. Use it in tests
// or with static streams (e.g. slice sourced streams).
func (s Stream[T]) MustConsume[F ~func(T)](f F) {
	err := consume(s, context.Background(), (func(T))(f), nil)
	if err != nil {
		panic(err)
	}
}

// ConsumeWithErr consumes the entire stream and applies the provided function to each element (sometimes named ForEach).
// Allow returning an error from the function to stop the pipeline
// It returns an error if the stream materialization fails in any stage of the pipeline
func (s Stream[T]) ConsumeWithErr[F ~func(T) error](ctx context.Context, f F, options ...ConsumeOption) error {
	return consumeWithErr(s, ctx, (func(T) error)(f), options)
}

//go:noinline
func consumeWithErr[T any](s Stream[T], ctx context.Context, f func(T) error, options []ConsumeOption) error {
	if len(options) > 0 {
		return consumeWithErrAndCtx(s, ctx, func(_ context.Context, v T) error {
			return f(v)
		}, options)
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
func (s Stream[T]) ConsumeWithErrAndCtx[F ~func(ctx context.Context, value T) error](ctx context.Context, f F, options ...ConsumeOption) error {
	return consumeWithErrAndCtx(s, ctx, (func(context.Context, T) error)(f), options)
}

//go:noinline
func consumeWithErrAndCtx[T any](s Stream[T], ctx context.Context, f func(ctx context.Context, value T) error, options []ConsumeOption) error {
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

// FindFirst materializes the stream only up to its first element and returns it. found is false
// when the stream is empty, which is not an error; err reports a failure of the pipeline (including
// a failure to open it). The stream is closed as soon as the first element has been read: no
// further element is pulled from the upstream. Once that element has been read, a cancellation of
// ctx is no longer reported: FindFirst returns the element (a DoFinally inside the pipeline may
// still observe the cancellation as it is closed).
//
//go:noinline
func FindFirst[T any](ctx context.Context, s Stream[T]) (first T, found bool, err error) {
	err = consumeLoop(ctx, s.allLifecycleElement, func(ctx context.Context) (bool, error) {
		v, pullErr := s.provider(ctx)
		if pullErr != nil {
			return false, pullErr
		}
		first, found = v, true
		// Stop right here: reporting io.EOF ends the drain and closes the stream without
		// pulling a second element.
		return false, io.EOF
	})
	if err != nil {
		return util.DefaultValue[T](), false, err
	}
	return first, found, nil
}

// FindLast materializes the whole stream and returns its last element. found is false when the
// stream is empty, which is not an error; err reports a failure of the pipeline.
//
//go:noinline
func FindLast[T any](ctx context.Context, s Stream[T]) (last T, found bool, err error) {
	err = consumeLoop(ctx, s.allLifecycleElement, func(ctx context.Context) (bool, error) {
		v, pullErr := s.provider(ctx)
		if pullErr != nil {
			return false, pullErr
		}
		last, found = v, true
		return true, nil
	})
	if err != nil {
		return util.DefaultValue[T](), false, err
	}
	return last, found, nil
}

// FindFirstAndLast materializes the whole stream and returns its first and last elements (the
// same element when the stream has exactly one). found is false when the stream is empty, which
// is not an error; err reports a failure of the pipeline.
func FindFirstAndLast[T any](ctx context.Context, s Stream[T]) (firstAndLast shpanstream.Tuple2[T, T], found bool, err error) {
	err = consume(s, ctx, func(v T) {
		if !found {
			firstAndLast.A = v
			found = true
		}
		firstAndLast.B = v
	}, nil)
	if err != nil {
		return shpanstream.Tuple2[T, T]{}, false, err
	}
	return firstAndLast, found, nil
}

// Collect materializes the stream, and collects all elements of the stream into a slice
// It returns an error if the stream materialization fails in any stage of the pipeline
//
//go:noinline
func Collect[T any](ctx context.Context, s Stream[T]) ([]T, error) {
	var result []T
	err := consume(s, ctx, func(v T) {
		result = append(result, v)
	}, nil)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// MustCollect is Collect with context.Background(), panicking if the stream fails. Use it in tests
// or with static streams (e.g. slice sourced streams).
func MustCollect[T any](s Stream[T]) []T {
	result, err := Collect(context.Background(), s)
	if err != nil {
		panic(err)
	}
	return result
}

// Filter keeps the elements for which predicate returns true.
func (s Stream[T]) Filter[P ~func(T) bool](predicate P) Stream[T] {
	return filterStream(s, shpanstream.Predicate[T](predicate).ToErrCtx())
}

// FilterWithErr keeps the elements for which predicate returns true; a predicate error fails the stream.
func (s Stream[T]) FilterWithErr[P ~func(T) (bool, error)](predicate P) Stream[T] {
	return filterStream(s, shpanstream.PredicateWithErr[T](predicate).ToErrCtx())
}

// FilterWithErrAndCtx is FilterWithErr with a predicate that also receives the stream's context.
func (s Stream[T]) FilterWithErrAndCtx[P ~func(context.Context, T) (bool, error)](predicate P) Stream[T] {
	return filterStream(s, shpanstream.PredicateWithErrAndCtx[T](predicate))
}

//go:noinline
func filterStream[T any](s Stream[T], predicate shpanstream.PredicateWithErrAndCtx[T]) Stream[T] {
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
func Count[T any](ctx context.Context, s Stream[T]) (int, error) {
	count := 0
	err := consume(s, ctx, func(v T) {
		count++
	}, nil)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// MustCount is Count with context.Background(), panicking if the stream fails. Use it in tests
// or with static streams (e.g. slice sourced streams).
func MustCount[T any](s Stream[T]) int {
	count, err := Count(context.Background(), s)
	if err != nil {
		panic(err)
	}
	return count
}

// IsEmpty reports whether the stream has no elements. It materializes the stream only up to its
// first element, like FindFirst.
func IsEmpty[T any](ctx context.Context, s Stream[T]) (bool, error) {
	_, found, err := FindFirst(ctx, s)
	if err != nil {
		return false, err
	}
	return !found, nil
}

// WithAdditionalLifecycle returns the stream with lch added to its lifecycle: lch is opened with
// the stream's other lifecycle elements when the stream is materialized, and closed with them.
func WithAdditionalLifecycle[T any](s Stream[T], lch Lifecycle) Stream[T] {
	return newStream(s.provider, appendLifecycle(s.allLifecycleElement, lch))
}

// Peek allows to peek at the elements of the stream without consuming them
// Peek will not materialize the stream, and will be invoked only (and if) the stream is materialized
func (s Stream[T]) Peek[F ~func(v T)](f F) Stream[T] {
	return peekStream(s, (func(T))(f))
}

//go:noinline
func peekStream[T any](s Stream[T], f func(v T)) Stream[T] {
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
