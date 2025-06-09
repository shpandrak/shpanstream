package stream

import (
	"context"
	"errors"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/lazy"
	"io"
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
	var openFunc func(ctx context.Context) error
	var closeFunc func()

	for _, option := range options {
		if option.openFunc != nil {
			openFunc = option.openFunc
		}
		if option.closeFunc != nil {
			closeFunc = option.closeFunc
		}
	}

	var lifeCycleElements []Lifecycle
	if openFunc != nil || closeFunc != nil {
		lifeCycleElements = []Lifecycle{
			NewLifecycle(openFunc, closeFunc),
		}
	}
	return Stream[T]{provider: streamProviderFunc, allLifecycleElement: lifeCycleElements}
}

type ProviderFunc[T any] func(ctx context.Context) (T, error)

// Consume consumes the entire stream and applies the provided function to each element (sometimes named ForEach)
// It returns an error if the stream materialization fails in any stage of the pipeline
// For empty streams, it returns immediately with no error
// For infinite streams, it will block until the stream either ctx is cancelled, stream is done or an error occurs
func (s Stream[T]) Consume(ctx context.Context, f func(T)) error {
	return s.ConsumeWithErr(ctx, func(v T) error {
		f(v)
		return nil
	})
}

// MustConsume is a convenience method that panics if the stream errors
func (s Stream[T]) MustConsume(f func(T)) {
	err := s.Consume(context.Background(), f)
	if err != nil {
		panic(err)
	}
}

// ConsumeWithErr consumes the entire stream and applies the provided function to each element (sometimes named ForEach)
// Allows to return an error from the function to stop the pipeline
// It returns an error if the stream materialization fails in any stage of the pipeline
func (s Stream[T]) ConsumeWithErr(ctx context.Context, f func(T) error) error {
	return s.ConsumeWithErrAndCtx(ctx, func(_ context.Context, v T) error {
		return f(v)
	})
}

// ConsumeWithErrAndCtx consumes the entire stream and applies the provided function to each element (sometimes named ForEach)
// Allows to return an error from the function to stop the pipeline,
// passing through the context allowing the function to gracefully cancel
// It returns an error if the stream materialization fails in any stage of the pipeline
func (s Stream[T]) ConsumeWithErrAndCtx(ctx context.Context, f func(ctx context.Context, value T) error) error {

	cancelFunc, err := doOpenStream[T](ctx, s)
	if err != nil {
		return err
	}

	// If we reach here, all lifecycle elements have been opened successfully
	// We can defer closing them until the end of the function
	defer func() {
		doCloseSubStream(s)
		cancelFunc()
	}()

	for {

		// Make sure to check if the context is done before trying to get the next item
		if ctx.Err() != nil {
			// If the context is cancelled, we need to close all lifecycle elements
			// return
			return ctx.Err()
		}
		v, err := s.provider(ctx)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		err = f(ctx, v)
		if err != nil {
			return err
		}
	}
}

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

	}).OrElseThrow(func() error {
		return errors.New("no \"first element\" in an empty stream")
	})
}

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
	}).OrElseThrow(func() error {
		return errors.New("no \"last element\" in an empty stream")
	})

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
	}).OrElseThrow(func() error {
		return errors.New("no \"first and last element\" in an empty stream")
	})

}

// Collect materializes the stream, and collects all elements of the stream into a slice
// It returns an error if the stream materialization fails in any stage of the pipeline
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
	var result []T
	err := s.Consume(context.Background(), func(v T) {
		result = append(result, v)
	})
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
				return util.DefaultValue[T](), fmt.Errorf("filter failed for Stream: %w", err)
			}
			if shouldKeep {
				return v, nil
			}
		}
	}, s.allLifecycleElement)
}

// Count counts the number of elements in the stream (materializes the stream)
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
	return newStream(s.provider, append(s.allLifecycleElement, lch))
}

func doOpenStream[T any](ctx context.Context, s Stream[T]) (context.CancelFunc, error) {
	ctxWithCancel, cancelFunc := context.WithCancel(ctx)
	// Running all lifecycle elements
	for lcIdx, l := range s.allLifecycleElement {
		err := l.Open(ctxWithCancel)
		if err != nil {
			// If we fail to open a lifecycle element, we need to close all previously opened elements
			// and return the error

			// Close only the successfully opened lifecycle elements
			for i := 0; i < lcIdx; i++ {
				s.allLifecycleElement[i].Close()
			}
			// Cancel the context to stop any ongoing operations
			cancelFunc()

			return nil, fmt.Errorf("failed to open stream lifecycle element %d: %w", lcIdx, err)
		}
	}
	return cancelFunc, nil
}

func doCloseSubStream[T any](s Stream[T]) {
	for _, l := range s.allLifecycleElement {
		l.Close()
	}
}

// Peek allows to peek at the elements of the stream without consuming them
// Peek will not materialize the stream, and will be invoked only (and if) the stream is materialized
func (s Stream[T]) Peek(f func(v T)) Stream[T] {
	return Map(
		s,
		func(v T) T {
			f(v)
			return v
		})
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
