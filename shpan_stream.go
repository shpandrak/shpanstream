package shpanstream

import (
	"context"
	"errors"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type Stream[T any] struct {
	provider            StreamProviderFunc[T]
	allLifecycleElement []StreamLifecycle
}

func NewStream[T any](provider StreamProvider[T]) Stream[T] {
	return newStream(provider.Emit, []StreamLifecycle{provider})
}

func newStream[T any](streamProviderFunc StreamProviderFunc[T], allLifecycleElement []StreamLifecycle) Stream[T] {
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

func NewSimpleStream[T any](streamProviderFunc StreamProviderFunc[T], options ...CreateStreamOption) Stream[T] {
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

	var lifeCycleElements []StreamLifecycle
	if openFunc != nil || closeFunc != nil {
		lifeCycleElements = []StreamLifecycle{
			NewStreamLifecycle(openFunc, closeFunc),
		}
	}
	return Stream[T]{provider: streamProviderFunc, allLifecycleElement: lifeCycleElements}
}

type StreamProviderFunc[T any] func(ctx context.Context) (T, error)

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

	ctx, cancelFunc, err := doOpenStream[T](ctx, s)
	if err != nil {
		return err
	}

	// If we reach here, all lifecycle elements have been opened successfully
	// We can defer closing them until the end of the function
	defer func() {
		for _, l := range s.allLifecycleElement {
			l.Close()
		}
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

type errResult struct {
}

func (e *errResult) Error() string {
	return "error"
}

func (s Stream[T]) FindFirst() Lazy[T] {
	return NewLazyOptional[T](func(ctx context.Context) (*T, error) {
		var result *T
		err := s.ConsumeWithErr(ctx, func(v T) error {
			result = &v
			return &errResult{}
		})

		if err != nil {
			var r *errResult
			if errors.As(err, &r) {
				return result, nil
			}
			return nil, err
		}
		return result, nil
	})
}

func (s Stream[T]) FindLast() Lazy[T] {
	return NewLazyOptional[T](func(ctx context.Context) (*T, error) {
		var result *T
		err := s.Consume(ctx, func(v T) {
			result = &v
		})
		if err != nil {
			return nil, err
		}
		return result, nil
	})
}

// FindFirstAndLast returns the first and last element of the stream, unless the stream is empty
// if the stream contains only one element, the first and last element are the same
func FindFirstAndLast[T any](s Stream[T]) Lazy[Entry[T, T]] {
	return NewLazyOptional[Entry[T, T]](func(ctx context.Context) (*Entry[T, T], error) {
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
		return &Entry[T, T]{
			Key:   *first,
			Value: *last,
		}, nil
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

func (s Stream[T]) Filter(predicate Predicate[T]) Stream[T] {
	return s.FilterWithErAndCtx(predicateToErrCtx(predicate))
}

func (s Stream[T]) FilterWithErr(predicate PredicateWithErr[T]) Stream[T] {
	return s.FilterWithErAndCtx(predicateErrToErrCtx(predicate))
}

func (s Stream[T]) FilterWithErAndCtx(predicate PredicateWithErrAndCtx[T]) Stream[T] {
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

func (s Stream[T]) WithAdditionalStreamLifecycle(lch StreamLifecycle) Stream[T] {
	return newStream(s.provider, append(s.allLifecycleElement, lch))
}

func doOpenStream[T any](ctx context.Context, s Stream[T]) (context.Context, context.CancelFunc, error) {

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

			return nil, nil, fmt.Errorf("failed to open stream lifecycle element %d: %w", lcIdx, err)
		}
	}
	return ctxWithCancel, cancelFunc, nil
}

func closeSubStream[T any](s Stream[T]) {
	for _, l := range s.allLifecycleElement {
		l.Close()
	}
}
