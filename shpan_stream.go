package shpanstream

import (
	"context"
	"errors"
	"fmt"
	"io"
)

type StreamLifecycle interface {
	Open(ctx context.Context) error
	Close()
}

type streamLifecycleWrapper struct {
	openFunc  func(ctx context.Context) error
	closeFunc func()
}

func NewStreamLifecycle(openFunc func(ctx context.Context) error, closeFunc func()) StreamLifecycle {
	return &streamLifecycleWrapper{openFunc: openFunc, closeFunc: closeFunc}
}

func (s *streamLifecycleWrapper) Open(ctx context.Context) error {
	if s.openFunc != nil {
		return s.openFunc(ctx)
	}
	return nil
}

func (s *streamLifecycleWrapper) Close() {
	if s.closeFunc != nil {
		s.closeFunc()
	}
}

type StreamProvider[T any] interface {
	StreamLifecycle
	Emit(ctx context.Context) (T, error)
}

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

type SimpleStreamOption struct {
	openFunc  func(ctx context.Context) error
	closeFunc func()
}

func WithOpenFuncOption(openFunc func(ctx context.Context) error) SimpleStreamOption {
	return SimpleStreamOption{openFunc: openFunc}
}

func WithCloseFuncOption(closeFunc func()) SimpleStreamOption {
	return SimpleStreamOption{closeFunc: closeFunc}
}

func NewSimpleStream[T any](streamProviderFunc StreamProviderFunc[T], options ...SimpleStreamOption) Stream[T] {
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

func (s Stream[T]) ConsumeWithErr(ctx context.Context, f func(T) error) error {

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

			return fmt.Errorf("failed to open stream lifecycle element %d: %w", lcIdx, err)
		}
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
		if ctx.Err() != nil {
			// If the context is cancelled, we need to close all lifecycle elements
			// return
			return ctx.Err()
		}

		v, err := s.provider(ctxWithCancel)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		err = f(v)
		if err != nil {
			return err
		}
	}
}

func FlatMapStream[SRC any, TGT any](src Stream[SRC], mapper func(SRC) Stream[TGT]) Stream[TGT] {
	streamOfStreams := MapStreamWithErrAndCtx[SRC, Stream[TGT]](src, func(ctx context.Context, src SRC) (Stream[TGT], error) {
		return mapper(src), nil
	})

	collect, err := streamOfStreams.Collect(context.Background())
	if err != nil {
		return NewErrorStream[TGT](err)
	}
	return ConcatStreams[TGT](collect...)
}

func (s Stream[T]) GetFirst(ctx context.Context) (*T, error) {
	return s.FindFirst().Get(ctx)
}

type errResult struct {
}

func (e *errResult) Error() string {
	return "error"
}

func (s Stream[T]) FindFirst() Lazy[T] {
	return NewLazy[T](func(ctx context.Context) (*T, error) {
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
	return NewLazy[T](func(ctx context.Context) (*T, error) {
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

func (s Stream[T]) Filter(predicate func(T) bool) Stream[T] {
	return s.FilterWithErrorAndContext(func(ctx context.Context, v T) (bool, error) {
		return predicate(v), nil
	})
}

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

func (s Stream[T]) FilterWithError(predicate func(T) (bool, error)) Stream[T] {
	return s.FilterWithErrorAndContext(func(_ context.Context, v T) (bool, error) {
		return predicate(v)
	})
}

func (s Stream[T]) FilterWithErrorAndContext(predicate func(context.Context, T) (bool, error)) Stream[T] {
	return newStream[T](func(ctx context.Context) (T, error) {
		for {
			v, err := s.provider(ctx)
			if err != nil {
				return v, err
			}
			shouldKeep, err := predicate(ctx, v)
			if err != nil {
				// Wrapping errors, e.g. we don't want EOF accidentally returned from here
				return defaultValue[T](), fmt.Errorf("filter failed for Stream: %w", err)
			}
			if shouldKeep {
				return v, nil
			}
		}
	}, s.allLifecycleElement)
}

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

func (s Stream[T]) IsEmpty(ctx context.Context) (bool, error) {
	return s.FindFirst().IsEmpty(ctx)
}

func (s Stream[T]) WithAdditionalStreamLifecycle(lch StreamLifecycle) Stream[T] {
	return newStream(s.provider, append(s.allLifecycleElement, lch))
}
