package stream

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"

	"github.com/shpandrak/shpanstream"
)

// ConsumeOption defines options for consuming a stream
type ConsumeOption interface {
	consumeOptionName() string
}

type concurrentConsumeOption struct {
	concurrency int
}

// WithConcurrentConsumeOption causes the consumer function to be called concurrently in separate goroutines.
// Note that the source stream is being read sequentially, and only the consumer function is called concurrently.
// This is useful when the consumer function is expensive (e.g., I/O bound operations).
// Note that the order of consumption is not guaranteed to match the order of the source stream.
func WithConcurrentConsumeOption(concurrency int) ConsumeOption {
	return &concurrentConsumeOption{concurrency: concurrency}
}

func (c *concurrentConsumeOption) consumeOptionName() string {
	return "concurrent"
}

// consumeConcurrently consumes the stream concurrently with the specified number of workers.
// The consumer function is called concurrently, but the source stream is read sequentially.
// Returns the first error encountered (either from reading the stream or from the consumer function).
func (s Stream[T]) consumeConcurrently(ctx context.Context, concurrency int, f func(ctx context.Context, value T) error) (err error) {
	if concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}

	// Adding a panic recovery to avoid leaking resources and allow returning an error via panic instead of returning it
	defer func() {
		if rvr := recover(); rvr != nil {
			slog.Error(fmt.Sprintf("Panic recovered: %v\n%s", rvr, debug.Stack()))
			asErr, ok := rvr.(error)
			if ok {
				err = fmt.Errorf("stream recovered error: %w", asErr)
			} else {
				err = fmt.Errorf("stream recovered error value: %v", rvr)
			}
		}
	}()

	cancelFunc, errVar := doOpenStream[T](ctx, s)
	if errVar != nil {
		return fmt.Errorf("failed to open stream: %w", errVar)
	}

	defer func() {
		doCloseSubStream(s)
		cancelFunc()
	}()

	// Create a buffered channel for items
	itemChan := make(chan shpanstream.Result[T], concurrency)

	// Context with cancel for early termination on error
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()

	// Use sync.Once to capture first error and cancel workers
	var firstErr error
	var errOnce sync.Once
	setErr := func(e error) {
		errOnce.Do(func() {
			firstErr = e
			workerCancel()
		})
	}

	// Channel to signal producer completion
	producerDone := make(chan struct{})

	// Producer goroutine - reads from stream and sends to workers
	go func() {
		defer close(producerDone)
		defer close(itemChan)
		for {
			if workerCtx.Err() != nil {
				return
			}
			v, err := s.provider(workerCtx)
			if err != nil {
				if err == io.EOF {
					return
				}
				select {
				case itemChan <- shpanstream.Result[T]{Err: err}:
				case <-workerCtx.Done():
					return
				}
				return
			}
			select {
			case itemChan <- shpanstream.Result[T]{Value: v}:
			case <-workerCtx.Done():
				return
			}
		}
	}()

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-workerCtx.Done():
					// Drain remaining items to allow producer to exit
					for range itemChan {
					}
					return
				case item, ok := <-itemChan:
					if !ok {
						return
					}
					if item.Err != nil {
						setErr(item.Err)
						return
					}
					if err := f(workerCtx, item.Value); err != nil {
						setErr(err)
						return
					}
				}
			}
		}()
	}

	// Wait for all workers to complete
	wg.Wait()

	// Wait for producer to complete before closing stream resources
	<-producerDone

	// Return the first error if any occurred
	if firstErr != nil {
		return firstErr
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
}
