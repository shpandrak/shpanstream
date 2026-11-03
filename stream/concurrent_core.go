package stream

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/shpandrak/shpanstream"
)

// The concurrent fan-out (WithConcurrentMapOption / WithConcurrentConsumeOption) is compiled once,
// over boxed `any` values, instead of once per element type: the generic entry points only box the
// source provider, the mapper and the consumer and unbox the result (see
// docs/compile-size-per-element-type.md). Boxing a struct element allocates, but only on these
// opt-in concurrent paths and in Untyped(), which reuses boxProvider below and has always boxed;
// no other sequential path goes through this file.

// anyProviderFunc is a ProviderFunc whose element is boxed.
type anyProviderFunc func(ctx context.Context) (any, error)

// anyMapperFunc is a MapperWithErrAndCtx over boxed source and target elements.
type anyMapperFunc func(ctx context.Context, v any) (any, error)

// anyConsumerFunc is a consume callback over a boxed element.
type anyConsumerFunc func(ctx context.Context, v any) error

// The box* adapters are marked noinline so that each closure body is compiled once per element
// type: inlined into every caller, the same closure is duplicated per call site (and again inside
// the compiler-generated pointer-receiver wrappers), which is exactly the code volume being cut.

// boxProvider adapts a typed provider to a boxed one. An error leaves the boxed value nil, matching
// the zero element the typed paths return alongside an error.
//
//go:noinline
func boxProvider[T any](p ProviderFunc[T]) anyProviderFunc {
	return func(ctx context.Context) (any, error) {
		v, err := p(ctx)
		if err != nil {
			return nil, err
		}
		return v, nil
	}
}

// boxMapper adapts a typed mapper to a boxed one; see boxProvider for the error convention.
//
//go:noinline
func boxMapper[SRC any, TGT any](m shpanstream.MapperWithErrAndCtx[SRC, TGT]) anyMapperFunc {
	return func(ctx context.Context, v any) (any, error) {
		tgt, err := m(ctx, unboxValue[SRC](v))
		if err != nil {
			return nil, err
		}
		return tgt, nil
	}
}

// boxConsumer adapts a typed consume callback to a boxed one.
//
//go:noinline
func boxConsumer[T any](f func(ctx context.Context, value T) error) anyConsumerFunc {
	return func(ctx context.Context, v any) error {
		return f(ctx, unboxValue[T](v))
	}
}

// unboxValue is the inverse of the boxing above: a nil interface (the boxed form of "no value",
// e.g. alongside an error) unboxes to T's zero value rather than panicking.
func unboxValue[T any](v any) T {
	t, _ := v.(T)
	return t
}

// unboxResult unboxes a (value, error) pair coming out of the boxed machinery.
func unboxResult[T any](v any, err error) (T, error) {
	return unboxValue[T](v), err
}

// concurrentMapCore is the per-consumption state of a concurrent map, (re)initialised in open and
// torn down in close. It is embedded by the generic concurrentStreamMapperProvider shell.
type concurrentMapCore struct {
	srcLifecycles  []Lifecycle
	srcChan        chan shpanstream.Result[any]
	tgtChan        chan shpanstream.Result[any]
	eofCtx         context.Context
	eofCancel      context.CancelFunc
	internalCancel context.CancelFunc
	srcCancel      context.CancelFunc
	producerDone   chan struct{}
}

func (c *concurrentMapCore) open(
	ctx context.Context,
	concurrency int,
	srcLifecycles []Lifecycle,
	srcProviderFunc anyProviderFunc,
	mapper anyMapperFunc,
) error {
	// Open (and later close) the source ourselves so its lifecycle is joined with the reader.
	c.srcLifecycles = srcLifecycles
	srcCancel, err := openStreamLifecycles(ctx, srcLifecycles)
	if err != nil {
		return err
	}
	c.srcCancel = srcCancel

	// Source channel and target channel are concurrency-sized to allow concurrent reads.
	c.srcChan = make(chan shpanstream.Result[any], concurrency)
	c.tgtChan = make(chan shpanstream.Result[any], concurrency)

	// internalCtx bounds the reader + worker goroutines; close cancels it and waits for them.
	internalCtx, internalCancel := context.WithCancel(ctx)
	c.internalCancel = internalCancel

	// eofCtx lets emit distinguish "closed because the source reached EOF" from "closed on cancel".
	eofCtx, eofCancelFunc := context.WithCancel(context.Background())
	c.eofCtx = eofCtx
	c.eofCancel = eofCancelFunc

	// Closed only after the reader and every worker have fully exited, so close can join on it.
	c.producerDone = make(chan struct{})

	srcChan, tgtChan := c.srcChan, c.tgtChan

	// Start the workers.
	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-internalCtx.Done():
					return
				case entry, stillGood := <-srcChan:
					if !stillGood {
						return
					}
					// Check if src had error
					if entry.Err != nil {
						select {
						case tgtChan <- shpanstream.Result[any]{Err: entry.Err}:
						case <-internalCtx.Done():
							return
						}
					} else {
						// Run the mapper function concurrently
						tgt, err := mapper(internalCtx, entry.Value)
						if err != nil {
							select {
							case tgtChan <- shpanstream.Result[any]{Err: err}:
							case <-internalCtx.Done():
								return
							}
						} else {
							select {
							case tgtChan <- shpanstream.Result[any]{Value: tgt}:
							case <-internalCtx.Done():
								return
							}
						}
					}
				}
			}
		}()
	}

	// Start the reader.
	go func() {
		// producerDone is closed last (LIFO), after the channels are drained and workers have
		// exited, so a close() blocked on <-producerDone is released only once nothing can still
		// touch the source.
		defer close(c.producerDone)
		defer func() {
			// Closing the channel, the workers will still process the remaining buffered messages.
			close(srcChan)
			// Wait for all workers to exit properly.
			wg.Wait()
			close(tgtChan)
		}()
		for {
			select {
			case <-internalCtx.Done():
				return
			default:
				// Read from the source stream
				v, err := srcProviderFunc(internalCtx)
				if err != nil {
					if err == io.EOF {
						// Source done: signal EOF and deplete the buffer before the target channel
						// closes (workers are still finishing).
						eofCancelFunc()
						return
					} else {
						// Pass the error through the buffer; it will get there.
						select {
						case srcChan <- shpanstream.Result[any]{Err: err}:
						case <-internalCtx.Done():
							return
						}
					}
				} else {
					select {
					case srcChan <- shpanstream.Result[any]{Value: v}:
					case <-internalCtx.Done():
						return
					}
				}
			}
		}
	}()

	return nil
}

func (c *concurrentMapCore) emit(ctx context.Context) (any, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r, stillGood := <-c.tgtChan:
		// Channel close is expected when the source stream is done (or when the context is done).
		if !stillGood {
			if c.eofCtx.Err() != nil {
				return nil, io.EOF
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// Should never happen
			return nil, fmt.Errorf("concurrent stream channel closed prematurely")
		}
		return r.Unpack()
	}
}

func (c *concurrentMapCore) close() {
	// Stop the reader + workers and wait for them to fully exit BEFORE closing the source, so
	// nothing drives the source's provider concurrently with its Close.
	if c.internalCancel != nil {
		c.internalCancel()
	}
	if c.producerDone != nil {
		<-c.producerDone
	}
	closeStreamLifecycles(c.srcLifecycles)
	if c.srcCancel != nil {
		c.srcCancel()
	}
	// On early termination the reader never reaches EOF and eofCancelFunc was never called;
	// release the eofCtx here (calling it twice is safe).
	if c.eofCancel != nil {
		c.eofCancel()
	}
}

// consumeConcurrently consumes the stream concurrently with the specified number of workers.
// The consumer function is called concurrently, but the source stream is read sequentially.
// Returns the first error encountered (either from reading the stream or from the consumer function).
func consumeConcurrently(
	ctx context.Context,
	concurrency int,
	lifecycles []Lifecycle,
	provider anyProviderFunc,
	f anyConsumerFunc,
) error {
	if concurrency <= 0 {
		return fmt.Errorf("concurrency must be > 0")
	}

	// The panic guard avoids leaking resources and allows returning an error via panic instead of returning it
	return consumeWithPanicGuard(func() error {
		return doConsumeConcurrently(ctx, concurrency, lifecycles, provider, f)
	})
}

func doConsumeConcurrently(
	ctx context.Context,
	concurrency int,
	lifecycles []Lifecycle,
	provider anyProviderFunc,
	f anyConsumerFunc,
) error {
	cancelFunc, errVar := openStreamLifecycles(ctx, lifecycles)
	if errVar != nil {
		return fmt.Errorf("failed to open stream: %w", errVar)
	}

	defer func() {
		closeStreamLifecycles(lifecycles)
		cancelFunc()
	}()

	// Create a buffered channel for items
	itemChan := make(chan shpanstream.Result[any], concurrency)

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
			v, err := provider(workerCtx)
			if err != nil {
				if err == io.EOF {
					return
				}
				select {
				case itemChan <- shpanstream.Result[any]{Err: err}:
				case <-workerCtx.Done():
					return
				}
				return
			}
			select {
			case itemChan <- shpanstream.Result[any]{Value: v}:
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
