package stream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
	"sync"
)

type concurrentStreamMapperProvider[SRC any, TGT any] struct {
	concurrency int
	mapper      func(context.Context, SRC) (TGT, error)
	src         Stream[SRC]

	// Per-consumption state, (re)initialised in open and torn down in close.
	srcChan        chan shpanstream.Result[SRC]
	tgtChan        chan shpanstream.Result[TGT]
	eofCtx         context.Context
	eofCancel      context.CancelFunc
	internalCancel context.CancelFunc
	srcCancel      context.CancelFunc
	producerDone   chan struct{}
}

// mapStreamConcurrently returns a mapped stream using a mapper function. The mapping is done
// concurrently. Note that the resulting stream order is not guaranteed to match the source order.
func mapStreamConcurrently[SRC any, TGT any](
	src Stream[SRC],
	concurrency int,
	mapper shpanstream.MapperWithErrAndCtx[SRC, TGT],
) Stream[TGT] {
	if concurrency <= 0 {
		return Error[TGT](fmt.Errorf("concurrency must be > 0"))
	}
	c := &concurrentStreamMapperProvider[SRC, TGT]{
		concurrency: concurrency,
		mapper:      mapper,
		src:         src,
	}
	// The provider owns the source lifecycle itself (rather than delegating to the unsafe builder)
	// so that close can join the background reader goroutine BEFORE closing the source — otherwise
	// the reader keeps driving the source's provider while the source is being torn down (a data
	// race that surfaces on early termination: Limit, ctx cancellation, downstream error).
	return NewSimpleStream(
		c.emit,
		WithOpenFuncOption(c.open),
		WithCloseFuncOption(c.close),
	)
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) open(ctx context.Context) error {
	// Open (and later close) the source ourselves so its lifecycle is joined with the reader.
	srcCancel, err := doOpenStream[SRC](ctx, c.src)
	if err != nil {
		return err
	}
	c.srcCancel = srcCancel
	srcProviderFunc := c.src.provider

	// Source channel and target channel are concurrency-sized to allow concurrent reads.
	c.srcChan = make(chan shpanstream.Result[SRC], c.concurrency)
	c.tgtChan = make(chan shpanstream.Result[TGT], c.concurrency)

	// internalCtx bounds the reader + worker goroutines; close cancels it and waits for them.
	internalCtx, internalCancel := context.WithCancel(ctx)
	c.internalCancel = internalCancel

	// eofCtx lets emit distinguish "closed because the source reached EOF" from "closed on cancel".
	eofCtx, eofCancelFunc := context.WithCancel(context.Background())
	c.eofCtx = eofCtx
	c.eofCancel = eofCancelFunc

	// Closed only after the reader and every worker have fully exited, so close can join on it.
	c.producerDone = make(chan struct{})

	// Start the workers.
	var wg sync.WaitGroup
	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-internalCtx.Done():
					return
				case entry, stillGood := <-c.srcChan:
					if !stillGood {
						return
					}
					// Check if src had error
					if entry.Err != nil {
						select {
						case c.tgtChan <- shpanstream.Result[TGT]{Err: entry.Err}:
						case <-internalCtx.Done():
							return
						}
					} else {
						// Run the mapper function concurrently
						tgt, err := c.mapper(internalCtx, entry.Value)
						if err != nil {
							select {
							case c.tgtChan <- shpanstream.Result[TGT]{Err: err}:
							case <-internalCtx.Done():
								return
							}
						} else {
							select {
							case c.tgtChan <- shpanstream.Result[TGT]{Value: tgt}:
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
			close(c.srcChan)
			// Wait for all workers to exit properly.
			wg.Wait()
			close(c.tgtChan)
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
						case c.srcChan <- shpanstream.Result[SRC]{Err: err}:
						case <-internalCtx.Done():
							return
						}
					}
				} else {
					select {
					case c.srcChan <- shpanstream.Result[SRC]{Value: v}:
					case <-internalCtx.Done():
						return
					}
				}
			}
		}
	}()

	return nil
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) emit(ctx context.Context) (TGT, error) {
	select {
	case <-ctx.Done():
		return util.DefaultValue[TGT](), ctx.Err()
	case r, stillGood := <-c.tgtChan:
		// Channel close is expected when the source stream is done (or when the context is done).
		if !stillGood {
			if c.eofCtx.Err() != nil {
				return util.DefaultValue[TGT](), io.EOF
			}
			if ctx.Err() != nil {
				return util.DefaultValue[TGT](), ctx.Err()
			}
			// Should never happen
			return util.DefaultValue[TGT](), fmt.Errorf("concurrent stream channel closed prematurely")
		}
		return r.Unpack()
	}
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) close() {
	// Stop the reader + workers and wait for them to fully exit BEFORE closing the source, so
	// nothing drives the source's provider concurrently with its Close.
	if c.internalCancel != nil {
		c.internalCancel()
	}
	if c.producerDone != nil {
		<-c.producerDone
	}
	doCloseSubStream[SRC](c.src)
	if c.srcCancel != nil {
		c.srcCancel()
	}
	// On early termination the reader never reaches EOF and eofCancelFunc was never called;
	// release the eofCtx here (calling it twice is safe).
	if c.eofCancel != nil {
		c.eofCancel()
	}
}
