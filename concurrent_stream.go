package shpanstream

import (
	"context"
	"fmt"
	"io"
	"sync"
)

type concurrentStreamMapperProvider[SRC any, TGT any] struct {
	srcStream   Stream[SRC]
	concurrency int
	srcChan     chan Entry[SRC, error]
	tgtChan     chan Entry[TGT, error]
	mapper      func(context.Context, SRC) (TGT, error)
	eofCtx      context.Context
}

// mapStreamConcurrently return a mapped stream using a mapper function. the mapping is done concurrently.
// note that the resulting stream order is not guaranteed to be the same as the source stream order.
func mapStreamConcurrently[SRC any, TGT any](src Stream[SRC], concurrency int, mapper func(context.Context, SRC) (TGT, error)) Stream[TGT] {
	if concurrency <= 0 {
		return NewErrorStream[TGT](fmt.Errorf("concurrency must be > 0"))
	}
	return NewStream[TGT](&concurrentStreamMapperProvider[SRC, TGT]{
		srcStream:   src,
		concurrency: concurrency,
		mapper:      mapper,
	})
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) Open(ctx context.Context) error {
	err := openSubStream(ctx, c.srcStream)
	if err != nil {
		return err
	}

	// Source channel has concurrency length to allow for concurrent reads
	c.srcChan = make(chan Entry[SRC, error], c.concurrency)
	// Target channel has concurrency length to allow for concurrent reads
	c.tgtChan = make(chan Entry[TGT, error], c.concurrency)

	eofCtx, eofCancelFunc := context.WithCancel(context.Background())
	c.eofCtx = eofCtx

	// Start the workers
	var wg sync.WaitGroup
	for i := 0; i < c.concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					// If the context is done, exit the goroutine
					return
				case entry, stillGood := <-c.srcChan:
					if !stillGood {
						return
					}
					// Check if src had error
					if entry.Value != nil {
						select {
						case c.tgtChan <- Entry[TGT, error]{Key: defaultValue[TGT](), Value: entry.Value}:
						case <-ctx.Done():
							return
						}

					} else {
						// Run the mapper function concurrently

						tgt, err := c.mapper(ctx, entry.Key)
						if err != nil {
							select {
							case c.tgtChan <- Entry[TGT, error]{Key: defaultValue[TGT](), Value: err}:
							case <-ctx.Done():
								return
							}
						} else {
							select {
							case c.tgtChan <- Entry[TGT, error]{Key: tgt, Value: nil}:
							case <-ctx.Done():
								return
							}
						}
					}
				}
			}
		}()
	}

	go func() {

		// Closing the streams when exiting
		defer func() {
			// Closing the channel, the workers will still process the remaining buffered messages (if any)
			//(yes, this is how channels work)
			close(c.srcChan)
			// Waiting for all workers to exit properly
			wg.Wait()
			close(c.tgtChan)
		}()
		for {
			select {
			case <-ctx.Done():

				return
			default:
				// Read from the source stream
				v, err := c.srcStream.provider(ctx)
				if err != nil {
					if err == io.EOF {
						// If the source stream is done, we need to deplete the buffer and only then return the EOF
						// This is because the workers are still running, and we need to wait for them to finish
						// before closing the target channel
						eofCancelFunc()
						return
					} else {
						// If an error occurs, just pass it through the buffer, it will get there
						select {

						case c.srcChan <- Entry[SRC, error]{Key: defaultValue[SRC](), Value: err}:
						case <-ctx.Done():
							return
						}
					}
				} else {
					select {
					case c.srcChan <- Entry[SRC, error]{Key: v, Value: nil}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return nil
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) Close() {
	closeSubStream(c.srcStream)
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) Emit(ctx context.Context) (TGT, error) {
	select {
	case <-ctx.Done():
		return defaultValue[TGT](), ctx.Err()
	case entry, stillGood := <-c.tgtChan:
		// Channel close is expected when the source stream is done (or when the context is done)
		if !stillGood {
			if c.eofCtx.Err() != nil {
				return defaultValue[TGT](), io.EOF
			}
			if ctx.Err() != nil {
				return defaultValue[TGT](), ctx.Err()
			}
			// Should never happen
			return defaultValue[TGT](), fmt.Errorf("concurrent stream channel closed prematurely")
		}
		if entry.Value != nil {
			return defaultValue[TGT](), entry.Value
		}
		return entry.Key, nil
	}
}
