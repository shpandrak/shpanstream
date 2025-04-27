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
	srcChan     chan shpanstream.Result[SRC]
	tgtChan     chan shpanstream.Result[TGT]
	mapper      func(context.Context, SRC) (TGT, error)
	eofCtx      context.Context
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) Close() {
}

// mapStreamConcurrently return a mapped stream using a mapper function. the mapping is done concurrently.
// note that the resulting stream order is not guaranteed to be the same as the source stream order.
func mapStreamConcurrently[SRC any, TGT any](
	src Stream[SRC],
	concurrency int,
	mapper shpanstream.MapperWithErrAndCtx[SRC, TGT],
) Stream[TGT] {
	if concurrency <= 0 {
		return ErrorStream[TGT](fmt.Errorf("concurrency must be > 0"))
	}
	return NewDownStream[SRC, TGT](
		src,
		&concurrentStreamMapperProvider[SRC, TGT]{
			concurrency: concurrency,
			mapper:      mapper,
		},
	)
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) Open(ctx context.Context, srcProviderFunc StreamProviderFunc[SRC]) error {

	// Source channel has concurrency length to allow for concurrent reads
	c.srcChan = make(chan shpanstream.Result[SRC], c.concurrency)
	// Target channel has concurrency length to allow for concurrent reads
	c.tgtChan = make(chan shpanstream.Result[TGT], c.concurrency)

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
					if entry.Err != nil {
						select {
						case c.tgtChan <- shpanstream.Result[TGT]{Err: entry.Err}:
						case <-ctx.Done():
							return
						}

					} else {
						// Run the mapper function concurrently

						tgt, err := c.mapper(ctx, entry.Value)
						if err != nil {
							select {
							case c.tgtChan <- shpanstream.Result[TGT]{Err: err}:
							case <-ctx.Done():
								return
							}
						} else {
							select {
							case c.tgtChan <- shpanstream.Result[TGT]{Value: tgt}:
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
				v, err := srcProviderFunc(ctx)
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

						case c.srcChan <- shpanstream.Result[SRC]{Err: err}:
						case <-ctx.Done():
							return
						}
					}
				} else {
					select {
					case c.srcChan <- shpanstream.Result[SRC]{Value: v}:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return nil
}

func (c *concurrentStreamMapperProvider[SRC, TGT]) Emit(ctx context.Context, _ StreamProviderFunc[SRC]) (TGT, error) {
	select {
	case <-ctx.Done():
		return util.DefaultValue[TGT](), ctx.Err()
	case r, stillGood := <-c.tgtChan:
		// Channel close is expected when the source stream is done (or when the context is done)
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
