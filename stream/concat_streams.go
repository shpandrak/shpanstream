package stream

import (
	"context"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type concatProvider[T any] struct {
	streamsProviderFunc ProviderFunc[Stream[T]]
	currProviderFunc    ProviderFunc[T]
	currStreamHandle    int
}

// Concat concatenates multiple streams into a single stream. the streams are joined sequentially one after the other.
func Concat[T any](streams Stream[Stream[T]]) Stream[T] {

	b := &unsafeProviderBuilder{}
	addStreamUnsafe(b, streams)
	cp := &concatProvider[T]{}

	return newUnsafeStream[T](
		b,
		cp.open,
		cp.emit,

		nil,
	)
}

// ConcatStreams concatenates multiple streams into a single stream. the streams are joined sequentially one after the other.
func ConcatStreams[T any](streams ...Stream[T]) Stream[T] {
	if len(streams) == 0 {
		return Empty[T]()
	}
	return Concat(Just(streams...))
}

func (cp *concatProvider[T]) open(ctx context.Context, b *unsafeProviderBuilder) error {
	// Open the steam of streams
	outer, err := openSubStreamUnsafe[Stream[T]](ctx, b, 0)
	if err != nil {
		return err
	}
	cp.streamsProviderFunc = outer
	// Open the first Stream

	currStream, err := outer(ctx)
	if err != nil {
		// If no streams are available, return nil, and current stream is nil so we're good
		if err == io.EOF {
			return nil
		}
		// if err is not EOF, we also return it...
		return err
	}

	cp.currStreamHandle = addStreamUnsafe(b, currStream)
	// Open the first Stream
	cp.currProviderFunc, err = openSubStreamUnsafe[T](ctx, b, cp.currStreamHandle)
	if err != nil {
		return err
	}

	return nil
}

func (cp *concatProvider[T]) emit(ctx context.Context, b *unsafeProviderBuilder) (T, error) {
	// First check if the context is done
	if ctx.Err() != nil {
		return util.DefaultValue[T](), ctx.Err()
	}
	if cp.currProviderFunc == nil {
		return util.DefaultValue[T](), io.EOF
	}

	currStreamNextItem, err := cp.currProviderFunc(ctx)
	if err != nil {
		if err == io.EOF {
			// If current Stream is done, close it and continue with the next one
			err := closeSubStreamUnsafe(b, cp.currStreamHandle)
			if err != nil {
				return util.DefaultValue[T](), err
			}
			cp.currProviderFunc = nil

			// Always check if the context is done before trying to get the next stream
			if ctx.Err() != nil {
				return util.DefaultValue[T](), ctx.Err()
			}

			// try getting the next stream
			nextStream, err := cp.streamsProviderFunc(ctx)
			if err != nil {
				// return the error (even if EOF)
				return util.DefaultValue[T](), err
			} else {
				cp.currStreamHandle = addStreamUnsafe(b, nextStream)
				// open the next stream
				cp.currProviderFunc, err = openSubStreamUnsafe[T](ctx, b, cp.currStreamHandle)
				if err != nil {
					return util.DefaultValue[T](), err
				}
			}
			// try getting the next item from the new stream
			return cp.emit(ctx, b)

		} else {
			// This is an error, not EOF
			return util.DefaultValue[T](), err
		}
	} else {
		return currStreamNextItem, nil
	}
}
