package shpanstream

import (
	"context"
	"errors"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type concatenatedStream[T any] struct {
	streams    Stream[Stream[T]]
	currStream *Stream[T]
}

func Concat[T any](streams Stream[Stream[T]]) Stream[T] {
	return NewStream(&concatenatedStream[T]{
		streams: streams,
	})
}

func ConcatStreams[T any](streams ...Stream[T]) Stream[T] {
	if len(streams) == 0 {
		return EmptyStream[T]()
	}
	return Concat(Just(streams...))
}

func (ms *concatenatedStream[T]) Open(ctx context.Context) error {
	// Open the steam of streams
	err := openSubStream(ctx, ms.streams)
	if err != nil {
		return err
	}
	firstStream, err := ms.streams.provider(ctx)
	if err != nil {

		// If no streams are available, return EOF, and current stream is nil so we're good
		if err == io.EOF {
			return nil
		}
		return err
	}

	// Open the first Stream
	err = openSubStream(ctx, firstStream)
	if err != nil {
		return err
	}

	ms.currStream = &firstStream
	return nil
}

func (ms *concatenatedStream[T]) Close() {
	if ms.currStream != nil {
		// Close only the current Stream
		closeSubStream(*ms.currStream)
		ms.currStream = nil
	}
	closeSubStream(ms.streams)
}

func (ms *concatenatedStream[T]) Emit(ctx context.Context) (T, error) {

	// this means we have no streams available
	if ms.currStream == nil {
		return util.DefaultValue[T](), io.EOF
	}

	currStreamNextItem, err := ms.currStream.provider(ctx)
	if err != nil {
		if err == io.EOF {
			// If current Stream is done, close it and continue with the next one
			closeSubStream(*ms.currStream)
			ms.currStream = nil

			// try getting the next stream
			nextStream, err := ms.streams.provider(ctx)
			if err != nil {
				// end of all streams
				if err == io.EOF {
					return util.DefaultValue[T](), err
				} else {
					// this is an error, not EOF
					return util.DefaultValue[T](), err
				}
			} else {
				// open the next stream
				err = openSubStream(ctx, nextStream)
				if err != nil {
					return util.DefaultValue[T](), err
				}
				ms.currStream = &nextStream
			}
			return ms.Emit(ctx)

		} else {
			// This is an error, not EOF
			return util.DefaultValue[T](), err
		}
	}
	return currStreamNextItem, nil

}

func openSubStream[T any](ctx context.Context, s Stream[T]) error {
	var allErrors []error
	for _, l := range s.allLifecycleElement {
		if err := l.Open(ctx); err != nil {
			allErrors = append(allErrors, err)
		}
	}
	return errors.Join(allErrors...)
}

func closeSubStream[T any](s Stream[T]) {
	for _, l := range s.allLifecycleElement {
		l.Close()
	}
}
