package jsonstream

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"io"
)

func StreamJsonToWriter[T any](ctx context.Context, w io.Writer, stream stream.Stream[T]) error {
	return StreamJsonToWriterWithInit(ctx, w, stream, func() error {
		return nil
	})
}

func StreamJsonToWriterWithInit[T any](
	ctx context.Context,
	w io.Writer,
	stream stream.Stream[T],
	initFunc func() error,
) error {
	first := true
	err := stream.ConsumeWithErr(ctx, func(v T) error {
		if first {
			first = false
			err := initFunc()
			if err != nil {
				return err
			}

			_, err = w.Write([]byte("["))
			if err != nil {
				return err
			}
		} else {
			_, err := w.Write([]byte(","))
			if err != nil {
				return err
			}
		}
		rawJson, err := json.Marshal(v)
		if err != nil {
			return err
		}
		_, err = w.Write(rawJson)
		return err
	})
	if err != nil {
		return err
	}

	// Handling the case of an empty Stream
	if first {
		err := initFunc()
		if err != nil {
			return err
		}
		_, err = w.Write([]byte("[]"))
		return err
	}
	_, err = w.Write([]byte("]"))
	return err
}

func StreamJsonAsReaderAndReturn[T any, V any](
	ctx context.Context,
	stream stream.Stream[T],
	consumer func(ctx context.Context, r io.Reader) (V, error),
) (V, error) {

	// Create a new context with cancel function allowing to cancel the streaming in case of errors
	ctx, cancelFunc := context.WithCancelCause(ctx)

	// Creating both ends of the pipe
	pr, pw := io.Pipe()
	go func() {
		defer pw.Close()

		first := true

		err := stream.ConsumeWithErr(ctx, func(v T) error {

			// Write start of array or delimiter if not the first element
			if first {
				_, err := pw.Write([]byte("["))
				if err != nil {
					return fmt.Errorf("failed writting start json array to pipe: %w", err)
				}
			} else {
				if _, err := pw.Write([]byte(",")); err != nil {
					return fmt.Errorf("failed to write json delimiter to pipe: %w", err)
				}
			}
			first = false

			rawJson, err := json.Marshal(v)
			if err != nil {
				return fmt.Errorf("failed to marshal json reading from stream: %w", err)
			}

			_, err = pw.Write(rawJson)
			if err != nil {
				return fmt.Errorf("failed to write json to pipe: %w", err)
			}
			return nil
		})

		if err != nil {
			err := fmt.Errorf("failed to read input stream for pipe: %w", err)
			cancelFunc(err)
			_ = pw.CloseWithError(err)
			return
		}

		// Handle the case of an empty Stream
		if first {
			_, err = pw.Write([]byte("[]"))
			if err != nil {
				err := fmt.Errorf("failed to write end json array to pipe: %w", err)
				cancelFunc(err)
				_ = pw.CloseWithError(err)
				return
			}
		} else {
			_, err = pw.Write([]byte("]"))
			if err != nil {
				err := fmt.Errorf("failed to write end json array to pipe: %w", err)
				cancelFunc(err)
				_ = pw.CloseWithError(err)
				return
			}
		}
	}()

	// Now allow the consumer to read from the pipe
	return consumer(ctx, pr)
}
