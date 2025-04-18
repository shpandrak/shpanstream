package shpanstream

import (
	"context"
	"encoding/json"
	"io"
)

func StreamJsonToWriter[T any](ctx context.Context, w io.Writer, stream Stream[T]) error {
	return StreamJsonToWriterWithInit(ctx, w, stream, func() error {
		return nil
	})
}

func StreamJsonToWriterWithInit[T any](
	ctx context.Context,
	w io.Writer,
	stream Stream[T],
	initFunc func() error,
) error {
	first := true
	err := stream.ConsumeWithErr(ctx, func(v T) error {
		if first {
			err := initFunc()
			if err != nil {
				return err
			}

			_, err = w.Write([]byte("["))
			if err != nil {
				return err
			}
			first = false
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
