package jsonstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
	"io"
)

type jsonArrayStreamProvider[T any] struct {
	readCloserProvider func(ctx context.Context) (io.ReadCloser, error)
	readCloser         io.ReadCloser
	jsonDecoder        *json.Decoder
}

func ReadJsonArray[T any](readCloserProvider func(ctx context.Context) (io.ReadCloser, error)) shpanstream.Stream[T] {
	return shpanstream.NewStream[T](&jsonArrayStreamProvider[T]{
		readCloserProvider: readCloserProvider,
	})
}

func (j *jsonArrayStreamProvider[T]) Open(ctx context.Context) error {
	rc, err := j.readCloserProvider(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	j.readCloser = rc

	j.jsonDecoder = json.NewDecoder(j.readCloser)

	// Check that the first token is an array start
	t, err := j.jsonDecoder.Token()
	if err != nil {
		return fmt.Errorf("failed to open JSON array stream: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '[' {
		return errors.New("input is not a JSON array")
	}

	return nil
}

func (j *jsonArrayStreamProvider[T]) Close() {
	if j.readCloser != nil {
		j.readCloser.Close()
		j.readCloser = nil
	}
	j.jsonDecoder = nil
}

func (j *jsonArrayStreamProvider[T]) Emit(ctx context.Context) (T, error) {

	// Check if the ctx is done
	select {
	case <-ctx.Done():
		return util.DefaultValue[T](), ctx.Err()
	default:
	}

	// Read key-value pairs one at a time
	if j.jsonDecoder.More() {
		var parsedElement T
		// Decode the next element

		if err := j.jsonDecoder.Decode(&parsedElement); err != nil {

			// If there is a parsing error, it is helpful to read the buffered data so this can be debugged
			bufferMessage := ""
			buffText, bufErr := io.ReadAll(j.jsonDecoder.Buffered())
			if bufErr == nil {
				bufferMessage = fmt.Sprintf(". parser buffer %s", buffText)
			}

			return util.DefaultValue[T](), fmt.Errorf("error parsing array element%s: %w", bufferMessage, err)
		}

		// Process element
		return parsedElement, nil
	} else {

		// Read closing array token
		t, err := j.jsonDecoder.Token()
		if err != nil {
			return util.DefaultValue[T](), err
		}
		if delim, ok := t.(json.Delim); !ok || delim != ']' {
			return util.DefaultValue[T](), fmt.Errorf("expected end of json array, got %v", t)
		}

		return util.DefaultValue[T](), io.EOF
	}

}
