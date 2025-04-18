package shpanstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

type jsonArrayStreamProvider[T any] struct {
	readCloserProvider func(ctx context.Context) (io.ReadCloser, error)
	readCloser         io.ReadCloser
	jsonDecoder        *json.Decoder
}

func ReadJsonArray[T any](readCloserProvider func(ctx context.Context) (io.ReadCloser, error)) Stream[T] {
	return NewStream[T](&jsonArrayStreamProvider[T]{
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
		return defaultValue[T](), ctx.Err()
	default:
	}

	// Read key-value pairs one at a time
	if j.jsonDecoder.More() {
		var parsedElement T
		// Decode the next element
		if err := j.jsonDecoder.Decode(&parsedElement); err != nil {
			return defaultValue[T](), fmt.Errorf("error parsing array element: %w", err)
		}

		// Process element
		return parsedElement, nil
	} else {

		// Read closing array token
		t, err := j.jsonDecoder.Token()
		if err != nil {
			return defaultValue[T](), err
		}
		if delim, ok := t.(json.Delim); !ok || delim != ']' {
			return defaultValue[T](), fmt.Errorf("expected end of json array, got %v", t)
		}

		return defaultValue[T](), io.EOF
	}

}
