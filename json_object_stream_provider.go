package shpanstream

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

type jsonObjectStreamProvider[T any] struct {
	readCloserProvider func(ctx context.Context) (io.ReadCloser, error)
	readCloser         io.ReadCloser
	jsonDecoder        *json.Decoder
}

func ReadJsonObject[T any](readCloserProvider func(ctx context.Context) (io.ReadCloser, error)) Stream[Entry[string, T]] {
	return NewStream(&jsonObjectStreamProvider[T]{
		readCloserProvider: readCloserProvider,
	})
}

func (j *jsonObjectStreamProvider[T]) Open(ctx context.Context) error {
	rc, err := j.readCloserProvider(ctx)
	if err != nil {
		return fmt.Errorf("failed to open stream: %w", err)
	}
	j.readCloser = rc

	j.jsonDecoder = json.NewDecoder(j.readCloser)

	// Read opening brace
	t, err := j.jsonDecoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read opening token: %w", err)
	}
	if delim, ok := t.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected start of object, got %v", t)
	}

	return nil
}

func (j *jsonObjectStreamProvider[T]) Close() {
	if j.readCloser != nil {
		j.readCloser.Close()
		j.readCloser = nil
	}
	j.jsonDecoder = nil
}

func (j *jsonObjectStreamProvider[T]) Emit(ctx context.Context) (Entry[string, T], error) {

	// Check if the ctx is done
	select {
	case <-ctx.Done():
		return defaultValue[Entry[string, T]](), ctx.Err()
	default:
	}

	// Read key-value pairs one at a time
	if j.jsonDecoder.More() {
		var parsedFieldValue T

		// Read key using Token()
		tok, err := j.jsonDecoder.Token()
		if err != nil {
			return defaultValue[Entry[string, T]](),
				fmt.Errorf("error reading key token: %w", err)
		}

		fieldName, ok := tok.(string)
		if !ok {
			return defaultValue[Entry[string, T]](),
				fmt.Errorf("expected string key for json, got %T: %v", tok, tok)
		}
		if err := j.jsonDecoder.Decode(&parsedFieldValue); err != nil {
			return defaultValue[Entry[string, T]](), fmt.Errorf("error decoding value: %w", err)
		}

		// Process fieldName and parsedFieldValue
		return Entry[string, T]{Key: fieldName, Value: parsedFieldValue}, nil
	} else {

		// Read closing brace
		t, err := j.jsonDecoder.Token()
		if err != nil {
			return defaultValue[Entry[string, T]](),
				fmt.Errorf("failed to read closing token: %w", err)
		}
		if delim, ok := t.(json.Delim); !ok || delim != '}' {
			return defaultValue[Entry[string, T]](),
				fmt.Errorf("expected end of object, got %v", t)
		}

		return defaultValue[Entry[string, T]](), io.EOF
	}

}
