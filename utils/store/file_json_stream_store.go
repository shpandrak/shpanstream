package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/shpandrak/shpanstream/integrations/file"
	"github.com/shpandrak/shpanstream/stream"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

type fileJsonStreamStore[T any] struct {
	fileMu   sync.RWMutex
	filePath string
}

func (fss *fileJsonStreamStore[T]) Put(ctx context.Context, value T) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:

		fss.fileMu.Lock()
		defer fss.fileMu.Unlock()
		// Open the file in append mode
		f, err := os.OpenFile(fss.filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
		if err != nil {
			return err
		}
		defer func(fileToClose *os.File) {
			err := fileToClose.Close()
			if err != nil {
				slog.Warn(fmt.Sprintf("error closing stream file %s after writting: %v", fss.filePath, err))
			}
		}(f)

		err = doAppendToFile(value, f)
		if err != nil {
			return err
		}

		// Sync the file to ensure data is written to disk
		if err = f.Sync(); err != nil {
			return err
		}

		return nil
	}
}

func doAppendToFile[T any](value T, file *os.File) error {
	record, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal record while persisting to json stream store file %s : %w", file.Name(), err)
	}

	// Append a newline, marking this is a record end, record must not contain newline
	if _, err = file.Write(append(record, '\n')); err != nil {
		return fmt.Errorf("failed to write to file while persisting to json stream store file %s : %w", file.Name(), err)
	}
	return nil
}

func (fss *fileJsonStreamStore[T]) PutAll(ctx context.Context, values stream.Stream[T]) error {

	f, err := os.OpenFile(fss.filePath, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer func(fileToClose *os.File) {
		err := fileToClose.Close()
		if err != nil {
			slog.Warn(fmt.Sprintf("error closing stream file %s after writting: %v", fss.filePath, err))
		}
	}(f)

	err = values.ConsumeWithErr(
		ctx,
		func(entry T) error {
			return doAppendToFile(entry, f)
		},
	)
	if err != nil {
		return err
	}
	// Sync the file to ensure data is written to disk
	if err = f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file while persisting to json stream store file %s : %w", f.Name(), err)
	}
	return nil
}

func (fss *fileJsonStreamStore[T]) ReadStream(reverse bool) stream.Stream[T] {

	return stream.MapWhileFiltering(
		file.StreamFromFile(fss.filePath, reverse),
		func(src []byte) *T {
			var result T
			err := json.Unmarshal(src, &result)

			// Skip malformed records, this might happen if the process crashed while writing a record,
			//we guarantee newline, so next line should be fine
			if err != nil {
				slog.Warn(fmt.Sprintf(
					"failed to unmarshal record while reading from json stream store file %s : %v", fss.filePath, err,
				))
				return nil
			}
			return &result
		},
	)
}

func NewFileJsonStreamStore[T any](filePath string) (JsonStreamStore[T], error) {
	dir := filepath.Dir(filePath)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		// Create the directory structure
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create directories: %w", err)
		}
	}

	return &fileJsonStreamStore[T]{
		filePath: filePath,
	}, nil
}
