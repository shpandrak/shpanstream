package file

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"io"
	"log/slog"
	"os"
)

type fsScanner interface {
	Err() error
	Bytes() []byte
	Scan() bool
}

// rawFileStreamProvider reads JSON objects of type T from a file line-by-line.
type rawFileStreamProvider struct {
	filePath              string
	file                  *os.File
	scanner               fsScanner
	fileMissingHenceEmpty bool
	reverse               bool
}

// StreamFromFile creates a lazy stream from a file path.
func StreamFromFile(filePath string, reverse bool) shpanstream.Stream[[]byte] {
	return shpanstream.NewStream(&rawFileStreamProvider{
		filePath: filePath,
		reverse:  reverse,
	})
}

// Open opens the file for reading and initializes the scanner.
func (fsp *rawFileStreamProvider) Open(_ context.Context) error {
	file, err := os.Open(fsp.filePath)
	if err != nil {

		// If no file, that's fine, it means stream is empty
		if errors.Is(err, os.ErrNotExist) {
			fsp.fileMissingHenceEmpty = true
			return nil
		}
		return err
	}

	fsp.file = file

	if fsp.reverse {
		fs, err := file.Stat()
		if err != nil {
			return err
		}
		fsp.scanner = NewReverseScanner(file, fs.Size())
		// Just get rid of last line
		fsp.scanner.Scan()
	} else {
		fsp.scanner = bufio.NewScanner(file)
	}

	return nil
}

// Close closes the file and releases any resources.
func (fsp *rawFileStreamProvider) Close() {
	if fsp.file != nil {

		err := fsp.file.Close()
		if err != nil {
			slog.Warn(fmt.Sprintf("error closing stream file %s: %v", fsp.filePath, err))
		}
		fsp.file = nil
		fsp.scanner = nil
	}
}

// Emit reads the next object of type T from the file.
func (fsp *rawFileStreamProvider) Emit(ctx context.Context) ([]byte, error) {
	if fsp.scanner == nil {
		// If we're empty, just return EOF to mark that nothing is here
		if fsp.fileMissingHenceEmpty {
			return nil, io.EOF
		}

		// Otherwise, it means emit is somehow called before Open, or after Close, which is impossible, but an error
		return nil, os.ErrClosed
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		for {
			if fsp.scanner.Scan() {
				return fsp.scanner.Bytes(), nil
			}
			if err := fsp.scanner.Err(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
	}
}
