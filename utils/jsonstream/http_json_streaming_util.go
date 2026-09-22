package jsonstream

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/httputil"
)

// DefaultCommitThreshold is the number of body bytes StreamJsonToHttpResponseWriter buffers
// before sending the status. Streams that fail before reaching it return their error with
// nothing written, so the caller can still respond with a proper error status.
const DefaultCommitThreshold = 32 << 10

type httpStreamConfig struct {
	commitThreshold int
}

type HttpStreamOption func(*httpStreamConfig)

// WithCommitThreshold overrides DefaultCommitThreshold. 0 sends the status with the first
// element, which is the lowest-latency choice for slow trickle streams but means any later
// failure is reported as a ResponseCommittedError.
func WithCommitThreshold(bytes int) HttpStreamOption {
	return func(c *httpStreamConfig) { c.commitThreshold = bytes }
}

// ResponseCommittedError wraps a stream error that happened after the status and part of the
// body had already been sent. The body is a truncated JSON array and cannot be repaired.
// StreamJsonToHttpResponseWriter has already expired the response's write deadline where the
// writer supports it, so the connection is torn down and nothing an error handler writes reaches
// the client. Handlers should log the cause and return. Match it with errors.As.
type ResponseCommittedError struct {
	Err error
}

func (e *ResponseCommittedError) Error() string {
	return "stream failed after the http response was committed: " + e.Err.Error()
}

func (e *ResponseCommittedError) Unwrap() error { return e.Err }

// StreamJsonToHttpResponseWriter writes the stream as a JSON array to w with status 200.
//
// The status and body are held back until DefaultCommitThreshold bytes have been produced (see
// WithCommitThreshold). A failure before that returns the stream's error with nothing written.
// A failure after that tears down the connection through http.ResponseController where possible
// and returns a *ResponseCommittedError.
func StreamJsonToHttpResponseWriter[T any](
	ctx context.Context,
	w http.ResponseWriter,
	stream stream.Stream[T],
	opts ...HttpStreamOption,
) error {
	cfg := httpStreamConfig{commitThreshold: DefaultCommitThreshold}
	for _, o := range opts {
		o(&cfg)
	}
	// The buffered writer holds headers too, so before commit w's header map is untouched and an
	// error response is not mislabeled as JSON.
	bw := httputil.NewBufferedResponseWriter(w, cfg.commitThreshold)
	err := StreamJsonToWriterWithInit(ctx, bw, stream, func() error {
		bw.Header().Set("Content-Type", "application/json")
		bw.WriteHeader(http.StatusOK)
		return nil
	})
	if err != nil {
		if bw.Committed() {
			return committed(w, err)
		}
		return err
	}
	// Commit sends the status before the body, so a write failure here is also post-commit.
	if err := bw.Commit(); err != nil {
		return committed(w, err)
	}
	return nil
}

// committed wraps a post-commit failure. The client already holds a 200 and a truncated array, so
// the only correct outcome is a connection the client sees as broken. Expiring the write deadline
// does that on both HTTP/1.1 (connection closed before the terminating chunk) and HTTP/2
// (RST_STREAM) without a panic, so the error still flows back to the caller's error handler, and
// anything that handler writes afterwards never reaches the client. Writers that do not support
// deadlines (recorders, wrappers without Unwrap) are left alone.
func committed(w http.ResponseWriter, err error) error {
	_ = http.NewResponseController(w).SetWriteDeadline(time.Now())
	return &ResponseCommittedError{Err: err}
}

func ExecuteStreamingHttpPostRequest[T any](
	ctx context.Context,
	client *http.Client,
	url string,
	stream stream.Stream[T],
) (*http.Response, error) {

	return StreamJsonAsReaderAndReturn(ctx, stream, func(ctx context.Context, r io.Reader) (*http.Response, error) {
		// Create a new HTTP request with the JSON payload
		req, err := http.NewRequestWithContext(ctx, "POST", url, r)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		response, err := client.Do(req)
		return response, err
	})

}
