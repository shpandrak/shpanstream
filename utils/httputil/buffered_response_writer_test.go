package httputil

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// flushRecorder records whether Flush reached the underlying writer.
type flushRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flushRecorder) Flush() { f.flushed = true }

func TestBufferedResponseWriter_HoldsUntilCommit(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := NewBufferedResponseWriter(rec, 100)

	bw.Header().Set("X-Test", "1")
	bw.WriteHeader(http.StatusCreated)
	_, err := bw.Write([]byte("hello"))
	require.NoError(t, err)

	require.False(t, bw.Committed())
	require.Equal(t, 0, rec.Body.Len())
	require.False(t, rec.Flushed)

	require.NoError(t, bw.Commit())
	require.True(t, bw.Committed())
	require.Equal(t, http.StatusCreated, rec.Code)
	require.Equal(t, "hello", rec.Body.String())
	require.Equal(t, "1", rec.Header().Get("X-Test"))

	// Second commit writes nothing more
	require.NoError(t, bw.Commit())
	require.Equal(t, "hello", rec.Body.String())
}

func TestBufferedResponseWriter_CrossingThresholdCommits(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := NewBufferedResponseWriter(rec, 10)

	_, err := bw.Write([]byte("12345"))
	require.NoError(t, err)
	require.False(t, bw.Committed())

	// 5 + 6 > 10: commit, then this write goes straight through
	_, err = bw.Write([]byte("678901"))
	require.NoError(t, err)
	require.True(t, bw.Committed())
	require.Equal(t, http.StatusOK, rec.Code, "no WriteHeader means 200")
	require.Equal(t, "12345678901", rec.Body.String())

	// After commit, writes and WriteHeader pass through
	_, err = bw.Write([]byte("x"))
	require.NoError(t, err)
	require.Equal(t, "12345678901x", rec.Body.String())
}

func TestBufferedResponseWriter_ExactThresholdStaysBuffered(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := NewBufferedResponseWriter(rec, 5)
	_, err := bw.Write([]byte("12345"))
	require.NoError(t, err)
	require.False(t, bw.Committed())
}

func TestBufferedResponseWriter_ZeroThresholdCommitsOnFirstWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := NewBufferedResponseWriter(rec, 0)
	bw.WriteHeader(http.StatusAccepted)
	require.False(t, bw.Committed())
	_, err := bw.Write([]byte("a"))
	require.NoError(t, err)
	require.True(t, bw.Committed())
	require.Equal(t, http.StatusAccepted, rec.Code)
	require.Equal(t, "a", rec.Body.String())
}

func TestBufferedResponseWriter_FlushCommitsAndFlushes(t *testing.T) {
	rec := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	bw := NewBufferedResponseWriter(rec, 100)
	_, _ = bw.Write([]byte("hi"))
	bw.Flush()
	require.True(t, bw.Committed())
	require.Equal(t, "hi", rec.Body.String())
	require.True(t, rec.flushed)

	// http.ResponseController reaches the underlying writer through Unwrap
	require.Same(t, http.ResponseWriter(rec), bw.Unwrap())
}

func TestBufferedResponseWriter_UncommittedErrorCanStillSetStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	bw := NewBufferedResponseWriter(rec, 1<<10)
	bw.Header().Set("Content-Type", "application/json")
	bw.Header().Set("Cache-Control", "public, max-age=3600")
	bw.WriteHeader(http.StatusOK)
	_, _ = bw.Write([]byte(`{"partial":`))

	// caller hits an error and writes it to the underlying writer instead
	require.False(t, bw.Committed())
	http.Error(bw.Unwrap(), "boom", http.StatusInternalServerError)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, "boom\n", rec.Body.String())
	require.True(t, strings.HasPrefix(rec.Header().Get("Content-Type"), "text/plain"))
	require.Empty(t, rec.Header().Get("Cache-Control"), "headers of the abandoned response must not leak into the error response")
}

func TestBufferedResponseWriter_HeadersHeldUntilCommit(t *testing.T) {
	rec := httptest.NewRecorder()
	rec.Header().Set("X-Middleware", "before-wrap")
	bw := NewBufferedResponseWriter(rec, 100)

	// headers already on the underlying writer are visible and preserved
	require.Equal(t, "before-wrap", bw.Header().Get("X-Middleware"))
	bw.Header().Set("Cache-Control", "no-store")
	bw.Header().Del("X-Middleware")
	require.Empty(t, rec.Header().Get("Cache-Control"), "not sent before commit")
	require.Equal(t, "before-wrap", rec.Header().Get("X-Middleware"), "not deleted before commit")

	require.NoError(t, bw.Commit())
	require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.Empty(t, rec.Header().Get("X-Middleware"))

	// after commit Header() is the live underlying map
	bw.Header().Set("Trailer-Ish", "x")
	require.Equal(t, "x", rec.Header().Get("Trailer-Ish"))
}

// codeRecorder records every WriteHeader call in order.
type codeRecorder struct {
	*httptest.ResponseRecorder
	codes []int
}

func (c *codeRecorder) WriteHeader(code int) { c.codes = append(c.codes, code) }

func TestBufferedResponseWriter_InformationalStatusPassesThrough(t *testing.T) {
	rec := &codeRecorder{ResponseRecorder: httptest.NewRecorder()}
	bw := NewBufferedResponseWriter(rec, 100)
	bw.WriteHeader(http.StatusEarlyHints)
	require.Equal(t, []int{http.StatusEarlyHints}, rec.codes, "1xx is sent immediately")
	require.False(t, bw.Committed())

	_, _ = bw.Write([]byte("body"))
	require.NoError(t, bw.Commit())
	require.Equal(t, []int{http.StatusEarlyHints, http.StatusOK}, rec.codes, "1xx is not replayed as the final status")
}

func TestBufferedResponseWriter_ReleasesBufferAfterCommit(t *testing.T) {
	bw := NewBufferedResponseWriter(httptest.NewRecorder(), 1<<10)
	_, _ = bw.Write(make([]byte, 512))
	require.NotZero(t, bw.buf.Cap())
	require.NoError(t, bw.Commit())
	require.Zero(t, bw.buf.Cap())
}

// brokenWriter fails every body write, like a client that hung up.
type brokenWriter struct{ *httptest.ResponseRecorder }

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("write: broken pipe") }

func TestBufferedResponseWriter_FlushErrorReportsFailedCommit(t *testing.T) {
	bw := NewBufferedResponseWriter(brokenWriter{httptest.NewRecorder()}, 100)
	_, err := bw.Write([]byte("hi"))
	require.NoError(t, err, "still buffered")

	// http.ResponseController prefers FlushError over Flush and surfaces the commit failure
	err = http.NewResponseController(bw).Flush()
	require.EqualError(t, err, "write: broken pipe")
	require.True(t, bw.Committed())
}
