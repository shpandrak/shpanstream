// Package httputil holds small net/http helpers used by the shpanstream HTTP integrations.
package httputil

import (
	"bytes"
	"maps"
	"net/http"
)

// BufferedResponseWriter wraps an http.ResponseWriter and holds back the status line, headers and
// body until more than threshold bytes have been written, Commit is called, or Flush is called.
//
// Until then nothing reaches the underlying writer, so a caller that hits an error midway can
// still send a different status with its own headers through Unwrap(). Once committed it is a
// transparent pass-through, and Committed() reports that the response can no longer be changed.
//
// A threshold of 0 commits on the first non-empty Write. The buffer grows lazily, so small
// responses never allocate threshold bytes. Like http.ResponseWriter, it is not safe for
// concurrent use.
type BufferedResponseWriter struct {
	w         http.ResponseWriter
	threshold int
	status    int
	header    http.Header // pre-commit working copy of w.Header(); nil until first Header() call
	buf       bytes.Buffer
	committed bool
}

func NewBufferedResponseWriter(w http.ResponseWriter, threshold int) *BufferedResponseWriter {
	return &BufferedResponseWriter{w: w, threshold: threshold}
}

// Header returns the header map. Before Commit it is a copy of the underlying map, taken on the
// first call, so headers set here reach the client only when the buffered response does. Commit
// replaces the underlying map with this copy. After commit it is the underlying map itself.
func (b *BufferedResponseWriter) Header() http.Header {
	if b.committed {
		return b.w.Header()
	}
	if b.header == nil {
		b.header = b.w.Header().Clone()
	}
	return b.header
}

// WriteHeader records the status until Commit. After commit it passes through. Informational
// 1xx codes are never final, so they always pass through immediately, as net/http sends them,
// carrying the underlying writer's headers rather than any set through Header() before commit.
func (b *BufferedResponseWriter) WriteHeader(code int) {
	if b.committed || (code >= 100 && code <= 199) {
		b.w.WriteHeader(code)
		return
	}
	b.status = code
}

func (b *BufferedResponseWriter) Write(p []byte) (int, error) {
	if b.committed {
		return b.w.Write(p)
	}
	if b.buf.Len()+len(p) > b.threshold {
		if err := b.Commit(); err != nil {
			return 0, err
		}
		return b.w.Write(p)
	}
	return b.buf.Write(p)
}

// Commit sends the recorded headers, status (200 if none was set) and buffered bytes. It is
// idempotent.
func (b *BufferedResponseWriter) Commit() error {
	if b.committed {
		return nil
	}
	b.committed = true
	if b.header != nil {
		h := b.w.Header()
		clear(h)
		maps.Copy(h, b.header)
		b.header = nil
	}
	if b.status == 0 {
		b.status = http.StatusOK
	}
	b.w.WriteHeader(b.status)
	if b.buf.Len() == 0 {
		return nil
	}
	_, err := b.w.Write(b.buf.Bytes())
	b.buf = bytes.Buffer{} // release the backing array; the writer is a pass-through from here on
	return err
}

// Committed reports whether the status and any body bytes have reached the underlying writer.
func (b *BufferedResponseWriter) Committed() bool { return b.committed }

// Flush commits and then flushes the underlying writer if it supports http.Flusher.
func (b *BufferedResponseWriter) Flush() { _ = b.FlushError() }

// FlushError is Flush with the error reported, for http.ResponseController.Flush. A failed Commit
// (the buffered body did not reach the wire) is returned rather than dropped.
func (b *BufferedResponseWriter) FlushError() error {
	if err := b.Commit(); err != nil {
		return err
	}
	switch f := b.w.(type) {
	case interface{ FlushError() error }:
		return f.FlushError()
	case http.Flusher:
		f.Flush()
	}
	return nil
}

// Unwrap returns the wrapped writer, for http.ResponseController and for writing an error
// response when the buffered one is abandoned before Commit.
func (b *BufferedResponseWriter) Unwrap() http.ResponseWriter { return b.w }
