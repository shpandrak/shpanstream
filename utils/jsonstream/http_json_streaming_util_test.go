package jsonstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/stretchr/testify/require"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type tstData struct {
	Str string `json:"str"`
	Int int    `json:"int"`
}

// Creating an infinite stream of tstData with ascending integers
func createTestInfiniteStream() stream.Stream[tstData] {
	idx := 0
	return stream.NewSimpleStream[tstData](func(ctx context.Context) (tstData, error) {
		select {
		case <-ctx.Done():
			return util.DefaultValue[tstData](), ctx.Err()
		default:
			idx++
			return tstData{
				Str: "hi",
				Int: idx,
			}, nil
		}
	})
}

func TestStreamingAcrossHttp(t *testing.T) {

	var wg sync.WaitGroup
	// Make the server process the post before we start the get
	wg.Add(1)

	// Setting up the test server
	mux := http.NewServeMux()

	var requestStream stream.Stream[tstData]
	// The POST request will accept a stream of JSON objects in its body and store it on the request stream variable
	mux.HandleFunc("/post", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancelFunc := context.WithCancel(r.Context())
		requestStream = ReadJsonArray[tstData](func(ctx context.Context) (io.ReadCloser, error) {
			return r.Body, nil
		}).
			// When the request stream is closed, cancel the context and return the post request
			// We can easily do that using WithAdditionalLifecycle to attach handlers for stream lifecycle events
			WithAdditionalLifecycle(stream.NewLifecycle(
				func(ctx context.Context) error {
					log.Println("Starting stream")
					return nil
				},
				func() {
					// When the request stream is closed
					cancelFunc()
				}))

		// We can use the stream in the GET handler
		wg.Done()

		// The post request keeps streaming the request body until the stream is closed (no more data)
		select {
		case <-ctx.Done():
			w.WriteHeader(http.StatusOK)
		}
	})

	// The GET request will take the request stream as it streams from the POST request,
	//transform it and send it back to the client via the response body
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		// Make sure the post request has started before we start the get request
		wg.Wait()

		_ = StreamJsonToHttpResponseWriter(
			r.Context(),
			w,
			stream.Map(
				requestStream,
				func(v tstData) tstData {
					return tstData{
						Str: "bye",
						Int: v.Int,
					}
				},
			),
		)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	// We start the POST request in a goroutine to avoid blocking the test,
	//since it will stay open until the stream is closed
	go func() {

		postResp, err := ExecuteStreamingHttpPostRequest(
			context.Background(),
			http.DefaultClient,
			server.URL+"/post",
			createTestInfiniteStream().
				// Remove the "Limit" to experience zero-memory infinite streaming
				Limit(1000),
		)
		require.NoError(t, err)
		_ = postResp.Body.Close()
		require.Equal(t, http.StatusOK, postResp.StatusCode)
	}()

	// Now we can use the manipulated data stream using the http GET command and it will stream while it gets pushed
	getResp, err := http.Get(server.URL + "/get")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	// Print the transformed response body as it streams back to the client
	err = ReadJsonArray[tstData](func(ctx context.Context) (io.ReadCloser, error) {
		return getResp.Body, nil
	}).Consume(context.Background(), func(d tstData) {
		fmt.Printf("Received: %v\n", d)
	})
	require.NoError(t, err)
}

// failAt yields 1..5 and fails when it reaches element n (1-based).
func failAt(n int) stream.Stream[int] {
	return stream.MapWithErr(stream.Just(1, 2, 3, 4, 5), func(v int) (int, error) {
		if v == n {
			return 0, errors.New("boom at element")
		}
		return v, nil
	})
}

func TestStreamJsonToHttpResponseWriter_FailBeforeThresholdWritesNothing(t *testing.T) {
	rec := httptest.NewRecorder()
	err := StreamJsonToHttpResponseWriter(context.Background(), rec, failAt(3))
	require.EqualError(t, err, "boom at element")
	var committed *ResponseCommittedError
	require.False(t, errors.As(err, &committed))
	require.Equal(t, 0, rec.Body.Len())
	require.Empty(t, rec.Header().Get("Content-Type"), "header map left as the caller had it")

	// the caller's error handler can still pick the status
	http.Error(rec, err.Error(), http.StatusInternalServerError)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
	require.Equal(t, "boom at element\n", rec.Body.String())
}

func TestStreamJsonToHttpResponseWriter_FailAfterThresholdIsCommittedError(t *testing.T) {
	// 0 is the opt-out that reproduces the pre-buffering behaviour: status goes out with element 1
	for _, threshold := range []int{0, 2} {
		t.Run(fmt.Sprintf("threshold=%d", threshold), func(t *testing.T) {
			rec := httptest.NewRecorder()
			err := StreamJsonToHttpResponseWriter(context.Background(), rec, failAt(3), WithCommitThreshold(threshold))
			var committed *ResponseCommittedError
			require.True(t, errors.As(err, &committed))
			require.EqualError(t, committed.Unwrap(), "boom at element")
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.Equal(t, "[1,2", rec.Body.String(), "nothing written after the failure")
		})
	}
}

func TestStreamJsonToHttpResponseWriter_SuccessOutputUnchanged(t *testing.T) {
	for _, tc := range []struct {
		name string
		s    stream.Stream[int]
		body string
	}{
		{"empty", stream.Empty[int](), "[]"},
		{"small", stream.Just(1, 2, 3), "[1,2,3]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			require.NoError(t, StreamJsonToHttpResponseWriter(context.Background(), rec, tc.s))
			require.Equal(t, http.StatusOK, rec.Code)
			require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			require.Equal(t, tc.body, rec.Body.String())
		})
	}
}

func TestStreamJsonToHttpResponseWriter_PresetContentType(t *testing.T) {
	// a middleware default set before the handler runs
	t.Run("success overrides it", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rec.Header().Set("Content-Type", "text/plain; charset=utf-8")
		require.NoError(t, StreamJsonToHttpResponseWriter(context.Background(), rec, stream.Just(1)))
		require.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	})
	t.Run("failure before commit leaves it", func(t *testing.T) {
		rec := httptest.NewRecorder()
		rec.Header().Set("Content-Type", "text/plain; charset=utf-8")
		err := StreamJsonToHttpResponseWriter(context.Background(), rec, failAt(3))
		require.EqualError(t, err, "boom at element")
		require.Equal(t, "text/plain; charset=utf-8", rec.Header().Get("Content-Type"))
	})
}

func TestStreamJsonToHttpResponseWriter_LargeSuccessStreamsValidJson(t *testing.T) {
	rec := httptest.NewRecorder()
	// 10 KiB of elements at a threshold of 1 KiB: commit happens mid-stream
	err := StreamJsonToHttpResponseWriter(
		context.Background(),
		rec,
		createTestInfiniteStream().Limit(1000),
		WithCommitThreshold(1<<10),
	)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code)
	var got []tstData
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 1000)
	require.Equal(t, 1000, got[999].Int)
}

// brokenWriter fails every body write, like a client that hung up.
type brokenWriter struct{ *httptest.ResponseRecorder }

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("write: broken pipe") }

func TestStreamJsonToHttpResponseWriter_CommitWriteFailureIsCommittedError(t *testing.T) {
	rec := brokenWriter{httptest.NewRecorder()}
	err := StreamJsonToHttpResponseWriter(context.Background(), rec, stream.Just(1, 2, 3))
	var committed *ResponseCommittedError
	require.True(t, errors.As(err, &committed), "status was sent before the body write failed")
	require.EqualError(t, committed.Unwrap(), "write: broken pipe")
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestStreamJsonToHttpResponseWriter_PostCommitFailureAbortsConnection runs a real server, because
// httptest.ResponseRecorder does not support write deadlines. The handler does what oapi-codegen's
// default ResponseErrorHandlerFunc does: http.Error after the failure. The client must see a
// broken response, not a 200 with the error text appended to the truncated array.
func TestStreamJsonToHttpResponseWriter_PostCommitFailureAbortsConnection(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%v", h2), func(t *testing.T) {
			// ~230 KiB of good elements are flushed to the client before the failure
			s := stream.MapWithErr(createTestInfiniteStream().Limit(10001), func(v tstData) (tstData, error) {
				if v.Int == 10001 {
					return v, errors.New("boom")
				}
				return v, nil
			})
			handlerErrs := make(chan error, 1)
			srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				err := StreamJsonToHttpResponseWriter(r.Context(), w, s)
				handlerErrs <- err
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}))
			if h2 {
				srv.EnableHTTP2 = true
				srv.StartTLS()
			} else {
				srv.Start()
			}
			defer srv.Close()

			resp, err := srv.Client().Get(srv.URL)
			require.NoError(t, err)
			defer resp.Body.Close()
			body, readErr := io.ReadAll(resp.Body)

			var committed *ResponseCommittedError
			require.True(t, errors.As(<-handlerErrs, &committed), "handler still receives the error")
			require.Equal(t, http.StatusOK, resp.StatusCode, "status was already on the wire")
			require.Error(t, readErr, "client must see the body as broken")
			require.NotContains(t, string(body), "stream failed", "handler's http.Error never reaches the client")
			require.True(t, bytes.HasPrefix(body, []byte(`[{"str":`)), "good prefix was delivered")
		})
	}
}
