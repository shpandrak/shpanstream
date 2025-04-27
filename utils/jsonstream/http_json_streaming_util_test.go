package jsonstream

import (
	"context"
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
			// We can easily do that using WithAdditionalStreamLifecycle to attach handlers for stream lifecycle events
			WithAdditionalStreamLifecycle(stream.NewStreamLifecycle(
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
			stream.MapStream(
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
