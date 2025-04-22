package jsonstream

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/internal/util"
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

func createTestInfiniteStream() shpanstream.Stream[tstData] {
	idx := 0
	return shpanstream.NewSimpleStream[tstData](func(ctx context.Context) (tstData, error) {

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

// Shared test state
var (
	mu   sync.Mutex
	data shpanstream.Stream[tstData]
	wg   sync.WaitGroup
)

func postHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancelFunc := context.WithCancel(r.Context())
	mu.Lock()
	data = ReadJsonArray[tstData](func(ctx context.Context) (io.ReadCloser, error) {
		return r.Body, nil
	}).WithAdditionalStreamLifecycle(shpanstream.NewStreamLifecycle(
		func(ctx context.Context) error {
			log.Println("Starting stream")
			return nil
		},
		func() {
			cancelFunc()
		}))
	mu.Unlock()
	wg.Done()

	select {
	case <-ctx.Done():
		w.WriteHeader(http.StatusOK)
	}
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	clientStream := data
	mu.Unlock()
	_ = StreamJsonToHttpResponseWriter(
		r.Context(),
		w,
		shpanstream.MapStream(
			clientStream,
			func(v tstData) tstData {
				return tstData{
					Str: "bye",
					Int: v.Int,
				}
			},
		),
	)

}
func TestExecuteStreamingHttpPostRequest(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/post", postHandler)
	mux.HandleFunc("/get", getHandler)

	server := httptest.NewServer(mux)
	defer server.Close()

	// Make the server process the post before we start the get
	wg.Add(1)

	go func() {

		postResp, err := ExecuteStreamingHttpPostRequest(
			context.Background(),
			http.DefaultClient,
			server.URL+"/post",
			createTestInfiniteStream().Limit(1000),
		)
		require.NoError(t, err)
		_ = postResp.Body.Close()
		require.Equal(t, http.StatusOK, postResp.StatusCode)
	}()

	// Wait for the POST request to be started
	wg.Wait()

	// Now we can use the data stream in the GET handler
	getResp, err := http.Get(server.URL + "/get")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	err = ReadJsonArray[tstData](func(ctx context.Context) (io.ReadCloser, error) {
		return getResp.Body, nil
	}).Consume(context.Background(), func(d tstData) {
		fmt.Printf("Received: %v\n", d)
	})
	require.NoError(t, err)

}
