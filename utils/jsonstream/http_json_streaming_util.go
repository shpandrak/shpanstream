package jsonstream

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"io"
	"net/http"
)

func StreamJsonToHttpResponseWriter[T any](ctx context.Context, w http.ResponseWriter, stream stream.Stream[T]) error {
	return StreamJsonToWriterWithInit(ctx, w, stream, func() error {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		return nil
	})
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
