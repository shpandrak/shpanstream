package shpanstream

import (
	"context"
	"net/http"
)

func StreamJsonToHttpResponseWriter[T any](ctx context.Context, w http.ResponseWriter, stream Stream[T]) error {
	return StreamJsonToWriterWithInit(ctx, w, stream, func() error {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		return nil
	})
}
