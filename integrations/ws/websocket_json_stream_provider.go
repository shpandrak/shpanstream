package ws

import (
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/shpandrak/shpanstream"
	"log"
)

type wsJsonStreamProvider[T any] struct {
	wsFactory func(ctx context.Context) (*websocket.Conn, error)
	ws        *websocket.Conn
}

func CreateJsonStreamFromWebSocket[T any](wsFactory func(ctx context.Context) (*websocket.Conn, error)) shpanstream.Stream[T] {
	return shpanstream.NewStream(&wsJsonStreamProvider[T]{
		wsFactory: wsFactory,
	})
}

func (w *wsJsonStreamProvider[T]) Open(ctx context.Context) error {
	ws, err := w.wsFactory(ctx)
	if err != nil {
		return err
	}
	w.ws = ws

	go func() {
		select {
		case <-ctx.Done():
			if w.ws != nil {
				closeErr := w.ws.Close()
				if closeErr != nil {
					log.Printf("error closing websocket: %v", closeErr)
				}
			}
		}

	}()

	return nil
}

func (w *wsJsonStreamProvider[T]) Close() {
	// no need to close the websocket here, it will be closed in the Open method when the context is done
}

func (w *wsJsonStreamProvider[T]) Emit(ctx context.Context) (T, error) {
	var ret T
	if ctx.Err() != nil {
		return ret, ctx.Err()
	}
	err := w.ws.ReadJSON(&ret)
	if err != nil {
		if ctx.Err() != nil {
			return ret, ctx.Err()
		}
		return ret, fmt.Errorf("error reading from websocket: %w", err)

	}
	return ret, nil
}
