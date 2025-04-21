package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/shpandrak/shpanstream"
	"github.com/shpandrak/shpanstream/examples/timeseries"
	"github.com/shpandrak/shpanstream/integrations/ws"
	"log"
	"os"
	"time"
)

type StockDto struct {
	Data []struct {
		//C          interface{} `json:"c"`
		LastPrice  float64 `json:"p"`
		Symbol     string  `json:"s"`
		TimeMillis int64   `json:"t"`
		Volume     float64 `json:"v"`
	} `json:"data"`
	Type string `json:"type"`
}

func main() {
	apiKey := os.Getenv("FINNHUB_API_KEY")
	if apiKey == "" {
		log.Fatal("Missing FINNHUB_API_KEY")
	}

	// Set timeout so it won't run forever
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*20)
	defer cancelFunc()

	// Align the stream to 3 seconds getting weighted average for price
	err := timeseries.AlignStream(

		// Map the source to a stream of timeseries.TsRecord[float64] (while filtering irrelevant data)
		shpanstream.MapStreamWhileFiltering(
			// Get the websocket stocks stream
			ws.CreateJsonStreamFromWebSocket[StockDto](createWebSocketFactory(apiKey)),

			// Map to timeseries.TsRecord[float64]
			mapStockToTimeSeries,
		),
		3*time.Second,
	).
		// Print the output to stdout
		Consume(ctx, func(t timeseries.TsRecord[float64]) {
			fmt.Printf("%+v\n", t)
		})

	if err != nil {
		if ctx.Err() != nil {
			log.Println("Done consuming the stream")
		} else {
			panic(err)
		}
	}

}

func mapStockToTimeSeries(m StockDto) *timeseries.TsRecord[float64] {
	if len(m.Data) > 0 {
		return &timeseries.TsRecord[float64]{
			Timestamp: time.UnixMilli(m.Data[0].TimeMillis),
			Value:     m.Data[0].LastPrice,
		}
	}
	return nil
}

func createWebSocketFactory(apiKey string) func(ctx context.Context) (*websocket.Conn, error) {
	return func(ctx context.Context) (*websocket.Conn, error) {
		w, _, err := websocket.DefaultDialer.Dial("wss://ws.finnhub.io?token="+apiKey, nil)
		if err != nil {
			panic(err)
		}

		//symbols := []string{"AAPL", "AMZN", "BINANCE:BTCUSDT", "IC MARKETS:1"}
		symbols := []string{"BINANCE:BTCUSDT"}
		for _, s := range symbols {
			msg, _ := json.Marshal(map[string]interface{}{"type": "subscribe", "symbol": s})
			err = w.WriteMessage(websocket.TextMessage, msg)
			if err != nil {
				return nil, fmt.Errorf("error writing message to websocket: %w", err)
			}
		}
		return w, nil

	}
}
