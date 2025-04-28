package stream

import (
	"fmt"
)

func ExampleFromChannel() {
	type tstLogEvent struct {
		Level string
		Msg   string
	}

	ch := make(chan tstLogEvent, 3)
	go func() {
		ch <- tstLogEvent{Level: "info", Msg: "Oh yes1"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no1"}
		ch <- tstLogEvent{Level: "info", Msg: "Oh yes2"}
		ch <- tstLogEvent{Level: "info", Msg: "Oh yes3"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no2"}
		ch <- tstLogEvent{Level: "info", Msg: "Oh yes4"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no3"}
		ch <- tstLogEvent{Level: "info", Msg: "Oh yes5"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no4"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no5"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no6"}
		ch <- tstLogEvent{Level: "info", Msg: "Oh yes6"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no7"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no8"}
		ch <- tstLogEvent{Level: "error", Msg: "Oh no9"}
		ch <- tstLogEvent{Level: "info", Msg: "Oh yes7"}

		// Closing the stream to signal that the stream is closed
		close(ch)
	}()

	// Output:
	// [Oh no4,Oh no5,Oh no6 Oh no7,Oh no8,Oh no9]
	fmt.Println(Map(
		Window(
			FromChannel(ch),
			3,
			WithSlidingWindowStepOption(1),
		).Filter(func(currWindow []tstLogEvent) bool {
			return Just(currWindow...).
				Filter(func(e tstLogEvent) bool {
					return e.Level == "error"
				}).MustCount() == 3
		}),
		func(currWindow []tstLogEvent) string {
			return MustReduce(
				Just(currWindow...),
				"",
				func(acc string, e tstLogEvent) string {
					if acc == "" {
						return fmt.Sprintf("%s", e.Msg)
					} else {
						return fmt.Sprintf("%s,%s", acc, e.Msg)
					}
				},
			)
		},
	).MustCollect())

}
