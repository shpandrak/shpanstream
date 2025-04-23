package shpanstream

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestWindow_NoOverlap(t *testing.T) {
	require.Equal(
		t,
		[][]int{
			{1, 2, 3},
			{4, 5, 6},
		},
		Window(Just(1, 2, 3, 4, 5, 6), 3).MustCollect(),
	)
}

func TestWindow_EmitPartialStep(t *testing.T) {

	// Without the omit partial step option, the last window will be emitted
	require.Equal(
		t,
		[][]int{
			{1, 2, 3},
			{4, 5, 6},
			{7},
		},
		Window(Just(1, 2, 3, 4, 5, 6, 7), 3).MustCollect(),
	)

	// With the omit partial step option, the last window will be emitted
	require.Equal(
		t,
		[][]int{
			{1, 2, 3},
			{4, 5, 6},
		},
		Window(Just(1, 2, 3, 4, 5, 6, 7), 3, WithOmitLastPartialWindowOption()).MustCollect(),
	)
}

func TestWindow_WithOverlap(t *testing.T) {
	require.Equal(
		t,
		[][]int{
			{1, 2, 3},
			{2, 3, 4},
			{3, 4, 5},
		},
		Window(Just(1, 2, 3, 4, 5), 3, WithSlidingWindowStepOption(1)).MustCollect(),
	)
}

func TestStepIncompleteWindow(t *testing.T) {
	// The second window has fewer elements
	require.Equal(
		t,
		[][]int{
			{1, 2, 3, 4, 5},
			{4, 5, 6, 7},
		},
		Window(Just(1, 2, 3, 4, 5, 6, 7), 5, WithSlidingWindowStepOption(3)).MustCollect(),
	)

	// Omit the last partial window
	require.Equal(
		t,
		[][]int{
			{1, 2, 3, 4, 5},
		},
		Window(Just(1, 2, 3, 4, 5, 6, 7), 5, WithSlidingWindowStepOption(3), WithOmitLastPartialWindowOption()).
			MustCollect(),
	)
}

func TestWindow_Errors(t *testing.T) {
	errStream := ErrorStream[int](errors.New("boom"))
	windowed := Window(errStream, 3)

	_, err := windowed.Collect(context.Background())
	require.Error(t, err)
	require.ErrorContains(t, err, "boom")
}

func TestWindow_InvalidConfig(t *testing.T) {
	// size = 0 should fail
	_, err := Window(Just(1, 2, 3), 0).Collect(context.Background())
	require.Error(t, err)

	// step = 0 should fail
	_, err = Window(Just(1, 2, 3), 3, WithSlidingWindowStepOption(0)).Collect(context.Background())
	require.Error(t, err)

	// step larger than window should fail
	_, err = Window(Just(1, 2, 3), 3, WithSlidingWindowStepOption(4)).Collect(context.Background())
	require.Error(t, err)
}

func TestWindow_MaxOverlap(t *testing.T) {
	require.Equal(
		t,
		[][]int{
			{1, 2},
			{2, 3},
			{3, 4},
		},
		Window(Just(1, 2, 3, 4), 2, WithSlidingWindowStepOption(1)).MustCollect(),
	)
}

func TestWindow_SingleFullWindow(t *testing.T) {
	require.Equal(
		t,
		[][]int{
			{1, 2, 3},
		},
		Window(Just(1, 2, 3), 3).MustCollect(),
	)
}

func TestWindow_TooShort(t *testing.T) {
	require.Equal(
		t,
		[][]int{
			{1, 2},
		},
		Window(Just(1, 2), 3).MustCollect(),
	)

	// With omit last partial window
	require.Len(
		t,
		Window(Just(1, 2), 3, WithOmitLastPartialWindowOption()).MustCollect(),
		0,
	)
}

func TestWindow_Of_One(t *testing.T) {
	// for smart asses, this is a valid case
	require.Equal(
		t,
		[][]int{
			{1},
			{2},
			{3},
		},
		Window(Just(1, 2, 3), 1).MustCollect(),
	)

}

func TestEmptySource(t *testing.T) {
	// for smart asses, this is a valid case
	require.Len(
		t,
		Window(EmptyStream[int](), 10).MustCollect(),
		0,
	)
}

// ExampleWindow demonstrates how to use the Window function with a sliding window.
// It returns the first window of 3 elements that contains at least 2 even numbers.
func ExampleWindow() {
	results := Window(
		Just(1, 3, 3, 5, 11, 6, 7, 8, 8, 8, 10, 10, 12, 13, 14, 15, 16, 17, 18, 19),
		3,
		WithSlidingWindowStepOption(1),
	).
		Filter(func(currWindow []int) bool {
			return Just(currWindow...).
				Filter(func(src int) bool {
					return src%2 == 0
				}).MustCount() >= 2
		}).FindFirst().MustGet()

	// Print the result
	fmt.Println(results)
	// Output: [6 7 8]
}

func Test_Alert(t *testing.T) {
	type alert struct {
		Severity int
		Message  string
	}

	alertsStream := Just([]alert{
		{Severity: 4, Message: "Alert 1"},
		{Severity: 2, Message: "Alert 2"},
		{Severity: 1, Message: "Alert 3"},
		{Severity: 3, Message: "Alert 3"},
		{Severity: 4, Message: "Alert 4"},
		{Severity: 3, Message: "Alert 5"},
		{Severity: 4, Message: "Alert 6"},
	}...)

	// Constructing a message first time server alerts (severity > 2) occur 2 times in the stream,
	// within a sliding window of size 3
	require.Equal(
		t,
		"Messages:\nAlert 3\nAlert 4",
		MapLazy(

			// Creating a sliding window of alerts of size 3 with a sliding step of 1
			Window(
				alertsStream,
				3,
				WithSlidingWindowStepOption(1),
			).

				// Filtering the windows to only include those with at least 2 severe alerts
				Filter(func(currWindow []alert) bool {
					return Just(currWindow...).
						Filter(func(a alert) bool {
							return a.Severity > 2
						}).MustCount() >= 2
				}).

				// Return the first occurrence of the violation as it happens
				FindFirst(),

			// Mapping the filtered windows to a the expected summary message
			func(alertsInWindow []alert) string {

				// Reducing the sever alerts to a single message with individual alert messages
				return MustReduce(
					MapStreamWhileFiltering(
						Just(alertsInWindow...),
						func(a alert) *string {
							// Only for the severe alerts
							if a.Severity > 2 {
								return &a.Message
							} else {
								return nil
							}
						},
					),
					"Messages:",
					func(acc string, a string) string {
						return acc + "\n" + a
					},
				)
			},
		).MustGet(),
	)
}
