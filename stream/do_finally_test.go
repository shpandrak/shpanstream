package stream

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// errAt returns a Map that yields values unchanged but errors when it reaches `bad`, simulating a
// provider/emit-stage error (as opposed to Error(), which fails at Open).
func errAt(s Stream[int], bad int, err error) Stream[int] {
	return MapWithErr(s, func(i int) (int, error) {
		if i == bad {
			return 0, err
		}
		return i, nil
	})
}

func TestDoFinally_SuccessFiresNilOnce(t *testing.T) {
	var calls int
	var got error
	got = errors.New("sentinel-not-called")

	out, err := Just(1, 2, 3).
		DoFinally(func(e error) { calls++; got = e }).
		Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, out)
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

func TestDoFinally_ProviderErrorFiresThatError(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	var got error

	_, err := errAt(Just(1, 2, 3, 4), 3, boom).
		DoFinally(func(e error) { calls++; got = e }).
		Collect(context.Background())

	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.ErrorIs(t, got, boom)
}

func TestDoFinally_ContextCancelMidDrainFiresContextErr(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var calls int
	var got error

	err := Just(1, 2, 3, 4, 5).
		DoFinally(func(e error) { calls++; got = e }).
		Consume(ctx, func(v int) {
			if v == 2 {
				cancel()
			}
		})

	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, 1, calls)
	require.ErrorIs(t, got, context.Canceled)
}

func TestDoFinally_LimitTruncationFiresNil(t *testing.T) {
	// DoFinally is *inside* the Limit: the wrapped node is truncated before its own EOF, which is a
	// clean stop, not an error.
	var calls int
	var got error = errors.New("sentinel-not-called")

	out, err := Just(1, 2, 3, 4, 5).
		DoFinally(func(e error) { calls++; got = e }).
		Limit(2).
		Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{1, 2}, out)
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

func TestDoFinally_BuriedInConcatFiresLocally(t *testing.T) {
	var calls int
	var got error = errors.New("sentinel-not-called")

	s1 := Just(1, 2).DoFinally(func(e error) { calls++; got = e })
	s2 := Just(3, 4)

	out, err := ConcatStreams(s1, s2).Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3, 4}, out)
	// s1 completes locally (EOF) as concat advances to s2.
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

// The decisive per-node case: in a merge of s1,s2 (both open), when s1 errors midway s1's DoFinally
// must fire that error while s2's — healthy, merely torn down — fires nil.
func TestDoFinally_MergeErrorIsPerNode(t *testing.T) {
	boom := errors.New("boom")

	var s1Calls, s2Calls int
	var s1Err, s2Err error
	s2Err = errors.New("sentinel-not-called")

	s1 := errAt(Just(10, 20), 20, boom).
		DoFinally(func(e error) { s1Calls++; s1Err = e })
	s2 := Just(15, 25).
		DoFinally(func(e error) { s2Calls++; s2Err = e })

	_, err := MergeSortedStreams(func(a, b int) int { return a - b }, s1, s2).
		Collect(context.Background())

	require.Error(t, err)

	require.Equal(t, 1, s1Calls)
	require.ErrorIs(t, s1Err, boom)

	require.Equal(t, 1, s2Calls)
	require.NoError(t, s2Err) // s2 was cancelled, not errored
}

func TestDoFinally_SurvivesDownstreamOperators(t *testing.T) {
	var calls int
	var got error = errors.New("sentinel-not-called")

	s := Just(1, 2, 3, 4, 5, 6).DoFinally(func(e error) { calls++; got = e })
	out, err := Map(s, func(i int) int { return i * 10 }).
		Filter(func(i int) bool { return i > 20 }).
		Limit(2).
		Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{30, 40}, out)
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

func TestDoFinally_FiresOncePerConsumption(t *testing.T) {
	var calls int
	s := Just(1, 2, 3).DoFinally(func(e error) { calls++ })

	_, err := s.Collect(context.Background())
	require.NoError(t, err)
	_, err = s.Collect(context.Background())
	require.NoError(t, err)

	require.Equal(t, 2, calls)
}

func TestDoFinally_PanickingHookDoesNotCrashConsumer(t *testing.T) {
	out, err := Just(1, 2, 3).
		DoFinally(func(e error) { panic("hook boom") }).
		Collect(context.Background())

	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, out)
}

func TestDoFinally_ConsumerCallbackErrorFiresNil(t *testing.T) {
	callbackErr := errors.New("callback failed")
	var calls int
	var got error = errors.New("sentinel-not-called")

	err := Just(1, 2, 3).
		DoFinally(func(e error) { calls++; got = e }).
		ConsumeWithErr(context.Background(), func(v int) error {
			if v == 2 {
				return callbackErr
			}
			return nil
		})

	require.ErrorIs(t, err, callbackErr)
	require.Equal(t, 1, calls)
	// Consumer-side failure is not the stream's terminal outcome (documented limitation).
	require.NoError(t, got)
}

func TestDoFinally_ConcurrentConsumeFiresOnce(t *testing.T) {
	var calls int
	var got error = errors.New("sentinel-not-called")

	err := Just(1, 2, 3, 4, 5).
		DoFinally(func(e error) { calls++; got = e }).
		Consume(context.Background(), func(v int) {}, WithConcurrentConsumeOption(2))

	require.NoError(t, err)
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

func TestDoFinally_ConcurrentConsumeErrorFiresError(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	var got error

	err := errAt(Just(1, 2, 3, 4, 5, 6), 4, boom).
		DoFinally(func(e error) { calls++; got = e }).
		Consume(context.Background(), func(v int) {}, WithConcurrentConsumeOption(3))

	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.ErrorIs(t, got, boom)
}

func TestDoFinally_EmptyStreamFiresNil(t *testing.T) {
	var calls int
	var got error = errors.New("sentinel-not-called")

	out, err := Empty[int]().
		DoFinally(func(e error) { calls++; got = e }).
		Collect(context.Background())

	require.NoError(t, err)
	require.Empty(t, out)
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

func TestDoFinally_MultipleHooksAllFire(t *testing.T) {
	boom := errors.New("boom")
	var a, b error
	var aCalls, bCalls int

	_, err := errAt(Just(1, 2, 3), 2, boom).
		DoFinally(func(e error) { aCalls++; a = e }).
		DoFinally(func(e error) { bCalls++; b = e }).
		Collect(context.Background())

	require.Error(t, err)
	require.Equal(t, 1, aCalls)
	require.Equal(t, 1, bCalls)
	require.ErrorIs(t, a, boom)
	require.ErrorIs(t, b, boom)
}

// A panic inside the pipeline is recovered by the consumer and surfaces as its returned error;
// it does not travel back through the provider as a value, so DoFinally reports nil (documented
// limitation, same bucket as consumer-side failures). This test pins that behavior.
func TestDoFinally_PipelinePanicFiresNil(t *testing.T) {
	var calls int
	var got error = errors.New("sentinel-not-called")

	_, err := Map(Just(1, 2, 3), func(i int) int {
		if i == 2 {
			panic("pipeline boom")
		}
		return i
	}).
		DoFinally(func(e error) { calls++; got = e }).
		Collect(context.Background())

	require.Error(t, err) // consumer still surfaces the panic as an error
	require.Equal(t, 1, calls)
	require.NoError(t, got) // ... but DoFinally does not observe it
}
