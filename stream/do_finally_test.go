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

func TestDoFinally_OpenFailureFiresError(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	var got error

	_, err := Error[int](boom).
		DoFinally(func(e error) { calls++; got = e }).
		Collect(context.Background())

	require.Error(t, err)
	require.ErrorIs(t, err, boom)
	require.Equal(t, 1, calls)
	require.ErrorIs(t, got, boom)
}

func TestDoFinally_OpenFailureBuriedUnderOperatorFiresError(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	var got error

	s := Error[int](boom).DoFinally(func(e error) { calls++; got = e })
	_, err := Map(s, func(i int) int { return i }).
		Filter(func(i int) bool { return true }).
		Collect(context.Background())

	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.ErrorIs(t, got, boom)
}

func TestDoFinally_OpenFailureConcurrentConsumeFiresError(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	var got error

	err := Error[int](boom).
		DoFinally(func(e error) { calls++; got = e }).
		Consume(context.Background(), func(v int) {}, WithConcurrentConsumeOption(2))

	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.ErrorIs(t, got, boom)
}

func TestDoFinally_OpenFailureFiresOncePerConsumption(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	s := Error[int](boom).DoFinally(func(e error) { calls++ })

	_, err := s.Collect(context.Background())
	require.Error(t, err)
	_, err = s.Collect(context.Background())
	require.Error(t, err)

	require.Equal(t, 2, calls)
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

// A panic inside the pipeline (upstream of the hook) is a drain failure: the consumer recovers it
// and returns it as an error, and DoFinally observes an equivalent error (wrapping the same
// panicked value) — the hook and the caller must agree that the drain failed.
func TestDoFinally_PipelinePanicFiresRecoveredError(t *testing.T) {
	var calls int
	var got error

	_, err := Map(Just(1, 2, 3), func(i int) int {
		if i == 2 {
			panic("pipeline boom")
		}
		return i
	}).
		DoFinally(func(e error) { calls++; got = e }).
		Collect(context.Background())

	require.Error(t, err)
	require.ErrorContains(t, err, "pipeline boom")
	require.Equal(t, 1, calls)
	require.Error(t, got)
	require.ErrorContains(t, got, "pipeline boom")
}

// A pipeline panic that wraps an error value must surface to the hook with the error chain intact.
func TestDoFinally_PipelinePanicWithErrorValueFiresMatchingError(t *testing.T) {
	boom := errors.New("boom")
	var got error

	_, err := Map(Just(1, 2, 3), func(i int) int {
		if i == 2 {
			panic(boom)
		}
		return i
	}).
		DoFinally(func(e error) { got = e }).
		Collect(context.Background())

	require.ErrorIs(t, err, boom)
	require.ErrorIs(t, got, boom)
}

// DoFinally is local: a panic in an operator applied *downstream* of the hook tears the hook's
// subtree down cleanly, so the hook fires nil — same rule as downstream errors.
func TestDoFinally_DownstreamPanicFiresNil(t *testing.T) {
	var calls int
	var got error = errors.New("sentinel-not-called")

	_, err := Map(
		Just(1, 2, 3).DoFinally(func(e error) { calls++; got = e }),
		func(i int) int {
			if i == 2 {
				panic("downstream boom")
			}
			return i
		},
	).Collect(context.Background())

	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

// A consumer-callback panic is a delivery failure, not a pipeline failure: the caller gets the
// recovered error, the hook fires nil — same boundary as consumer-callback errors.
func TestDoFinally_ConsumerCallbackPanicFiresNil(t *testing.T) {
	var calls int
	var got error = errors.New("sentinel-not-called")

	err := Just(1, 2, 3).
		DoFinally(func(e error) { calls++; got = e }).
		Consume(context.Background(), func(i int) {
			if i == 2 {
				panic("consumer boom")
			}
		})

	require.Error(t, err)
	require.Equal(t, 1, calls)
	require.NoError(t, got)
}

// Under the legacy GODEBUG=panicnil=1 mode, recover() returns nil for panic(nil). The DoFinally
// provider guard must not turn such a panic into a spurious zero element and a clean completion;
// it surfaces as a pipeline error to both the caller and the hook.
func TestDoFinally_LegacyPanicNilDoesNotCorruptStream(t *testing.T) {
	t.Setenv("GODEBUG", "panicnil=1")

	var got error
	out, err := Map(Just(1, 2, 3), func(i int) int {
		if i == 2 {
			panic(nil)
		}
		return i
	}).
		DoFinally(func(e error) { got = e }).
		Collect(context.Background())

	require.Error(t, err)
	require.NotContains(t, out, 0) // no spurious zero element
	require.Error(t, got)
}

// Re-consuming after a panicking drain must reset the hook's state: the second drain reports its
// own outcome, not the first drain's panic.
func TestDoFinally_ReconsumeAfterPanicFiresOwnOutcome(t *testing.T) {
	var outcomes []error
	shouldPanic := true

	s := Map(Just(1, 2, 3), func(i int) int {
		if shouldPanic && i == 2 {
			panic("first drain boom")
		}
		return i
	}).DoFinally(func(e error) { outcomes = append(outcomes, e) })

	_, err := s.Collect(context.Background())
	require.Error(t, err)

	shouldPanic = false
	out, err := s.Collect(context.Background())
	require.NoError(t, err)
	require.Equal(t, []int{1, 2, 3}, out)

	require.Len(t, outcomes, 2)
	require.Error(t, outcomes[0])
	require.ErrorContains(t, outcomes[0], "first drain boom")
	require.NoError(t, outcomes[1])
}
