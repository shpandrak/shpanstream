package stream

import (
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStream_ErrorMap(t *testing.T) {
	_, err := MapWithErr(Just(1, 2, 3, 4, 5), func(i int) (int, error) {
		if i == 3 {
			return 0, errors.New("test error")
		}
		return i, nil
	}).Collect(context.Background())
	require.Error(t, err)

}

func TestStream_PanicMap(t *testing.T) {
	_, err := Map(Just(1, 2, 3, 4, 5), func(i int) int {
		if i == 3 {
			panic(errors.New("test error"))
		}
		return i
	}).Collect(context.Background())
	require.Error(t, err)

}

// Under the legacy GODEBUG=panicnil=1 mode, recover() returns nil for panic(nil) — and calling
// recover() cancels the panic. The consumer-boundary guard must not let such a panic dissolve into
// a clean, silently-truncated drain: the caller must get a pipeline error even when no DoFinally
// node is present (the DoFinally provider guard covers this independently when one is).
func TestStream_LegacyPanicNilSurfacesErrorToConsumer(t *testing.T) {
	t.Setenv("GODEBUG", "panicnil=1")

	out, err := Map(Just(1, 2, 3), func(i int) int {
		if i == 2 {
			panic(nil)
		}
		return i
	}).Collect(context.Background())

	require.Error(t, err, "legacy panic(nil) must surface as an error, not truncated success (got %v)", out)
}

func TestStream_ErrorFilter(t *testing.T) {
	_, err := Just(1, 2, 3, 4, 5).
		FilterWithErr(func(i int) (bool, error) {

			if i == 3 {
				return false, errors.New("test error")
			}
			return i > 1, nil
		}).
		Collect(context.Background())

	require.Error(t, err)

}

func TestStream_PanicFilter(t *testing.T) {
	_, err := Just(1, 2, 3, 4, 5).
		Filter(func(i int) bool {
			if i == 3 {
				panic(errors.New("test error"))
			}
			return i > 1

		}).
		Collect(context.Background())
	require.Error(t, err)

}

func TestStream_ProviderWithError(t *testing.T) {
	provider := &testingStreamProvider{
		emitErrorIndex: 10,
		emitError:      errors.New("test error"),
	}
	_, err := NewStream(provider).Collect(context.Background())
	require.Error(t, err)
	require.True(t, provider.isCloseCalled)
	require.Equal(t, 11, provider.currEmitIndex)
}

func TestStream_ProviderWithPanic(t *testing.T) {
	provider := &testingStreamProvider{
		emitErrorIndex: 10,
		emitPanic:      errors.New("test error"),
	}
	_, err := NewStream(provider).Collect(context.Background())
	require.Error(t, err)
	require.True(t, provider.isCloseCalled)
	require.Equal(t, 11, provider.currEmitIndex)
}

func TestStream_ProviderWithOpenError(t *testing.T) {
	provider := &testingStreamProvider{
		openError: errors.New("open error"),
	}
	_, err := NewStream(provider).Collect(context.Background())
	require.Error(t, err)
	// Not closed, since was never opened
	require.False(t, provider.isCloseCalled)
	require.Equal(t, 0, provider.currEmitIndex)
}

func TestStream_ProviderWithOpenPanic(t *testing.T) {
	provider := &testingStreamProvider{
		openPanic: errors.New("open error"),
	}
	_, err := NewStream(provider).Collect(context.Background())
	require.Error(t, err)
	// Not closed, since was never opened
	require.False(t, provider.isCloseCalled)
	require.Equal(t, 0, provider.currEmitIndex)
}

type testingStreamProvider struct {
	openError      error
	openPanic      error
	emitError      error
	emitPanic      error
	emitErrorIndex int
	currEmitIndex  int
	isCloseCalled  bool
}

func (t *testingStreamProvider) Open(_ context.Context) error {
	if t.openError != nil {
		return t.openError
	}
	if t.openPanic != nil {
		panic(t.openPanic)
	}
	return nil
}

func (t *testingStreamProvider) Close() {
	t.isCloseCalled = true
}

func (t *testingStreamProvider) Emit(_ context.Context) (int, error) {
	curr := t.currEmitIndex
	t.currEmitIndex++
	if curr == t.emitErrorIndex {
		if t.emitError != nil {
			return 0, t.emitError
		}
		if t.emitPanic != nil {
			panic(t.emitPanic)
		}
	}
	return curr, nil
}
