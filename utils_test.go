package shpanstream

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResultUnpack(t *testing.T) {
	v, err := Result[int]{Value: 42}.Unpack()
	require.NoError(t, err)
	require.Equal(t, 42, v)

	boom := errors.New("boom")
	_, err = Result[int]{Err: boom}.Unpack()
	require.ErrorIs(t, err, boom)
}

func TestUnpackResult(t *testing.T) {
	v, err := UnpackResult(Result[string]{Value: "hello"})
	require.NoError(t, err)
	require.Equal(t, "hello", v)

	boom := errors.New("boom")
	_, err = UnpackResult(Result[string]{Err: boom})
	require.ErrorIs(t, err, boom)
}
