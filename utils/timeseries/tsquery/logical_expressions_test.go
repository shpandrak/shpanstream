package tsquery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTimestampComparison_Equals(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	fn, err := ConditionOperatorEquals.GetFuncImpl(DataTypeTimestamp)
	require.NoError(t, err)
	require.True(t, fn(ts, ts))
	require.False(t, fn(ts, ts.Add(time.Second)))
}

func TestTimestampComparison_NotEquals(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	fn, err := ConditionOperatorNotEquals.GetFuncImpl(DataTypeTimestamp)
	require.NoError(t, err)
	require.False(t, fn(ts, ts))
	require.True(t, fn(ts, ts.Add(time.Second)))
}

func TestTimestampComparison_GreaterThan(t *testing.T) {
	ts1 := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	ts2 := time.Date(2025, 6, 15, 10, 31, 0, 0, time.UTC)
	fn, err := ConditionOperatorGreaterThan.GetFuncImpl(DataTypeTimestamp)
	require.NoError(t, err)
	require.True(t, fn(ts2, ts1))
	require.False(t, fn(ts1, ts2))
	require.False(t, fn(ts1, ts1))
}

func TestTimestampComparison_LessThan(t *testing.T) {
	ts1 := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	ts2 := time.Date(2025, 6, 15, 10, 31, 0, 0, time.UTC)
	fn, err := ConditionOperatorLessThan.GetFuncImpl(DataTypeTimestamp)
	require.NoError(t, err)
	require.True(t, fn(ts1, ts2))
	require.False(t, fn(ts2, ts1))
	require.False(t, fn(ts1, ts1))
}

func TestTimestampComparison_GreaterEqual(t *testing.T) {
	ts1 := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	ts2 := time.Date(2025, 6, 15, 10, 31, 0, 0, time.UTC)
	fn, err := ConditionOperatorGreaterEqual.GetFuncImpl(DataTypeTimestamp)
	require.NoError(t, err)
	require.True(t, fn(ts2, ts1))
	require.True(t, fn(ts1, ts1))
	require.False(t, fn(ts1, ts2))
}

func TestTimestampComparison_LessEqual(t *testing.T) {
	ts1 := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	ts2 := time.Date(2025, 6, 15, 10, 31, 0, 0, time.UTC)
	fn, err := ConditionOperatorLessEqual.GetFuncImpl(DataTypeTimestamp)
	require.NoError(t, err)
	require.True(t, fn(ts1, ts2))
	require.True(t, fn(ts1, ts1))
	require.False(t, fn(ts2, ts1))
}

func TestTimestampComparison_NilHandling(t *testing.T) {
	ts := time.Date(2025, 6, 15, 10, 30, 0, 0, time.UTC)
	fn, err := ConditionOperatorEquals.GetFuncImpl(DataTypeTimestamp)
	require.NoError(t, err)
	wrapped := WrapComparisonWithNilChecks(fn)
	require.False(t, wrapped(nil, ts))
	require.False(t, wrapped(ts, nil))
	require.False(t, wrapped(nil, nil))
	require.True(t, wrapped(ts, ts))
}
