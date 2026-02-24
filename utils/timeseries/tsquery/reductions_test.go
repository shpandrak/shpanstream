package tsquery_test

import (
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// --- Accumulator Unit Tests ---

func TestAccumulator_SumInteger(t *testing.T) {
	acc, err := tsquery.ReductionTypeSum.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)

	acc.Add(int64(10), ts1)
	acc.Add(int64(20), ts2)
	acc.Add(int64(30), ts3)

	require.Equal(t, int64(60), acc.Result())
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_SumDecimal(t *testing.T) {
	acc, err := tsquery.ReductionTypeSum.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	acc.Add(1.5, ts)
	acc.Add(2.5, ts)
	acc.Add(3.0, ts)

	require.Equal(t, 7.0, acc.Result())
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_AvgInteger(t *testing.T) {
	acc, err := tsquery.ReductionTypeAvg.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	acc.Add(int64(10), ts)
	acc.Add(int64(20), ts)
	acc.Add(int64(30), ts)

	require.InDelta(t, 20.0, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_AvgDecimal(t *testing.T) {
	acc, err := tsquery.ReductionTypeAvg.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	acc.Add(10.0, ts)
	acc.Add(20.0, ts)
	acc.Add(30.0, ts)

	require.InDelta(t, 20.0, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_Count(t *testing.T) {
	acc, err := tsquery.ReductionTypeCount.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	acc.Add(int64(100), ts)
	acc.Add(int64(200), ts)
	acc.Add(int64(300), ts)

	require.Equal(t, int64(3), acc.Result())
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_MinInteger_TracksTimestamp(t *testing.T) {
	acc, err := tsquery.ReductionTypeMin.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)

	acc.Add(int64(30), ts1)
	acc.Add(int64(10), ts2) // min occurs here
	acc.Add(int64(20), ts3)

	require.Equal(t, int64(10), acc.Result())
	require.NotNil(t, acc.ResultTimestamp())
	require.Equal(t, ts2, *acc.ResultTimestamp())
}

func TestAccumulator_MinDecimal_TracksTimestamp(t *testing.T) {
	acc, err := tsquery.ReductionTypeMin.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)

	acc.Add(30.0, ts1)
	acc.Add(10.0, ts2) // min occurs here
	acc.Add(20.0, ts3)

	require.Equal(t, 10.0, acc.Result())
	require.NotNil(t, acc.ResultTimestamp())
	require.Equal(t, ts2, *acc.ResultTimestamp())
}

func TestAccumulator_MaxInteger_TracksTimestamp(t *testing.T) {
	acc, err := tsquery.ReductionTypeMax.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)

	acc.Add(int64(10), ts1)
	acc.Add(int64(30), ts2) // max occurs here
	acc.Add(int64(20), ts3)

	require.Equal(t, int64(30), acc.Result())
	require.NotNil(t, acc.ResultTimestamp())
	require.Equal(t, ts2, *acc.ResultTimestamp())
}

func TestAccumulator_MaxDecimal_TracksTimestamp(t *testing.T) {
	acc, err := tsquery.ReductionTypeMax.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)

	acc.Add(10.0, ts1)
	acc.Add(30.0, ts2) // max occurs here

	require.Equal(t, 30.0, acc.Result())
	require.NotNil(t, acc.ResultTimestamp())
	require.Equal(t, ts2, *acc.ResultTimestamp())
}

func TestAccumulator_First(t *testing.T) {
	acc, err := tsquery.ReductionTypeFirst.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)

	acc.Add(int64(100), ts1) // first
	acc.Add(int64(200), ts2)
	acc.Add(int64(300), ts3)

	require.Equal(t, int64(100), acc.Result())
	require.NotNil(t, acc.ResultTimestamp())
	require.Equal(t, ts1, *acc.ResultTimestamp())
}

func TestAccumulator_Last(t *testing.T) {
	acc, err := tsquery.ReductionTypeLast.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 1, 1, 1, 0, 0, 0, time.UTC)
	ts3 := time.Date(2025, 1, 1, 2, 0, 0, 0, time.UTC)

	acc.Add(100.0, ts1)
	acc.Add(200.0, ts2)
	acc.Add(300.0, ts3) // last

	require.Equal(t, 300.0, acc.Result())
	require.NotNil(t, acc.ResultTimestamp())
	require.Equal(t, ts3, *acc.ResultTimestamp())
}

func TestAccumulator_Empty(t *testing.T) {
	types := []tsquery.ReductionType{
		tsquery.ReductionTypeSum,
		tsquery.ReductionTypeAvg,
		tsquery.ReductionTypeMin,
		tsquery.ReductionTypeMax,
		tsquery.ReductionTypeFirst,
		tsquery.ReductionTypeLast,
	}

	for _, rt := range types {
		t.Run(string(rt), func(t *testing.T) {
			acc, err := rt.NewAccumulator(tsquery.DataTypeInteger)
			require.NoError(t, err)

			require.Nil(t, acc.Result(), "empty accumulator should return nil for %s", rt)
			require.Nil(t, acc.ResultTimestamp(), "empty accumulator should return nil timestamp for %s", rt)
		})
	}

	// count returns 0 (not nil) for empty stream
	t.Run("count", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeCount.NewAccumulator(tsquery.DataTypeInteger)
		require.NoError(t, err)

		require.Equal(t, int64(0), acc.Result(), "empty count accumulator should return 0")
		require.Nil(t, acc.ResultTimestamp(), "empty count accumulator should return nil timestamp")
	})
}

func TestAccumulator_SingleValue(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// For integer accumulators
	intTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeSum,
		tsquery.ReductionTypeMin,
		tsquery.ReductionTypeMax,
		tsquery.ReductionTypeFirst,
		tsquery.ReductionTypeLast,
	}
	for _, rt := range intTypes {
		t.Run(string(rt)+"_integer", func(t *testing.T) {
			acc, err := rt.NewAccumulator(tsquery.DataTypeInteger)
			require.NoError(t, err)
			acc.Add(int64(42), ts)
			require.Equal(t, int64(42), acc.Result())
		})
	}

	// Avg with single value
	t.Run("avg_single", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeAvg.NewAccumulator(tsquery.DataTypeInteger)
		require.NoError(t, err)
		acc.Add(int64(42), ts)
		require.InDelta(t, 42.0, acc.Result().(float64), 0.0001)
	})

	// Count with single value
	t.Run("count_single", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeCount.NewAccumulator(tsquery.DataTypeInteger)
		require.NoError(t, err)
		acc.Add(int64(42), ts)
		require.Equal(t, int64(1), acc.Result())
	})

	// first/last/min/max should have timestamp on single value
	tsTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeFirst,
		tsquery.ReductionTypeLast,
		tsquery.ReductionTypeMin,
		tsquery.ReductionTypeMax,
	}
	for _, rt := range tsTypes {
		t.Run(string(rt)+"_single_timestamp", func(t *testing.T) {
			acc, err := rt.NewAccumulator(tsquery.DataTypeInteger)
			require.NoError(t, err)
			acc.Add(int64(42), ts)
			require.NotNil(t, acc.ResultTimestamp())
			require.Equal(t, ts, *acc.ResultTimestamp())
		})
	}
}

// --- GetReducerFunc tests for first/last ---

func TestGetReducerFunc_First(t *testing.T) {
	reducer, err := tsquery.ReductionTypeFirst.GetReducerFunc(tsquery.DataTypeInteger)
	require.NoError(t, err)

	result := reducer([]any{int64(10), int64(20), int64(30)})
	require.Equal(t, int64(10), result)
}

func TestGetReducerFunc_Last(t *testing.T) {
	reducer, err := tsquery.ReductionTypeLast.GetReducerFunc(tsquery.DataTypeInteger)
	require.NoError(t, err)

	result := reducer([]any{int64(10), int64(20), int64(30)})
	require.Equal(t, int64(30), result)
}

// --- UseIdentityWhenSingleValue for first/last ---

func TestUseIdentityWhenSingleValue_FirstLast(t *testing.T) {
	require.True(t, tsquery.ReductionTypeFirst.UseIdentityWhenSingleValue())
	require.True(t, tsquery.ReductionTypeLast.UseIdentityWhenSingleValue())
}

// --- GetResultDataType for first/last ---

func TestGetResultDataType_FirstLast(t *testing.T) {
	require.Equal(t, tsquery.DataTypeInteger, tsquery.ReductionTypeFirst.GetResultDataType(tsquery.DataTypeInteger))
	require.Equal(t, tsquery.DataTypeDecimal, tsquery.ReductionTypeFirst.GetResultDataType(tsquery.DataTypeDecimal))
	require.Equal(t, tsquery.DataTypeInteger, tsquery.ReductionTypeLast.GetResultDataType(tsquery.DataTypeInteger))
	require.Equal(t, tsquery.DataTypeDecimal, tsquery.ReductionTypeLast.GetResultDataType(tsquery.DataTypeDecimal))
}
