package tsquery_test

import (
	"math"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
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
		tsquery.ReductionTypeStddev,
		tsquery.ReductionTypeVariance,
		tsquery.ReductionTypeSpread,
		tsquery.ReductionTypeP50,
		tsquery.ReductionTypeP75,
		tsquery.ReductionTypeP90,
		tsquery.ReductionTypeP95,
		tsquery.ReductionTypeP99,
		tsquery.ReductionTypeP999,
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

	// stddev/variance/spread of single value = 0.0 / 0 respectively
	t.Run("stddev_single_integer", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeStddev.NewAccumulator(tsquery.DataTypeInteger)
		require.NoError(t, err)
		acc.Add(int64(42), ts)
		require.InDelta(t, 0.0, acc.Result().(float64), 0.0001)
	})
	t.Run("variance_single_integer", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeVariance.NewAccumulator(tsquery.DataTypeInteger)
		require.NoError(t, err)
		acc.Add(int64(42), ts)
		require.InDelta(t, 0.0, acc.Result().(float64), 0.0001)
	})
	t.Run("spread_single_integer", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeSpread.NewAccumulator(tsquery.DataTypeInteger)
		require.NoError(t, err)
		acc.Add(int64(42), ts)
		require.Equal(t, int64(0), acc.Result())
	})
	t.Run("spread_single_decimal", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeSpread.NewAccumulator(tsquery.DataTypeDecimal)
		require.NoError(t, err)
		acc.Add(42.0, ts)
		require.Equal(t, 0.0, acc.Result())
	})

	// percentiles of single value = that value (as float64)
	pTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeP50,
		tsquery.ReductionTypeP75,
		tsquery.ReductionTypeP90,
		tsquery.ReductionTypeP95,
		tsquery.ReductionTypeP99,
		tsquery.ReductionTypeP999,
	}
	for _, rt := range pTypes {
		t.Run(string(rt)+"_single_integer", func(t *testing.T) {
			acc, err := rt.NewAccumulator(tsquery.DataTypeInteger)
			require.NoError(t, err)
			acc.Add(int64(42), ts)
			require.InDelta(t, 42.0, acc.Result().(float64), 0.0001)
		})
		t.Run(string(rt)+"_single_decimal", func(t *testing.T) {
			acc, err := rt.NewAccumulator(tsquery.DataTypeDecimal)
			require.NoError(t, err)
			acc.Add(42.5, ts)
			require.InDelta(t, 42.5, acc.Result().(float64), 0.0001)
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

// --- Statistical reduction type tests ---

func TestAccumulator_StddevInteger(t *testing.T) {
	acc, err := tsquery.ReductionTypeStddev.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// values: 2, 4, 4, 4, 5, 5, 7, 9  → population stddev = 2.0
	for _, v := range []int64{2, 4, 4, 4, 5, 5, 7, 9} {
		acc.Add(v, ts)
	}

	require.InDelta(t, 2.0, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_StddevDecimal(t *testing.T) {
	acc, err := tsquery.ReductionTypeStddev.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		acc.Add(v, ts)
	}

	require.InDelta(t, 2.0, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_VarianceInteger(t *testing.T) {
	acc, err := tsquery.ReductionTypeVariance.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// values: 2, 4, 4, 4, 5, 5, 7, 9  → population variance = 4.0
	for _, v := range []int64{2, 4, 4, 4, 5, 5, 7, 9} {
		acc.Add(v, ts)
	}

	require.InDelta(t, 4.0, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_VarianceDecimal(t *testing.T) {
	acc, err := tsquery.ReductionTypeVariance.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		acc.Add(v, ts)
	}

	require.InDelta(t, 4.0, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_SpreadInteger(t *testing.T) {
	acc, err := tsquery.ReductionTypeSpread.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	acc.Add(int64(10), ts)
	acc.Add(int64(50), ts)
	acc.Add(int64(30), ts)

	require.Equal(t, int64(40), acc.Result())
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_SpreadDecimal(t *testing.T) {
	acc, err := tsquery.ReductionTypeSpread.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	acc.Add(10.0, ts)
	acc.Add(50.5, ts)
	acc.Add(30.0, ts)

	require.InDelta(t, 40.5, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestAccumulator_Percentiles(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// 100 values: 1, 2, 3, ..., 100
	intVals := make([]int64, 100)
	for i := range intVals {
		intVals[i] = int64(i + 1)
	}

	tests := []struct {
		rt       tsquery.ReductionType
		expected float64
	}{
		{tsquery.ReductionTypeP50, 50.5},
		{tsquery.ReductionTypeP75, 75.25},
		{tsquery.ReductionTypeP90, 90.1},
		{tsquery.ReductionTypeP95, 95.05},
		{tsquery.ReductionTypeP99, 99.01},
		{tsquery.ReductionTypeP999, 99.901},
	}

	// Accumulator uses streaming t-digest (approximate), so tolerances are wider
	// than the exact batch reducer tests below.
	for _, tc := range tests {
		t.Run(string(tc.rt)+"_integer", func(t *testing.T) {
			acc, err := tc.rt.NewAccumulator(tsquery.DataTypeInteger)
			require.NoError(t, err)
			for _, v := range intVals {
				acc.Add(v, ts)
			}
			require.InDelta(t, tc.expected, acc.Result().(float64), 1.0)
			require.Nil(t, acc.ResultTimestamp())
		})
		t.Run(string(tc.rt)+"_decimal", func(t *testing.T) {
			acc, err := tc.rt.NewAccumulator(tsquery.DataTypeDecimal)
			require.NoError(t, err)
			for _, v := range intVals {
				acc.Add(float64(v), ts)
			}
			require.InDelta(t, tc.expected, acc.Result().(float64), 1.0)
			require.Nil(t, acc.ResultTimestamp())
		})
	}
}

func TestAccumulator_PercentileSmallDataset(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	// 3 values: 10, 20, 30
	// Exact p50: idx = 0.5 * 2 = 1.0 → sorted[1] = 20.0
	// t-digest approximation may differ slightly for small datasets.
	acc, err := tsquery.ReductionTypeP50.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)
	acc.Add(int64(30), ts)
	acc.Add(int64(10), ts)
	acc.Add(int64(20), ts)
	require.InDelta(t, 20.0, acc.Result().(float64), 2.0)
}

// --- GetReducerFunc tests for statistical types ---

func TestGetReducerFunc_Stddev(t *testing.T) {
	reducer, err := tsquery.ReductionTypeStddev.GetReducerFunc(tsquery.DataTypeInteger)
	require.NoError(t, err)
	// stddev of {2,4,4,4,5,5,7,9} = 2.0
	result := reducer([]any{int64(2), int64(4), int64(4), int64(4), int64(5), int64(5), int64(7), int64(9)})
	require.InDelta(t, 2.0, result.(float64), 0.0001)
}

func TestGetReducerFunc_Variance(t *testing.T) {
	reducer, err := tsquery.ReductionTypeVariance.GetReducerFunc(tsquery.DataTypeDecimal)
	require.NoError(t, err)
	result := reducer([]any{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0})
	require.InDelta(t, 4.0, result.(float64), 0.0001)
}

func TestGetReducerFunc_Spread(t *testing.T) {
	reducer, err := tsquery.ReductionTypeSpread.GetReducerFunc(tsquery.DataTypeInteger)
	require.NoError(t, err)
	result := reducer([]any{int64(10), int64(50), int64(30)})
	require.Equal(t, int64(40), result)

	reducerDec, err := tsquery.ReductionTypeSpread.GetReducerFunc(tsquery.DataTypeDecimal)
	require.NoError(t, err)
	resultDec := reducerDec([]any{10.0, 50.5, 30.0})
	require.InDelta(t, 40.5, resultDec.(float64), 0.0001)
}

func TestGetReducerFunc_Percentile(t *testing.T) {
	reducer, err := tsquery.ReductionTypeP95.GetReducerFunc(tsquery.DataTypeDecimal)
	require.NoError(t, err)
	// 20 values: 1.0 .. 20.0, p95 with linear interpolation: idx = 0.95*19 = 18.05 → 19 + 0.05*(20-19) = 19.05
	vals := make([]any, 20)
	for i := range vals {
		vals[i] = float64(i + 1)
	}
	result := reducer(vals)
	require.InDelta(t, 19.05, result.(float64), 0.001)
}

// --- UseIdentityWhenSingleValue for statistical types ---

func TestUseIdentityWhenSingleValue_Statistical(t *testing.T) {
	// stddev, variance, spread → false
	require.False(t, tsquery.ReductionTypeStddev.UseIdentityWhenSingleValue())
	require.False(t, tsquery.ReductionTypeVariance.UseIdentityWhenSingleValue())
	require.False(t, tsquery.ReductionTypeSpread.UseIdentityWhenSingleValue())

	// percentiles → true
	require.True(t, tsquery.ReductionTypeP50.UseIdentityWhenSingleValue())
	require.True(t, tsquery.ReductionTypeP75.UseIdentityWhenSingleValue())
	require.True(t, tsquery.ReductionTypeP90.UseIdentityWhenSingleValue())
	require.True(t, tsquery.ReductionTypeP95.UseIdentityWhenSingleValue())
	require.True(t, tsquery.ReductionTypeP99.UseIdentityWhenSingleValue())
	require.True(t, tsquery.ReductionTypeP999.UseIdentityWhenSingleValue())
}

// --- GetResultDataType for statistical types ---

func TestGetResultDataType_Statistical(t *testing.T) {
	// stddev, variance, percentiles → always Decimal
	alwaysDecimal := []tsquery.ReductionType{
		tsquery.ReductionTypeStddev,
		tsquery.ReductionTypeVariance,
		tsquery.ReductionTypeP50,
		tsquery.ReductionTypeP75,
		tsquery.ReductionTypeP90,
		tsquery.ReductionTypeP95,
		tsquery.ReductionTypeP99,
		tsquery.ReductionTypeP999,
	}
	for _, rt := range alwaysDecimal {
		require.Equal(t, tsquery.DataTypeDecimal, rt.GetResultDataType(tsquery.DataTypeInteger), "expected Decimal for %s with Integer input", rt)
		require.Equal(t, tsquery.DataTypeDecimal, rt.GetResultDataType(tsquery.DataTypeDecimal), "expected Decimal for %s with Decimal input", rt)
	}

	// spread → preserves input type
	require.Equal(t, tsquery.DataTypeInteger, tsquery.ReductionTypeSpread.GetResultDataType(tsquery.DataTypeInteger))
	require.Equal(t, tsquery.DataTypeDecimal, tsquery.ReductionTypeSpread.GetResultDataType(tsquery.DataTypeDecimal))
}

// --- RequiresNumeric for statistical types ---

func TestRequiresNumeric_Statistical(t *testing.T) {
	numericTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeStddev,
		tsquery.ReductionTypeVariance,
		tsquery.ReductionTypeSpread,
		tsquery.ReductionTypeP50,
		tsquery.ReductionTypeP75,
		tsquery.ReductionTypeP90,
		tsquery.ReductionTypeP95,
		tsquery.ReductionTypeP99,
		tsquery.ReductionTypeP999,
	}
	for _, rt := range numericTypes {
		require.True(t, rt.RequiresNumeric(), "expected RequiresNumeric=true for %s", rt)
	}
}

// --- Nil handling for accumulators ---

func TestAccumulator_NilSkipped(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("stddev", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeStddev.NewAccumulator(tsquery.DataTypeInteger)
		require.NoError(t, err)
		acc.Add(nil, ts)
		acc.Add(int64(10), ts)
		acc.Add(nil, ts)
		acc.Add(int64(20), ts)
		// stddev of {10, 20} = 5.0
		require.InDelta(t, 5.0, acc.Result().(float64), 0.0001)
	})

	t.Run("percentile", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeP50.NewAccumulator(tsquery.DataTypeDecimal)
		require.NoError(t, err)
		acc.Add(nil, ts)
		acc.Add(10.0, ts)
		acc.Add(nil, ts)
		acc.Add(20.0, ts)
		acc.Add(30.0, ts)
		// p50 of {10, 20, 30}: exact = 20.0; t-digest approximate
		require.InDelta(t, 20.0, acc.Result().(float64), 2.0)
	})

	t.Run("spread", func(t *testing.T) {
		acc, err := tsquery.ReductionTypeSpread.NewAccumulator(tsquery.DataTypeDecimal)
		require.NoError(t, err)
		acc.Add(nil, ts)
		acc.Add(10.0, ts)
		acc.Add(nil, ts)
		acc.Add(30.0, ts)
		require.InDelta(t, 20.0, acc.Result().(float64), 0.0001)
	})
}

// --- Variance / Stddev numerical stability ---

func TestAccumulator_VarianceLargeOffset(t *testing.T) {
	// Welford's should handle large offset gracefully
	acc, err := tsquery.ReductionTypeVariance.NewAccumulator(tsquery.DataTypeDecimal)
	require.NoError(t, err)

	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	base := 1e9
	acc.Add(base+1.0, ts)
	acc.Add(base+2.0, ts)
	acc.Add(base+3.0, ts)
	// variance of {1e9+1, 1e9+2, 1e9+3} = variance of {1, 2, 3} = 2/3
	require.InDelta(t, 2.0/3.0, acc.Result().(float64), 0.0001)
}

// --- Stddev is sqrt of variance ---

func TestAccumulator_StddevIsSqrtOfVariance(t *testing.T) {
	ts := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	vals := []int64{3, 7, 7, 19}

	varAcc, err := tsquery.ReductionTypeVariance.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)
	stdAcc, err := tsquery.ReductionTypeStddev.NewAccumulator(tsquery.DataTypeInteger)
	require.NoError(t, err)

	for _, v := range vals {
		varAcc.Add(v, ts)
		stdAcc.Add(v, ts)
	}

	variance := varAcc.Result().(float64)
	stddev := stdAcc.Result().(float64)
	require.InDelta(t, math.Sqrt(variance), stddev, 0.0001)
}
