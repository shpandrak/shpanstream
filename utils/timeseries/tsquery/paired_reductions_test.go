package tsquery_test

import (
	"math"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
)

var pairedTestTS = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// --- MAE ---

func TestPairedAccumulator_MAE(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAE.NewPairedAccumulator()
	require.NoError(t, err)

	// actual=[10, 20, 30], predicted=[12, 18, 25]
	// errors: |10-12|=2, |20-18|=2, |30-25|=5  → MAE = 9/3 = 3.0
	acc.AddPair(10.0, 12.0, pairedTestTS)
	acc.AddPair(20.0, 18.0, pairedTestTS)
	acc.AddPair(30.0, 25.0, pairedTestTS)

	require.InDelta(t, 3.0, acc.Result().(float64), 0.0001)
	require.Nil(t, acc.ResultTimestamp())
}

func TestPairedAccumulator_MAE_Integer(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAE.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(int64(10), int64(12), pairedTestTS)
	acc.AddPair(int64(20), int64(18), pairedTestTS)

	require.InDelta(t, 2.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_MAE_PerfectPrediction(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAE.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(5.0, 5.0, pairedTestTS)
	acc.AddPair(10.0, 10.0, pairedTestTS)

	require.InDelta(t, 0.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_MAE_Empty(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAE.NewPairedAccumulator()
	require.NoError(t, err)
	require.Nil(t, acc.Result())
}

func TestPairedAccumulator_MAE_NilSkipped(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAE.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(10.0, nil, pairedTestTS)  // skipped
	acc.AddPair(nil, 10.0, pairedTestTS)  // skipped
	acc.AddPair(10.0, 12.0, pairedTestTS) // counted

	require.InDelta(t, 2.0, acc.Result().(float64), 0.0001)
}

// --- RMSE ---

func TestPairedAccumulator_RMSE(t *testing.T) {
	acc, err := tsquery.ReductionTypeRMSE.NewPairedAccumulator()
	require.NoError(t, err)

	// actual=[10, 20, 30], predicted=[12, 18, 25]
	// squared errors: 4, 4, 25 → mean=33/3=11 → RMSE=√11 ≈ 3.3166
	acc.AddPair(10.0, 12.0, pairedTestTS)
	acc.AddPair(20.0, 18.0, pairedTestTS)
	acc.AddPair(30.0, 25.0, pairedTestTS)

	require.InDelta(t, math.Sqrt(11.0), acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_RMSE_PerfectPrediction(t *testing.T) {
	acc, err := tsquery.ReductionTypeRMSE.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(5.0, 5.0, pairedTestTS)
	acc.AddPair(10.0, 10.0, pairedTestTS)

	require.InDelta(t, 0.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_RMSE_Empty(t *testing.T) {
	acc, err := tsquery.ReductionTypeRMSE.NewPairedAccumulator()
	require.NoError(t, err)
	require.Nil(t, acc.Result())
}

// --- MBE ---

func TestPairedAccumulator_MBE(t *testing.T) {
	acc, err := tsquery.ReductionTypeMBE.NewPairedAccumulator()
	require.NoError(t, err)

	// actual=[1, 2, 3], predicted=[2, 3, 4]
	// errors: -1, -1, -1 → MBE = -1.0 (model over-predicts)
	acc.AddPair(1.0, 2.0, pairedTestTS)
	acc.AddPair(2.0, 3.0, pairedTestTS)
	acc.AddPair(3.0, 4.0, pairedTestTS)

	require.InDelta(t, -1.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_MBE_PositiveBias(t *testing.T) {
	acc, err := tsquery.ReductionTypeMBE.NewPairedAccumulator()
	require.NoError(t, err)

	// actual=[10, 20], predicted=[8, 15] → errors: +2, +5 → MBE = 3.5 (model under-predicts)
	acc.AddPair(10.0, 8.0, pairedTestTS)
	acc.AddPair(20.0, 15.0, pairedTestTS)

	require.InDelta(t, 3.5, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_MBE_Empty(t *testing.T) {
	acc, err := tsquery.ReductionTypeMBE.NewPairedAccumulator()
	require.NoError(t, err)
	require.Nil(t, acc.Result())
}

// --- MAPE ---

func TestPairedAccumulator_MAPE(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAPE.NewPairedAccumulator()
	require.NoError(t, err)

	// actual=[100, 200], predicted=[90, 220]
	// pct errors: |10/100|=0.1, |20/200|=0.1 → mean=0.1 → MAPE = 10%
	acc.AddPair(100.0, 90.0, pairedTestTS)
	acc.AddPair(200.0, 220.0, pairedTestTS)

	require.InDelta(t, 10.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_MAPE_SkipsZeroActual(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAPE.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(0.0, 10.0, pairedTestTS)   // skipped (actual=0)
	acc.AddPair(100.0, 90.0, pairedTestTS)  // |10/100|=0.1

	require.InDelta(t, 10.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_MAPE_AllZeroActual(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAPE.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(0.0, 5.0, pairedTestTS)
	acc.AddPair(0.0, 10.0, pairedTestTS)

	require.Nil(t, acc.Result()) // all rows skipped → nil
}

func TestPairedAccumulator_MAPE_Empty(t *testing.T) {
	acc, err := tsquery.ReductionTypeMAPE.NewPairedAccumulator()
	require.NoError(t, err)
	require.Nil(t, acc.Result())
}

// --- Pearson ---

func TestPairedAccumulator_Pearson_PerfectPositive(t *testing.T) {
	acc, err := tsquery.ReductionTypePearson.NewPairedAccumulator()
	require.NoError(t, err)

	// Perfect linear: predicted = 2*actual + 1
	acc.AddPair(1.0, 3.0, pairedTestTS)
	acc.AddPair(2.0, 5.0, pairedTestTS)
	acc.AddPair(3.0, 7.0, pairedTestTS)
	acc.AddPair(4.0, 9.0, pairedTestTS)

	require.InDelta(t, 1.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_Pearson_PerfectNegative(t *testing.T) {
	acc, err := tsquery.ReductionTypePearson.NewPairedAccumulator()
	require.NoError(t, err)

	// Perfect negative: predicted = -actual + 10
	acc.AddPair(1.0, 9.0, pairedTestTS)
	acc.AddPair(2.0, 8.0, pairedTestTS)
	acc.AddPair(3.0, 7.0, pairedTestTS)
	acc.AddPair(4.0, 6.0, pairedTestTS)

	require.InDelta(t, -1.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_Pearson_NilForSinglePair(t *testing.T) {
	acc, err := tsquery.ReductionTypePearson.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(1.0, 2.0, pairedTestTS)

	require.Nil(t, acc.Result()) // n < 2
}

func TestPairedAccumulator_Pearson_NilForConstantActuals(t *testing.T) {
	acc, err := tsquery.ReductionTypePearson.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(5.0, 1.0, pairedTestTS)
	acc.AddPair(5.0, 2.0, pairedTestTS)
	acc.AddPair(5.0, 3.0, pairedTestTS)

	require.Nil(t, acc.Result()) // zero variance in actual
}

func TestPairedAccumulator_Pearson_Empty(t *testing.T) {
	acc, err := tsquery.ReductionTypePearson.NewPairedAccumulator()
	require.NoError(t, err)
	require.Nil(t, acc.Result())
}

// --- R² ---

func TestPairedAccumulator_R2_PerfectPrediction(t *testing.T) {
	acc, err := tsquery.ReductionTypeR2.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(1.0, 1.0, pairedTestTS)
	acc.AddPair(2.0, 2.0, pairedTestTS)
	acc.AddPair(3.0, 3.0, pairedTestTS)

	require.InDelta(t, 1.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_R2_KnownDataset(t *testing.T) {
	acc, err := tsquery.ReductionTypeR2.NewPairedAccumulator()
	require.NoError(t, err)

	// actual=[1, 2, 3, 4, 5], predicted=[1.1, 2.0, 2.9, 4.1, 4.9]
	// SS_res = 0.01 + 0 + 0.01 + 0.01 + 0.01 = 0.04
	// mean_actual = 3.0
	// SS_tot = 4 + 1 + 0 + 1 + 4 = 10
	// R² = 1 - 0.04/10 = 0.996
	acc.AddPair(1.0, 1.1, pairedTestTS)
	acc.AddPair(2.0, 2.0, pairedTestTS)
	acc.AddPair(3.0, 2.9, pairedTestTS)
	acc.AddPair(4.0, 4.1, pairedTestTS)
	acc.AddPair(5.0, 4.9, pairedTestTS)

	require.InDelta(t, 0.996, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_R2_NilForSinglePair(t *testing.T) {
	acc, err := tsquery.ReductionTypeR2.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(1.0, 2.0, pairedTestTS)

	require.Nil(t, acc.Result()) // n < 2
}

func TestPairedAccumulator_R2_NilForConstantActuals(t *testing.T) {
	acc, err := tsquery.ReductionTypeR2.NewPairedAccumulator()
	require.NoError(t, err)

	acc.AddPair(5.0, 4.0, pairedTestTS)
	acc.AddPair(5.0, 6.0, pairedTestTS)
	acc.AddPair(5.0, 5.0, pairedTestTS)

	require.Nil(t, acc.Result()) // SS_tot = 0
}

func TestPairedAccumulator_R2_Empty(t *testing.T) {
	acc, err := tsquery.ReductionTypeR2.NewPairedAccumulator()
	require.NoError(t, err)
	require.Nil(t, acc.Result())
}

func TestPairedAccumulator_R2_NegativeR2(t *testing.T) {
	acc, err := tsquery.ReductionTypeR2.NewPairedAccumulator()
	require.NoError(t, err)

	// Terrible predictions: worse than predicting the mean
	// actual=[1, 2, 3], predicted=[3, 1, 2] (shuffled)
	acc.AddPair(1.0, 3.0, pairedTestTS)
	acc.AddPair(2.0, 1.0, pairedTestTS)
	acc.AddPair(3.0, 2.0, pairedTestTS)

	// SS_res = 4 + 1 + 1 = 6, SS_tot = 1+0+1 = 2, R² = 1 - 6/2 = -2.0
	result := acc.Result().(float64)
	require.True(t, result < 0, "R² should be negative for terrible predictions")
	require.InDelta(t, -2.0, result, 0.0001)
}

// --- Numerical stability (large offset) ---

func TestPairedAccumulator_Pearson_LargeOffset(t *testing.T) {
	// Welford-like online algorithm should handle large offsets gracefully.
	// With the naive formula (n*sumXY - sumX*sumY), values around 1e9 would
	// cause catastrophic cancellation. Welford avoids this by tracking deviations from running means.
	acc, err := tsquery.ReductionTypePearson.NewPairedAccumulator()
	require.NoError(t, err)

	base := 1e9
	// actual = base+{1,2,3,4}, predicted = base+{2,4,6,8} → perfect positive linear (r=1)
	acc.AddPair(base+1.0, base+2.0, pairedTestTS)
	acc.AddPair(base+2.0, base+4.0, pairedTestTS)
	acc.AddPair(base+3.0, base+6.0, pairedTestTS)
	acc.AddPair(base+4.0, base+8.0, pairedTestTS)

	require.InDelta(t, 1.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_R2_LargeOffset(t *testing.T) {
	acc, err := tsquery.ReductionTypeR2.NewPairedAccumulator()
	require.NoError(t, err)

	base := 1e9
	// Perfect prediction with large offset
	acc.AddPair(base+1.0, base+1.0, pairedTestTS)
	acc.AddPair(base+2.0, base+2.0, pairedTestTS)
	acc.AddPair(base+3.0, base+3.0, pairedTestTS)

	require.InDelta(t, 1.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_RMSE_LargeOffset(t *testing.T) {
	acc, err := tsquery.ReductionTypeRMSE.NewPairedAccumulator()
	require.NoError(t, err)

	base := 1e9
	// Small errors relative to large values — verifies no precision loss from direct summation
	acc.AddPair(base+1.0, base+2.0, pairedTestTS)
	acc.AddPair(base+2.0, base+3.0, pairedTestTS)
	acc.AddPair(base+3.0, base+4.0, pairedTestTS)

	// All errors are 1.0, so RMSE = √(mean(1²)) = 1.0
	require.InDelta(t, 1.0, acc.Result().(float64), 0.0001)
}

func TestPairedAccumulator_Pearson_ConstantPredicted(t *testing.T) {
	acc, err := tsquery.ReductionTypePearson.NewPairedAccumulator()
	require.NoError(t, err)

	// Varying actuals, constant predicted → zero variance in Y → nil
	acc.AddPair(1.0, 5.0, pairedTestTS)
	acc.AddPair(2.0, 5.0, pairedTestTS)
	acc.AddPair(3.0, 5.0, pairedTestTS)

	require.Nil(t, acc.Result()) // zero variance in predicted
}

// --- IsPaired ---

func TestReductionType_IsPaired(t *testing.T) {
	pairedTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeMAE, tsquery.ReductionTypeRMSE, tsquery.ReductionTypeMBE,
		tsquery.ReductionTypeMAPE, tsquery.ReductionTypePearson, tsquery.ReductionTypeR2,
	}
	for _, rt := range pairedTypes {
		require.True(t, rt.IsPaired(), "expected %s to be paired", rt)
	}

	nonPairedTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeSum, tsquery.ReductionTypeAvg, tsquery.ReductionTypeMin,
		tsquery.ReductionTypeMax, tsquery.ReductionTypeCount, tsquery.ReductionTypeFirst,
		tsquery.ReductionTypeLast, tsquery.ReductionTypeStddev, tsquery.ReductionTypeVariance,
		tsquery.ReductionTypeSpread, tsquery.ReductionTypeP50,
	}
	for _, rt := range nonPairedTypes {
		require.False(t, rt.IsPaired(), "expected %s to NOT be paired", rt)
	}
}

// --- NewPairedAccumulator rejects non-paired types ---

func TestNewPairedAccumulator_RejectsNonPaired(t *testing.T) {
	_, err := tsquery.ReductionTypeSum.NewPairedAccumulator()
	require.Error(t, err)
	require.Contains(t, err.Error(), "not a paired reduction")
}

// --- ResultUnit for paired types ---

func TestReductionType_ResultUnit(t *testing.T) {
	// Error metrics preserve source unit
	require.Equal(t, "kWh", tsquery.ReductionTypeMAE.ResultUnit("kWh"))
	require.Equal(t, "kWh", tsquery.ReductionTypeRMSE.ResultUnit("kWh"))
	require.Equal(t, "kWh", tsquery.ReductionTypeMBE.ResultUnit("kWh"))

	// MAPE always returns "percent" (haystack style)
	require.Equal(t, "percent", tsquery.ReductionTypeMAPE.ResultUnit("kWh"))
	require.Equal(t, "percent", tsquery.ReductionTypeMAPE.ResultUnit(""))

	// Pearson and R² are dimensionless
	require.Equal(t, "", tsquery.ReductionTypePearson.ResultUnit("kWh"))
	require.Equal(t, "", tsquery.ReductionTypeR2.ResultUnit("kWh"))
}

// --- GetResultDataType for paired types ---

func TestReductionType_PairedResultDataType(t *testing.T) {
	pairedTypes := []tsquery.ReductionType{
		tsquery.ReductionTypeMAE, tsquery.ReductionTypeRMSE, tsquery.ReductionTypeMBE,
		tsquery.ReductionTypeMAPE, tsquery.ReductionTypePearson, tsquery.ReductionTypeR2,
	}
	for _, rt := range pairedTypes {
		require.Equal(t, tsquery.DataTypeDecimal, rt.GetResultDataType(tsquery.DataTypeInteger),
			"paired reduction %s should always return decimal", rt)
		require.Equal(t, tsquery.DataTypeDecimal, rt.GetResultDataType(tsquery.DataTypeDecimal),
			"paired reduction %s should always return decimal", rt)
	}
}
