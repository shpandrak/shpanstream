package tsquery_test

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// TestFilteredMultiDatasource_DeltaNonNegative_ThenReduce tests the core use case:
// two cumulative counters with resets, apply delta(nonNegative) via FilteredMultiDatasource,
// then reduce with sum. This ensures the aligner doesn't interpolate between pre-reset and
// post-reset values (which would produce garbage).
func TestFilteredMultiDatasource_DeltaNonNegative_ThenReduce(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Counter1: cumulative counter that resets at hour 3
	// 100 -> 200 -> 300 -> 0 (reset) -> 100
	counter1 := createDatasource(t, "Counter1", tsquery.DataTypeInteger, true, "bytes",
		[]time.Time{
			baseTime,
			baseTime.Add(1 * time.Hour),
			baseTime.Add(2 * time.Hour),
			baseTime.Add(3 * time.Hour),
			baseTime.Add(4 * time.Hour),
		},
		[]any{int64(100), int64(200), int64(300), int64(0), int64(100)},
	)

	// Counter2: cumulative counter, no resets
	// 10 -> 30 -> 60 -> 100 -> 150
	counter2 := createDatasource(t, "Counter2", tsquery.DataTypeInteger, true, "bytes",
		[]time.Time{
			baseTime,
			baseTime.Add(1 * time.Hour),
			baseTime.Add(2 * time.Hour),
			baseTime.Add(3 * time.Hour),
			baseTime.Add(4 * time.Hour),
		},
		[]any{int64(10), int64(30), int64(60), int64(100), int64(150)},
	)

	// Create inner multi datasource
	innerMultiDS := datasource.NewListMultiDatasource([]datasource.DataSource{counter1, counter2})

	// Wrap with FilteredMultiDatasource applying delta(nonNegative=true)
	filteredMultiDS := datasource.NewFilteredMultiDatasource(innerMultiDS, []datasource.Filter{
		datasource.NewDeltaFilter(true, 0),
	})

	// Now reduce with sum using an aligner
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		datasource.NewAlignerFilter(timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC)),
		filteredMultiDS,
		tsquery.AddFieldMeta{Urn: "total_delta"},
	)

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()

	// Delta of Counter1 (nonNeg): skip first, then: 100, 100, 0 (reset from 300→0, so delta=0), 100
	// Delta of Counter2: skip first, then: 20, 30, 40, 50
	// After alignment (1h, data already aligned), inner join:
	// Hour 1: 100 + 20 = 120
	// Hour 2: 100 + 30 = 130
	// Hour 3: 0 + 40 = 40  (counter reset handled gracefully)
	// Hour 4: 100 + 50 = 150
	require.Len(t, records, 4)
	require.Equal(t, int64(120), records[0].Value)
	require.Equal(t, int64(130), records[1].Value)
	require.Equal(t, int64(40), records[2].Value)
	require.Equal(t, int64(150), records[3].Value)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "total_delta", meta.Urn())
	require.Equal(t, tsquery.DataTypeInteger, meta.DataType())
	require.Equal(t, "bytes", meta.Unit())
}

// TestReductionDatasource_AlignerWithFillMode verifies that the aligner's fillMode
// is respected in the reduction datasource.
func TestReductionDatasource_AlignerWithFillMode(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a datasource with a gap: data at hours 0, 1, 3 (missing hour 2)
	ds := createDatasource(t, "Metric", tsquery.DataTypeDecimal, true, "units",
		[]time.Time{
			baseTime,
			baseTime.Add(1 * time.Hour),
			baseTime.Add(3 * time.Hour),
		},
		[]any{10.0, 20.0, 40.0},
	)

	multiDS := datasource.NewListMultiDatasource([]datasource.DataSource{ds})

	// Create reduction with linear fill aligner
	reductionDS := datasource.NewReductionDatasource(
		tsquery.ReductionTypeSum,
		datasource.NewInterpolatingAlignerFilter(
			timeseries.NewFixedAlignmentPeriod(1*time.Hour, time.UTC),
			timeseries.FillModeLinear,
		),
		multiDS,
		tsquery.AddFieldMeta{Urn: "filled"},
	)

	result, err := reductionDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	// With linear fill, the gap at hour 2 should be interpolated: (20 + 40) / 2 = 30
	require.Len(t, records, 4)
	require.Equal(t, 10.0, records[0].Value) // hour 0
	require.Equal(t, 20.0, records[1].Value) // hour 1
	require.Equal(t, 30.0, records[2].Value) // hour 2 (filled)
	require.Equal(t, 40.0, records[3].Value) // hour 3
}
