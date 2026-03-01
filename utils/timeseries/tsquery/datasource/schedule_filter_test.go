package datasource

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
)

func TestScheduleFilter_MixedMatching(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	// Business hours: 09:00–17:00 on weekdays
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{{fromMinutes: 9 * 60, toMinutes: 17 * 60}},
		[]int{1, 2, 3, 4, 5},
		nil, nil,
	)
	require.NoError(t, err)
	schedule := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)
	filter := NewScheduleFilter(schedule)

	// Monday June 17, 2024
	records := []timeseries.TsRecord[any]{
		{Value: int64(1), Timestamp: time.Date(2024, 6, 17, 8, 0, 0, 0, time.UTC)},  // Mon 08:00 → no
		{Value: int64(2), Timestamp: time.Date(2024, 6, 17, 10, 0, 0, 0, time.UTC)}, // Mon 10:00 → yes
		{Value: int64(3), Timestamp: time.Date(2024, 6, 17, 18, 0, 0, 0, time.UTC)}, // Mon 18:00 → no
		{Value: int64(4), Timestamp: time.Date(2024, 6, 18, 12, 0, 0, 0, time.UTC)}, // Tue 12:00 → yes
		{Value: int64(5), Timestamp: time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)}, // Sat 10:00 → no
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, 6, 19, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	filteredResult, err := filter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 2)
	require.Equal(t, int64(2), resultData[0].Value)
	require.Equal(t, int64(4), resultData[1].Value)
}

func TestScheduleFilter_AllPass(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	// Empty condition matches everything
	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	schedule := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)
	filter := NewScheduleFilter(schedule)

	records := []timeseries.TsRecord[any]{
		{Value: int64(1), Timestamp: time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)},
		{Value: int64(2), Timestamp: time.Date(2024, 6, 16, 10, 0, 0, 0, time.UTC)},
		{Value: int64(3), Timestamp: time.Date(2024, 6, 17, 10, 0, 0, 0, time.UTC)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, 6, 18, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	filteredResult, err := filter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 3)
}

func TestScheduleFilter_AllFiltered(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	// Only Sundays
	cond, err := NewScheduleCondition(nil, []int{0}, nil, nil)
	require.NoError(t, err)
	schedule := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)
	filter := NewScheduleFilter(schedule)

	// All records on Monday
	records := []timeseries.TsRecord[any]{
		{Value: int64(1), Timestamp: time.Date(2024, 6, 17, 10, 0, 0, 0, time.UTC)}, // Monday
		{Value: int64(2), Timestamp: time.Date(2024, 6, 17, 12, 0, 0, 0, time.UTC)}, // Monday
		{Value: int64(3), Timestamp: time.Date(2024, 6, 17, 14, 0, 0, 0, time.UTC)}, // Monday
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC), time.Date(2024, 6, 18, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	filteredResult, err := filter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 0)
}

func TestScheduleFilter_EmptyStream(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	schedule := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)
	filter := NewScheduleFilter(schedule)

	records := []timeseries.TsRecord[any]{}
	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	filteredResult, err := filter.Filter(ctx, result)
	require.NoError(t, err)

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 0)
}

func TestScheduleFilter_MetadataPreserved(t *testing.T) {
	ctx := context.Background()

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		"test_field",
		tsquery.DataTypeDecimal,
		false,
		"meters",
		map[string]any{"custom": "value"},
	)
	require.NoError(t, err)

	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	schedule := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)
	filter := NewScheduleFilter(schedule)

	records := []timeseries.TsRecord[any]{
		{Value: 10.5, Timestamp: time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	filteredResult, err := filter.Filter(ctx, result)
	require.NoError(t, err)

	resultMeta := filteredResult.Meta()
	require.Equal(t, "test_field", resultMeta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, resultMeta.DataType())
	require.Equal(t, "meters", resultMeta.Unit())
	require.Equal(t, "value", resultMeta.CustomMeta()["custom"])

	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 1)
	require.Equal(t, 10.5, resultData[0].Value)
}
