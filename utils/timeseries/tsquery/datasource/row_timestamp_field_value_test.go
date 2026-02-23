package datasource

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
)

func TestRowTimestampFieldValue_ReturnsTimestamp(t *testing.T) {
	ctx := context.Background()
	ts := time.Date(2025, 6, 15, 14, 30, 0, 0, time.UTC)

	field := NewRowTimestampFieldValue()
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	meta, supplier, err := field.Execute(ctx, *fieldMeta)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeTimestamp, meta.DataType)
	require.True(t, meta.Required)

	row := timeseries.TsRecord[any]{Timestamp: ts, Value: int64(42)}
	val, err := supplier(ctx, row)
	require.NoError(t, err)
	require.Equal(t, ts, val)
}

func TestRowTimestampFieldValue_DifferentTimestamps(t *testing.T) {
	ctx := context.Background()
	field := NewRowTimestampFieldValue()
	fieldMeta, _ := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)

	_, supplier, err := field.Execute(ctx, *fieldMeta)
	require.NoError(t, err)

	ts1 := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC)

	val1, err := supplier(ctx, timeseries.TsRecord[any]{Timestamp: ts1, Value: nil})
	require.NoError(t, err)
	require.Equal(t, ts1, val1)

	val2, err := supplier(ctx, timeseries.TsRecord[any]{Timestamp: ts2, Value: nil})
	require.NoError(t, err)
	require.Equal(t, ts2, val2)
}
