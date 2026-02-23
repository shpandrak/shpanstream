package timeseries

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAlignedTimestampsStream_FixedPeriod(t *testing.T) {
	// Test with 1-minute alignment period from 00:00:00 to 00:05:00
	// Should generate 5 timestamps: 00:00, 00:01, 00:02, 00:03, 00:04
	ap := NewFixedAlignmentPeriod(time.Minute, time.UTC)
	from := time.Unix(0, 0).UTC()
	to := time.Unix(300, 0).UTC() // 5 minutes

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, timestamps, 5)

	expected := []time.Time{
		time.Unix(0, 0).UTC(),
		time.Unix(60, 0).UTC(),
		time.Unix(120, 0).UTC(),
		time.Unix(180, 0).UTC(),
		time.Unix(240, 0).UTC(),
	}
	require.Equal(t, expected, timestamps)
}

func TestAlignedTimestampsStream_NonAlignedFrom(t *testing.T) {
	// Test when 'from' is not aligned - should start from aligned boundary
	// from=00:00:30, to=00:03:00, period=1min
	// Should generate: 00:00, 00:01, 00:02
	ap := NewFixedAlignmentPeriod(time.Minute, time.UTC)
	from := time.Unix(30, 0).UTC()  // 30 seconds in
	to := time.Unix(180, 0).UTC()   // 3 minutes

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, timestamps, 3)

	expected := []time.Time{
		time.Unix(0, 0).UTC(),
		time.Unix(60, 0).UTC(),
		time.Unix(120, 0).UTC(),
	}
	require.Equal(t, expected, timestamps)
}

func TestAlignedTimestampsStream_ToExclusive(t *testing.T) {
	// Test that 'to' is exclusive
	// from=00:00:00, to=00:02:00 exactly, period=1min
	// Should generate: 00:00, 00:01 (not 00:02)
	ap := NewFixedAlignmentPeriod(time.Minute, time.UTC)
	from := time.Unix(0, 0).UTC()
	to := time.Unix(120, 0).UTC() // exactly 2 minutes

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, timestamps, 2)

	expected := []time.Time{
		time.Unix(0, 0).UTC(),
		time.Unix(60, 0).UTC(),
	}
	require.Equal(t, expected, timestamps)
}

func TestAlignedTimestampsStream_EmptyRange(t *testing.T) {
	// Test when from >= to - should return empty stream
	ap := NewFixedAlignmentPeriod(time.Minute, time.UTC)
	from := time.Unix(120, 0).UTC()
	to := time.Unix(60, 0).UTC() // to < from

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, timestamps)
}

func TestAlignedTimestampsStream_SameFromTo(t *testing.T) {
	// Test when from == to - should return empty stream
	ap := NewFixedAlignmentPeriod(time.Minute, time.UTC)
	from := time.Unix(60, 0).UTC()
	to := time.Unix(60, 0).UTC()

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Empty(t, timestamps)
}

func TestAlignedTimestampsStream_DayPeriod(t *testing.T) {
	// Test with day alignment period
	// 3 days from Jan 1 to Jan 4
	ap := NewDayAlignmentPeriod(time.UTC)
	from := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC) // noon on Jan 1
	to := time.Date(2024, 1, 4, 0, 0, 0, 0, time.UTC)    // midnight Jan 4

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, timestamps, 3)

	expected := []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC),
	}
	require.Equal(t, expected, timestamps)
}

func TestAlignedTimestampsStream_MonthPeriod(t *testing.T) {
	// Test with month alignment period (variable length periods)
	ap := NewMonthAlignmentPeriod(time.UTC)
	from := time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC) // mid-January
	to := time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)    // April 1

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, timestamps, 3)

	expected := []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
	}
	require.Equal(t, expected, timestamps)
}

func TestFixedAlignmentPeriod_OffsetShiftsBuckets(t *testing.T) {
	// 1h duration + 15min offset → buckets at :15, 1:15, 2:15, ...
	ap := NewFixedAlignmentPeriodWithOffset(time.Hour, 15*time.Minute, time.UTC)

	// A time at 00:30 should fall in the bucket starting at 00:15
	got := ap.GetStartTime(time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC))
	require.Equal(t, time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC), got)

	// A time at 01:00 should still be in the 00:15 bucket (ends at 01:15)
	got = ap.GetStartTime(time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC))
	require.Equal(t, time.Date(2024, 1, 1, 0, 15, 0, 0, time.UTC), got)

	// A time at 01:15 exactly should start a new bucket
	got = ap.GetStartTime(time.Date(2024, 1, 1, 1, 15, 0, 0, time.UTC))
	require.Equal(t, time.Date(2024, 1, 1, 1, 15, 0, 0, time.UTC), got)

	// End time for bucket starting at 00:15 should be 01:15
	got = ap.GetEndTime(time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC))
	require.Equal(t, time.Date(2024, 1, 1, 1, 15, 0, 0, time.UTC), got)
}

func TestFixedAlignmentPeriod_ZeroOffsetMatchesNoOffset(t *testing.T) {
	apNoOffset := NewFixedAlignmentPeriod(time.Hour, time.UTC)
	apZeroOffset := NewFixedAlignmentPeriodWithOffset(time.Hour, 0, time.UTC)

	testTimes := []time.Time{
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 12, 45, 0, 0, time.UTC),
		time.Date(2024, 6, 15, 23, 59, 59, 0, time.UTC),
	}
	for _, tt := range testTimes {
		require.Equal(t, apNoOffset.GetStartTime(tt), apZeroOffset.GetStartTime(tt), "mismatch for %v", tt)
		require.Equal(t, apNoOffset.GetEndTime(tt), apZeroOffset.GetEndTime(tt), "mismatch for %v", tt)
	}
}

func TestFixedAlignmentPeriod_OffsetNormalization(t *testing.T) {
	// offset >= duration should wrap: 75min offset with 1h duration → 15min offset
	ap := NewFixedAlignmentPeriodWithOffset(time.Hour, 75*time.Minute, time.UTC)
	apExpected := NewFixedAlignmentPeriodWithOffset(time.Hour, 15*time.Minute, time.UTC)

	tt := time.Date(2024, 1, 1, 0, 30, 0, 0, time.UTC)
	require.Equal(t, apExpected.GetStartTime(tt), ap.GetStartTime(tt))
	require.Equal(t, apExpected.GetEndTime(tt), ap.GetEndTime(tt))
}

func TestAlignedTimestampsStream_WithOffset(t *testing.T) {
	// 6h duration + 3h offset → buckets at 03:00, 09:00, 15:00, 21:00
	ap := NewFixedAlignmentPeriodWithOffset(6*time.Hour, 3*time.Hour, time.UTC)
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)

	timestamps, err := AlignedTimestampsStream(ap, from, to).Collect(context.Background())
	require.NoError(t, err)
	require.Len(t, timestamps, 5)

	expected := []time.Time{
		time.Date(2023, 12, 31, 21, 0, 0, 0, time.UTC), // previous day 21:00
		time.Date(2024, 1, 1, 3, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 9, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 15, 0, 0, 0, time.UTC),
		time.Date(2024, 1, 1, 21, 0, 0, 0, time.UTC),
	}
	require.Equal(t, expected, timestamps)
}

func TestAlignedTimestampsStream_ContextCancellation(t *testing.T) {
	// Test that stream respects context cancellation
	ap := NewFixedAlignmentPeriod(time.Minute, time.UTC)
	from := time.Unix(0, 0).UTC()
	to := time.Unix(600, 0).UTC() // 10 minutes

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := AlignedTimestampsStream(ap, from, to).Collect(ctx)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}
