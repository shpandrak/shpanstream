package datasource

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// --- Time Slot Matching ---

func TestSchedule_TimeSlot_BasicMatch(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 10:00 UTC → matches 09:00–17:00
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_TimeSlot_OutsideSlot(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 08:00 UTC → outside 09:00–17:00
	ts := time.Date(2024, 6, 15, 8, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_TimeSlot_FromInclusive(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 09:00 exactly → matches (from is inclusive)
	ts := time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_TimeSlot_ToExclusive(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 17:00 exactly → no match (to is exclusive)
	ts := time.Date(2024, 6, 15, 17, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_TimeSlot_CrossMidnight_MatchesLateEvening(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 22, 0, 6, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 23:00 → matches 22:00–06:00
	ts := time.Date(2024, 6, 15, 23, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_TimeSlot_CrossMidnight_MatchesEarlyMorning(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 22, 0, 6, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 03:00 → matches 22:00–06:00
	ts := time.Date(2024, 6, 16, 3, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_TimeSlot_CrossMidnight_Gap(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 22, 0, 6, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 12:00 → does NOT match 22:00–06:00
	ts := time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_TimeSlot_MultipleSlots_OR(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{
			mustTimeSlot(t, 9, 0, 12, 0),  // 09:00–12:00
			mustTimeSlot(t, 13, 0, 17, 0), // 13:00–17:00
		},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// 14:00 → matches second slot
	ts := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))

	// 12:30 → matches neither slot
	ts2 := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	require.False(t, s.Matches(ts2))
}

// --- Day of Week ---

func TestSchedule_DayOfWeek_WeekdayMatch(t *testing.T) {
	// Tuesday = 2
	cond, err := NewScheduleCondition(nil, []int{1, 2, 3, 4, 5}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Tuesday June 18, 2024
	ts := time.Date(2024, 6, 18, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Tuesday, ts.Weekday())
	require.True(t, s.Matches(ts))
}

func TestSchedule_DayOfWeek_WeekendNoMatch(t *testing.T) {
	cond, err := NewScheduleCondition(nil, []int{1, 2, 3, 4, 5}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Saturday June 15, 2024
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Saturday, ts.Weekday())
	require.False(t, s.Matches(ts))
}

func TestSchedule_DayOfWeek_AllDays(t *testing.T) {
	cond, err := NewScheduleCondition(nil, []int{0, 1, 2, 3, 4, 5, 6}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Any day matches
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC) // Saturday
	require.True(t, s.Matches(ts))
}

func TestSchedule_DayOfWeek_NoConstraint(t *testing.T) {
	// Empty daysOfWeek → no constraint → matches any day
	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC) // Saturday
	require.True(t, s.Matches(ts))
}

// --- Period Matching ---

func TestSchedule_Period_WithinPeriod(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 1, 1, 6, 30)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// March 15 → within Jan 1 – Jun 30
	ts := time.Date(2024, 3, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Period_OutsidePeriod(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 1, 1, 6, 30)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// August 15 → outside Jan 1 – Jun 30
	ts := time.Date(2024, 8, 15, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_Period_StartInclusive(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 1, 1, 6, 30)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Jan 1 → matches (start inclusive)
	ts := time.Date(2024, 1, 1, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Period_EndInclusive(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 1, 1, 6, 30)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Jun 30 → matches (end inclusive)
	ts := time.Date(2024, 6, 30, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Period_CrossYear(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 11, 1, 2, 28)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// December 15 → within Nov 1 – Feb 28 (cross-year)
	ts := time.Date(2024, 12, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Period_CrossYear_OtherSide(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 11, 1, 2, 28)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// January 15 → within Nov 1 – Feb 28 (cross-year, other side)
	ts := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Period_CrossYear_Gap(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 11, 1, 2, 28)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// June 15 → outside Nov 1 – Feb 28
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

// --- Date Matching ---

func TestSchedule_Date_ExactMatch(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"})
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Date_NoMatch(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"})
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 12, 26, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_Date_MultipleDates(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil,
		[]string{"2024-12-25", "2024-12-31", "2025-01-01"},
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 12, 31, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))

	ts2 := time.Date(2024, 12, 26, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts2))
}

// --- AND Logic (within a condition) ---

func TestSchedule_AND_TimeSlotAndDayOfWeek_BothMatch(t *testing.T) {
	// Weekday + business hours
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5}, // Mon–Fri
		nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday June 17, 2024 at 10:00
	ts := time.Date(2024, 6, 17, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Monday, ts.Weekday())
	require.True(t, s.Matches(ts))
}

func TestSchedule_AND_TimeSlotAndDayOfWeek_DayFails(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Saturday June 15, 2024 at 10:00 → day fails
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Saturday, ts.Weekday())
	require.False(t, s.Matches(ts))
}

func TestSchedule_AND_TimeSlotAndDayOfWeek_TimeFails(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday June 17, 2024 at 20:00 → time fails
	ts := time.Date(2024, 6, 17, 20, 0, 0, 0, time.UTC)
	require.Equal(t, time.Monday, ts.Weekday())
	require.False(t, s.Matches(ts))
}

func TestSchedule_AND_AllFieldsMatch(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		[]SchedulePeriod{mustPeriod(t, 1, 1, 6, 30)},
		[]string{"2024-06-17"},
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday June 17, 2024 at 10:00 → all match
	ts := time.Date(2024, 6, 17, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Monday, ts.Weekday())
	require.True(t, s.Matches(ts))
}

func TestSchedule_AND_AllFieldsOneFails(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		[]SchedulePeriod{mustPeriod(t, 7, 1, 12, 31)}, // Jul–Dec
		[]string{"2024-06-17"},
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday June 17, 2024 at 10:00 → time+day+date match, but period (Jul–Dec) fails
	ts := time.Date(2024, 6, 17, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

// --- OR Logic (between conditions) ---

func TestSchedule_OR_FirstConditionMatches(t *testing.T) {
	cond1, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 12, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond1, cond2}, nil, nil, nil, time.UTC)

	// 10:00 → matches first condition
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_OR_SecondConditionMatches(t *testing.T) {
	cond1, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 12, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond1, cond2}, nil, nil, nil, time.UTC)

	// 15:00 → matches second condition
	ts := time.Date(2024, 6, 15, 15, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_OR_NeitherConditionMatches(t *testing.T) {
	cond1, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 12, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond1, cond2}, nil, nil, nil, time.UTC)

	// 13:00 → neither condition matches
	ts := time.Date(2024, 6, 15, 13, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

// --- Exclude Conditions ---

func TestSchedule_Exclude_IncludeMatchesNoExcludes(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Exclude_IncludeAndExcludeBothMatch(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	excludeCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 12, 0, 13, 0)}, // lunch hour excluded
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	// 12:30 → include matches (09–17), exclude also matches (12–13) → excluded
	ts := time.Date(2024, 6, 15, 12, 30, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_Exclude_IncludeMatchesExcludeDoesNot(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	excludeCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 12, 0, 13, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	// 10:00 → include matches, exclude doesn't → matches
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Exclude_WithDates_HolidayExcluded(t *testing.T) {
	// Business hours on weekdays
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil,
	)
	require.NoError(t, err)
	// Exclude specific holidays
	excludeCond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"})
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	// Wednesday Dec 25, 2024 at 10:00 → business hours + weekday, but excluded by date
	ts := time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Wednesday, ts.Weekday())
	require.False(t, s.Matches(ts))

	// Thursday Dec 26, 2024 at 10:00 → matches (not excluded)
	ts2 := time.Date(2024, 12, 26, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Thursday, ts2.Weekday())
	require.True(t, s.Matches(ts2))
}

// --- Schedule Bounds (startTime / endTime) ---

func TestSchedule_Bounds_BeforeStartTime(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, nil, time.UTC)

	ts := time.Date(2024, 5, 31, 23, 59, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_Bounds_AfterEndTime(t *testing.T) {
	end := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, &end, time.UTC)

	// Exactly at endTime → no match (endTime exclusive)
	ts := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))

	// After endTime → no match
	ts2 := time.Date(2024, 7, 1, 0, 0, 1, 0, time.UTC)
	require.False(t, s.Matches(ts2))
}

func TestSchedule_Bounds_WithinBounds(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, &end, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Bounds_NoBoundsSet(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Far in the past
	ts := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))

	// Far in the future
	ts2 := time.Date(2100, 12, 31, 23, 59, 0, 0, time.UTC)
	require.True(t, s.Matches(ts2))
}

// --- Timezone ---

func TestSchedule_Timezone_UTCDefault(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	// nil location defaults to UTC
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, nil)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Timezone_CustomTimezone_DayOfWeekChange(t *testing.T) {
	// 23:00 UTC on Saturday → 01:00 Sunday in UTC+2
	loc := time.FixedZone("UTC+2", 2*60*60)
	// Condition: only Sundays
	cond, err := NewScheduleCondition(nil, []int{0}, nil, nil) // Sunday=0
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, loc)

	// Saturday June 15, 2024 at 23:00 UTC → Sunday June 16 at 01:00 UTC+2
	ts := time.Date(2024, 6, 15, 23, 0, 0, 0, time.UTC)
	require.Equal(t, time.Saturday, ts.Weekday()) // UTC is Saturday
	require.True(t, s.Matches(ts))                // but in UTC+2 it's Sunday → matches
}

func TestSchedule_Timezone_AffectsDateMatching(t *testing.T) {
	// 23:30 UTC on Dec 24 = 00:30 Dec 25 in UTC+1
	loc := time.FixedZone("UTC+1", 1*60*60)
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"})
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, loc)

	ts := time.Date(2024, 12, 24, 23, 30, 0, 0, time.UTC)
	require.True(t, s.Matches(ts)) // In UTC+1 this is Dec 25

	// Same time in UTC → Dec 24 → no match with UTC timezone
	s2 := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)
	require.False(t, s2.Matches(ts))
}

// --- Edge Cases ---

func TestSchedule_EmptyCondition_MatchesEverything(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_NoConditions_NoMatch(t *testing.T) {
	// No include conditions at all → nothing matches
	s := NewSchedule(nil, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

// --- Constructor Validation ---

func TestSchedule_InvalidDateFormat(t *testing.T) {
	_, err := NewScheduleCondition(nil, nil, nil, []string{"25-12-2024"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid date format")
}

func TestSchedule_InvalidDayOfWeek(t *testing.T) {
	_, err := NewScheduleCondition(nil, []int{7}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid day of week")

	_, err = NewScheduleCondition(nil, []int{-1}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid day of week")
}

// --- Time Slot Constructor Validation ---

func TestSchedule_InvalidTimeSlot_HourOutOfRange(t *testing.T) {
	_, err := NewScheduleTimeSlot(25, 0, 17, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid fromHour")

	_, err = NewScheduleTimeSlot(9, 0, 24, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid toHour")

	_, err = NewScheduleTimeSlot(-1, 0, 17, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid fromHour")
}

func TestSchedule_InvalidTimeSlot_MinuteOutOfRange(t *testing.T) {
	_, err := NewScheduleTimeSlot(9, 60, 17, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid fromMinute")

	_, err = NewScheduleTimeSlot(9, 0, 17, 60)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid toMinute")

	_, err = NewScheduleTimeSlot(9, -1, 17, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid fromMinute")
}

func TestSchedule_InvalidTimeSlot_ZeroLength(t *testing.T) {
	_, err := NewScheduleTimeSlot(9, 0, 9, 0)
	require.Error(t, err)
	require.Contains(t, err.Error(), "zero-length time slot")
}

func TestSchedule_ValidTimeSlot(t *testing.T) {
	slot, err := NewScheduleTimeSlot(9, 30, 17, 45)
	require.NoError(t, err)
	require.Equal(t, 9*60+30, slot.fromMinutes)
	require.Equal(t, 17*60+45, slot.toMinutes)
}

// --- Period Constructor Validation ---

func TestSchedule_InvalidPeriod_MonthOutOfRange(t *testing.T) {
	_, err := NewSchedulePeriod(13, 1, 6, 30)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid startMonth")

	_, err = NewSchedulePeriod(0, 1, 6, 30)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid startMonth")

	_, err = NewSchedulePeriod(1, 1, 13, 30)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid endMonth")
}

func TestSchedule_InvalidPeriod_DayOutOfRange(t *testing.T) {
	// Feb has max 29
	_, err := NewSchedulePeriod(2, 30, 6, 30)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid startDay")

	// April has max 30
	_, err = NewSchedulePeriod(1, 1, 4, 31)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid endDay")

	// Day 0 is invalid
	_, err = NewSchedulePeriod(1, 0, 6, 30)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid startDay")
}

func TestSchedule_ValidPeriod(t *testing.T) {
	p, err := NewSchedulePeriod(1, 15, 6, 30)
	require.NoError(t, err)
	require.Equal(t, 1, p.startMonth)
	require.Equal(t, 15, p.startDay)
	require.Equal(t, 6, p.endMonth)
	require.Equal(t, 30, p.endDay)
}

// --- Test Helpers ---

func mustTimeSlot(t *testing.T, fromHour, fromMinute, toHour, toMinute int) ScheduleTimeSlot {
	t.Helper()
	slot, err := NewScheduleTimeSlot(fromHour, fromMinute, toHour, toMinute)
	require.NoError(t, err)
	return slot
}

func mustPeriod(t *testing.T, startMonth, startDay, endMonth, endDay int) SchedulePeriod {
	t.Helper()
	p, err := NewSchedulePeriod(startMonth, startDay, endMonth, endDay)
	require.NoError(t, err)
	return p
}
