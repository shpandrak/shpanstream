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
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
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
	cond, err := NewScheduleCondition(nil, []int{1, 2, 3, 4, 5}, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Tuesday June 18, 2024
	ts := time.Date(2024, 6, 18, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Tuesday, ts.Weekday())
	require.True(t, s.Matches(ts))
}

func TestSchedule_DayOfWeek_WeekendNoMatch(t *testing.T) {
	cond, err := NewScheduleCondition(nil, []int{1, 2, 3, 4, 5}, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Saturday June 15, 2024
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Saturday, ts.Weekday())
	require.False(t, s.Matches(ts))
}

func TestSchedule_DayOfWeek_AllDays(t *testing.T) {
	cond, err := NewScheduleCondition(nil, []int{0, 1, 2, 3, 4, 5, 6}, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Any day matches
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC) // Saturday
	require.True(t, s.Matches(ts))
}

func TestSchedule_DayOfWeek_NoConstraint(t *testing.T) {
	// Empty daysOfWeek → no constraint → matches any day
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC) // Saturday
	require.True(t, s.Matches(ts))
}

// --- Period Matching ---

func TestSchedule_Period_WithinPeriod(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 1, 1, 6, 30)},
		nil, nil, nil,
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
		nil, nil, nil,
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
		nil, nil, nil,
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
		nil, nil, nil,
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
		nil, nil, nil,
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
		nil, nil, nil,
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
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// June 15 → outside Nov 1 – Feb 28
	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

// --- Date Matching ---

func TestSchedule_Date_ExactMatch(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Date_NoMatch(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 12, 26, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_Date_MultipleDates(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil,
		[]string{"2024-12-25", "2024-12-31", "2025-01-01"},
		nil, nil,
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
		nil, nil, nil, nil,
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
		nil, nil, nil, nil,
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
		nil, nil, nil, nil,
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
		nil, nil,
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
		nil, nil,
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
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 17, 0)},
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 17, 0)},
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 17, 0)},
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Exclude_IncludeAndExcludeBothMatch(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	excludeCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 12, 0, 13, 0)}, // lunch hour excluded
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	excludeCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 12, 0, 13, 0)},
		nil, nil, nil, nil, nil,
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
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	// Exclude specific holidays
	excludeCond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
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
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, nil, time.UTC)

	ts := time.Date(2024, 5, 31, 23, 59, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))
}

func TestSchedule_Bounds_AfterEndTime(t *testing.T) {
	end := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
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
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, &end, time.UTC)

	ts := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))
}

func TestSchedule_Bounds_NoBoundsSet(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
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
		nil, nil, nil, nil, nil,
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
	cond, err := NewScheduleCondition(nil, []int{0}, nil, nil, nil, nil) // Sunday=0
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
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
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
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
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
	_, err := NewScheduleCondition(nil, nil, nil, []string{"25-12-2024"}, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid date format")
}

func TestSchedule_InvalidDayOfWeek(t *testing.T) {
	_, err := NewScheduleCondition(nil, []int{7}, nil, nil, nil, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid day of week")

	_, err = NewScheduleCondition(nil, []int{-1}, nil, nil, nil, nil)
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

// --- Per-Condition Exclude Periods ---

func TestSchedule_ExcludePeriods_BasicExclusion(t *testing.T) {
	// Match all year, but exclude Dec 24-26
	cond, err := NewScheduleCondition(nil, nil, nil, nil,
		[]SchedulePeriod{mustPeriod(t, 12, 24, 12, 26)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Dec 25 → excluded
	ts := time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts))

	// Dec 23 → not excluded → matches
	ts2 := time.Date(2024, 12, 23, 10, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts2))
}

func TestSchedule_ExcludePeriods_ScopedToCondition(t *testing.T) {
	// This is the KEY use case: per-condition exclusions don't affect other conditions.
	// Condition 1: Weekdays, exclude Dec 25
	weekdayCond, err := NewScheduleCondition(
		nil,
		[]int{1, 2, 3, 4, 5}, // Mon–Fri
		nil, nil,
		[]SchedulePeriod{mustPeriod(t, 12, 25, 12, 25)}, // exclude Christmas
		nil,
	)
	require.NoError(t, err)

	// Condition 2: Weekends (no exclusions)
	weekendCond, err := NewScheduleCondition(
		nil,
		[]int{0, 6}, // Sat–Sun
		nil, nil, nil, nil,
	)
	require.NoError(t, err)

	s := NewSchedule([]ScheduleCondition{weekdayCond, weekendCond}, nil, nil, nil, time.UTC)

	// Wednesday Dec 25, 2024 → weekday matches but excluded by excludePeriod.
	// Weekend condition doesn't match (it's Wednesday).
	// → no match
	ts := time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Wednesday, ts.Weekday())
	require.False(t, s.Matches(ts))

	// Thursday Dec 26, 2024 → weekday matches, not excluded → matches
	ts2 := time.Date(2024, 12, 26, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Thursday, ts2.Weekday())
	require.True(t, s.Matches(ts2))

	// Saturday Dec 28, 2024 → weekend condition matches → matches
	ts3 := time.Date(2024, 12, 28, 10, 0, 0, 0, time.UTC)
	require.Equal(t, time.Saturday, ts3.Weekday())
	require.True(t, s.Matches(ts3))
}

func TestSchedule_ExcludePeriods_MultipleExcludePeriods(t *testing.T) {
	// Exclude both Christmas week and New Year's
	cond, err := NewScheduleCondition(nil, nil, nil, nil,
		[]SchedulePeriod{
			mustPeriod(t, 12, 24, 12, 26),
			mustPeriod(t, 12, 31, 1, 1), // cross-year
		},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.False(t, s.Matches(time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC))) // Christmas
	require.False(t, s.Matches(time.Date(2024, 12, 31, 10, 0, 0, 0, time.UTC))) // NYE
	require.False(t, s.Matches(time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC)))   // New Year's
	require.True(t, s.Matches(time.Date(2024, 12, 27, 10, 0, 0, 0, time.UTC)))  // between
}

// --- Per-Condition Exclude Dates ---

func TestSchedule_ExcludeDates_BasicExclusion(t *testing.T) {
	// Match all, but exclude specific date
	cond, err := NewScheduleCondition(nil, nil, nil, nil,
		nil, []string{"2024-12-25"},
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.False(t, s.Matches(time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC)))
	require.True(t, s.Matches(time.Date(2024, 12, 26, 10, 0, 0, 0, time.UTC)))

	// Same month/day next year is not excluded (dates are specific)
	require.True(t, s.Matches(time.Date(2025, 12, 25, 10, 0, 0, 0, time.UTC)))
}

func TestSchedule_ExcludeDates_ScopedToCondition(t *testing.T) {
	// Condition 1: Weekdays, exclude specific Christmas 2024
	weekdayCond, err := NewScheduleCondition(
		nil,
		[]int{1, 2, 3, 4, 5},
		nil, nil,
		nil, []string{"2024-12-25"},
	)
	require.NoError(t, err)

	// Condition 2: A holiday condition that explicitly includes Dec 25 with reduced hours
	holidayCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 10, 0, 14, 0)},
		nil, nil,
		[]string{"2024-12-25"},
		nil, nil,
	)
	require.NoError(t, err)

	s := NewSchedule([]ScheduleCondition{weekdayCond, holidayCond}, nil, nil, nil, time.UTC)

	// Dec 25, 2024 at 11:00 → weekday excluded, but holiday condition matches → true
	ts := time.Date(2024, 12, 25, 11, 0, 0, 0, time.UTC)
	require.True(t, s.Matches(ts))

	// Dec 25, 2024 at 16:00 → weekday excluded, holiday condition time slot doesn't match → false
	ts2 := time.Date(2024, 12, 25, 16, 0, 0, 0, time.UTC)
	require.False(t, s.Matches(ts2))
}

func TestSchedule_ExcludeDates_CombinedWithExcludePeriods(t *testing.T) {
	// Exclude a period AND a specific date
	cond, err := NewScheduleCondition(nil, nil, nil, nil,
		[]SchedulePeriod{mustPeriod(t, 7, 1, 7, 31)}, // exclude all of July
		[]string{"2024-12-25"},                       // also exclude Christmas
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.False(t, s.Matches(time.Date(2024, 7, 15, 10, 0, 0, 0, time.UTC)))  // July → excluded
	require.False(t, s.Matches(time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC))) // Christmas → excluded
	require.True(t, s.Matches(time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC)))   // June → fine
}

func TestSchedule_ExcludeDates_InvalidFormat(t *testing.T) {
	_, err := NewScheduleCondition(nil, nil, nil, nil, nil, []string{"25-12-2024"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid excludeDate format")
}

// =============================================================================
// MatchesDay Tests
// =============================================================================

// --- MatchesDay: Basic ---

func TestMatchesDay_BasicMatch_IgnoresTimeSlots(t *testing.T) {
	// Schedule with time-slot 09:00-17:00 and weekday constraint
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5}, // Mon–Fri
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday June 17, 2024 — weekday matches. MatchesDay ignores time-slots.
	date := time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC)
	require.Equal(t, time.Monday, date.Weekday())
	require.True(t, s.MatchesDay(date))
}

func TestMatchesDay_WeekendNoMatch(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Saturday June 15, 2024 — weekday fails.
	date := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	require.Equal(t, time.Saturday, date.Weekday())
	require.False(t, s.MatchesDay(date))
}

func TestMatchesDay_TimeComponentIgnored(t *testing.T) {
	// Time-slot only: 09:00-17:00. MatchesDay should ignore it entirely.
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Pass 03:00 — outside the time-slot, but MatchesDay should still return true.
	date := time.Date(2024, 6, 15, 3, 0, 0, 0, time.UTC)
	require.True(t, s.MatchesDay(date))

	// Pass 23:59 — also outside. Still true.
	date2 := time.Date(2024, 6, 15, 23, 59, 0, 0, time.UTC)
	require.True(t, s.MatchesDay(date2))
}

func TestMatchesDay_NoConditions_NoMatch(t *testing.T) {
	s := NewSchedule(nil, nil, nil, nil, time.UTC)
	require.False(t, s.MatchesDay(time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_EmptyCondition_MatchesAnyDay(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.True(t, s.MatchesDay(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)))
	require.True(t, s.MatchesDay(time.Date(2024, 7, 4, 0, 0, 0, 0, time.UTC)))
	require.True(t, s.MatchesDay(time.Date(2024, 12, 31, 0, 0, 0, 0, time.UTC)))
}

// --- MatchesDay: Period ---

func TestMatchesDay_Period_WithinPeriod(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 6, 1, 8, 31)}, // Jun–Aug (summer)
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.True(t, s.MatchesDay(time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_Period_OutsidePeriod(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 6, 1, 8, 31)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.False(t, s.MatchesDay(time.Date(2024, 3, 15, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_Period_CrossYear(t *testing.T) {
	// Winter: Nov 1 – Feb 28
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 11, 1, 2, 28)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.True(t, s.MatchesDay(time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)))
	require.True(t, s.MatchesDay(time.Date(2025, 1, 15, 0, 0, 0, 0, time.UTC)))
	require.False(t, s.MatchesDay(time.Date(2024, 5, 15, 0, 0, 0, 0, time.UTC)))
}

// --- MatchesDay: Specific Dates ---

func TestMatchesDay_SpecificDate_Match(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.True(t, s.MatchesDay(time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)))
	require.False(t, s.MatchesDay(time.Date(2024, 12, 26, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_SpecificDate_WithTimeSlot_StillMatchesDay(t *testing.T) {
	// Has time-slot 09:00-12:00 AND specific date. MatchesDay ignores time-slot.
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 12, 0)},
		nil, nil, []string{"2024-12-25"}, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.True(t, s.MatchesDay(time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)))
}

// --- MatchesDay: Bounds (day-level) ---

func TestMatchesDay_Bounds_BeforeStartTime(t *testing.T) {
	start := time.Date(2024, 6, 15, 10, 0, 0, 0, time.UTC) // starts mid-day
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, nil, time.UTC)

	// June 14 — entirely before startTime → no match
	require.False(t, s.MatchesDay(time.Date(2024, 6, 14, 0, 0, 0, 0, time.UTC)))

	// June 15 — overlaps startTime (start is at 10:00, day goes to 24:00) → match
	require.True(t, s.MatchesDay(time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)))

	// June 16 — fully after startTime → match
	require.True(t, s.MatchesDay(time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_Bounds_AfterEndTime(t *testing.T) {
	end := time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC) // ends mid-day
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, &end, time.UTC)

	// June 30 — fully before endTime → match
	require.True(t, s.MatchesDay(time.Date(2024, 6, 30, 0, 0, 0, 0, time.UTC)))

	// July 1 — partially before endTime (00:00-12:00) → match
	require.True(t, s.MatchesDay(time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)))

	// July 2 — entirely after endTime → no match
	require.False(t, s.MatchesDay(time.Date(2024, 7, 2, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_Bounds_EndTimeAtMidnight(t *testing.T) {
	// End at midnight of July 1 means June 30 is the last full day.
	end := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, &end, time.UTC)

	require.True(t, s.MatchesDay(time.Date(2024, 6, 30, 0, 0, 0, 0, time.UTC)))
	// July 1 at midnight: dayStart=July 1 00:00, endTime=July 1 00:00. dayStart.Before(endTime) is false.
	require.False(t, s.MatchesDay(time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_Bounds_BothBounds(t *testing.T) {
	start := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, &end, time.UTC)

	require.False(t, s.MatchesDay(time.Date(2024, 5, 31, 0, 0, 0, 0, time.UTC)))
	require.True(t, s.MatchesDay(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)))
	require.True(t, s.MatchesDay(time.Date(2024, 6, 30, 0, 0, 0, 0, time.UTC)))
	require.False(t, s.MatchesDay(time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)))
}

// --- MatchesDay: Exclude Conditions ---

func TestMatchesDay_Exclude_HolidayExcluded(t *testing.T) {
	// Business: weekdays
	cond, err := NewScheduleCondition(nil, []int{1, 2, 3, 4, 5}, nil, nil, nil, nil)
	require.NoError(t, err)
	// Exclude Christmas
	excludeCond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	// Wednesday Dec 25 → weekday, but excluded
	require.False(t, s.MatchesDay(time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)))

	// Thursday Dec 26 → weekday, not excluded
	require.True(t, s.MatchesDay(time.Date(2024, 12, 26, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_Exclude_WeekendExcluded(t *testing.T) {
	// Include: all days
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	// Exclude: weekends
	excludeCond, err := NewScheduleCondition(nil, []int{0, 6}, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	require.True(t, s.MatchesDay(time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC)))  // Monday
	require.False(t, s.MatchesDay(time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC))) // Saturday
	require.False(t, s.MatchesDay(time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC))) // Sunday
}

func TestMatchesDay_PerConditionExcludePeriod(t *testing.T) {
	// Weekdays, but exclude all of December
	cond, err := NewScheduleCondition(
		nil,
		[]int{1, 2, 3, 4, 5},
		nil, nil,
		[]SchedulePeriod{mustPeriod(t, 12, 1, 12, 31)},
		nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday in November → match
	require.True(t, s.MatchesDay(time.Date(2024, 11, 18, 0, 0, 0, 0, time.UTC)))

	// Monday in December → excluded by per-condition period
	require.False(t, s.MatchesDay(time.Date(2024, 12, 16, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_PerConditionExcludeDate(t *testing.T) {
	cond, err := NewScheduleCondition(
		nil, nil, nil, nil,
		nil, []string{"2024-07-04"},
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	require.False(t, s.MatchesDay(time.Date(2024, 7, 4, 0, 0, 0, 0, time.UTC)))
	require.True(t, s.MatchesDay(time.Date(2024, 7, 3, 0, 0, 0, 0, time.UTC)))
	// Same date next year → not excluded (dates are specific)
	require.True(t, s.MatchesDay(time.Date(2025, 7, 4, 0, 0, 0, 0, time.UTC)))
}

// --- MatchesDay: OR between conditions ---

func TestMatchesDay_OR_MultipleConditions(t *testing.T) {
	// Condition 1: weekdays in summer
	cond1, err := NewScheduleCondition(nil, []int{1, 2, 3, 4, 5},
		[]SchedulePeriod{mustPeriod(t, 6, 1, 8, 31)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	// Condition 2: specific holidays (any day of week)
	cond2, err := NewScheduleCondition(nil, nil, nil,
		[]string{"2024-12-25", "2025-01-01"},
		nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond1, cond2}, nil, nil, nil, time.UTC)

	// Summer weekday → cond1 matches
	require.True(t, s.MatchesDay(time.Date(2024, 7, 15, 0, 0, 0, 0, time.UTC)))
	// Summer weekend → cond1 fails (weekday), cond2 fails (not a holiday)
	require.False(t, s.MatchesDay(time.Date(2024, 7, 13, 0, 0, 0, 0, time.UTC)))
	// Christmas → cond1 fails (not summer), cond2 matches
	require.True(t, s.MatchesDay(time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)))
	// Random winter weekday → neither
	require.False(t, s.MatchesDay(time.Date(2024, 11, 18, 0, 0, 0, 0, time.UTC)))
}

// --- MatchesDay: Timezone ---

func TestMatchesDay_Timezone_UTCPlus_DayShift(t *testing.T) {
	// Schedule in UTC+9 (Tokyo). Weekdays only.
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	cond, err := NewScheduleCondition(nil, []int{1, 2, 3, 4, 5}, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, tokyo)

	// Friday June 14 2024 at 20:00 UTC = Saturday June 15 05:00 Tokyo
	// → In Tokyo it's Saturday → weekday check fails
	date := time.Date(2024, 6, 14, 20, 0, 0, 0, time.UTC)
	require.Equal(t, time.Friday, date.Weekday()) // UTC is Friday
	require.False(t, s.MatchesDay(date))           // Tokyo is Saturday → false

	// Friday June 14 2024 at 14:00 UTC = Friday June 14 23:00 Tokyo
	// → In Tokyo it's still Friday → weekday check passes
	date2 := time.Date(2024, 6, 14, 14, 0, 0, 0, time.UTC)
	require.True(t, s.MatchesDay(date2))
}

func TestMatchesDay_Timezone_UTCMinus_DayShift(t *testing.T) {
	// Schedule in US Eastern (UTC-5 / UTC-4 DST).
	eastern, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	// Date: 2024-12-25 only
	cond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, eastern)

	// Dec 25 01:00 UTC = Dec 24 20:00 Eastern → in Eastern it's Dec 24 → no match
	require.False(t, s.MatchesDay(time.Date(2024, 12, 25, 1, 0, 0, 0, time.UTC)))

	// Dec 25 06:00 UTC = Dec 25 01:00 Eastern → in Eastern it's Dec 25 → match
	require.True(t, s.MatchesDay(time.Date(2024, 12, 25, 6, 0, 0, 0, time.UTC)))

	// Dec 26 04:00 UTC = Dec 25 23:00 Eastern → still Dec 25 → match
	require.True(t, s.MatchesDay(time.Date(2024, 12, 26, 4, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_Timezone_BoundsInDifferentTZ(t *testing.T) {
	// Schedule in UTC+3 (Israel). Start: July 1 00:00 UTC+3 = June 30 21:00 UTC
	israel := time.FixedZone("IST", 3*60*60)
	start := time.Date(2024, 7, 1, 0, 0, 0, 0, israel) // = June 30 21:00 UTC
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, nil, israel)

	// June 30 in Israel → entirely before start (July 1 00:00 Israel)
	require.False(t, s.MatchesDay(time.Date(2024, 6, 30, 12, 0, 0, 0, israel)))

	// July 1 in Israel → starts at the boundary → match
	require.True(t, s.MatchesDay(time.Date(2024, 7, 1, 0, 0, 0, 0, israel)))
}

// --- MatchesDay: Complex / realistic scenarios ---

func TestMatchesDay_EnergyPeakDay_Realistic(t *testing.T) {
	// Peak schedule: weekdays, summer period (Jun–Sep), exclude July 4
	peakCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 18, 0)}, // 2–6 PM peak hours
		[]int{1, 2, 3, 4, 5},                               // weekdays
		[]SchedulePeriod{mustPeriod(t, 6, 1, 9, 30)},       // summer
		nil,
		nil, []string{"2024-07-04"}, // exclude Independence Day
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{peakCond}, nil, nil, nil, time.UTC)

	// Peak day: Wednesday July 10 → weekday + summer + not July 4
	require.True(t, s.MatchesDay(time.Date(2024, 7, 10, 0, 0, 0, 0, time.UTC)))

	// July 4 → excluded
	require.False(t, s.MatchesDay(time.Date(2024, 7, 4, 0, 0, 0, 0, time.UTC)))

	// Saturday July 13 → weekend → false
	require.False(t, s.MatchesDay(time.Date(2024, 7, 13, 0, 0, 0, 0, time.UTC)))

	// Monday Nov 4 → outside summer → false
	require.False(t, s.MatchesDay(time.Date(2024, 11, 4, 0, 0, 0, 0, time.UTC)))

	// The time-slot (14-18) is ignored by MatchesDay — the day is still a "peak day"
	// even though only 4 hours are actually peak
	require.True(t, s.MatchesDay(time.Date(2024, 8, 5, 0, 0, 0, 0, time.UTC)))
}

func TestMatchesDay_ConsistentWithMatches(t *testing.T) {
	// If MatchesDay is true, at least one minute on that day should Matches()==true.
	// (Because a matching day-level condition must have at least some active time-slot.)
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday June 17 — MatchesDay should be true
	date := time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC)
	require.True(t, s.MatchesDay(date))

	// Verify at least one minute matches
	found := false
	for h := 0; h < 24 && !found; h++ {
		for m := 0; m < 60 && !found; m++ {
			if s.Matches(time.Date(2024, 6, 17, h, m, 0, 0, time.UTC)) {
				found = true
			}
		}
	}
	require.True(t, found, "MatchesDay==true but no minute on that day Matches()==true")

	// Saturday June 15 — MatchesDay should be false
	date2 := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	require.False(t, s.MatchesDay(date2))
}

// =============================================================================
// ActiveWindows Tests
// =============================================================================

// --- ActiveWindows: Basic ---

func TestActiveWindows_SingleSlot_FullDay(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 1)
	require.Equal(t, time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 17, 0, 0, 0, time.UTC), windows[0].To)
}

func TestActiveWindows_NoConditions_Empty(t *testing.T) {
	s := NewSchedule(nil, nil, nil, nil, time.UTC)
	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	require.Empty(t, s.ActiveWindows(from, to).MustCollect())
}

func TestActiveWindows_EmptyCondition_AllDay(t *testing.T) {
	// No time-slot constraint → every minute matches
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 1)
	require.Equal(t, from, windows[0].From)
	require.Equal(t, to, windows[0].To)
}

func TestActiveWindows_InvalidRange_Empty(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// to before from
	to := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	from := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	require.Empty(t, s.ActiveWindows(from, to).MustCollect())

	// equal
	require.Empty(t, s.ActiveWindows(from, from).MustCollect())
}

// --- ActiveWindows: Multiple Slots ---

func TestActiveWindows_MultipleSlots_SameDay(t *testing.T) {
	// Morning + afternoon blocks
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{
			mustTimeSlot(t, 9, 0, 12, 0),
			mustTimeSlot(t, 13, 0, 17, 0),
		},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 2)
	require.Equal(t, time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC), windows[0].To)
	require.Equal(t, time.Date(2024, 6, 15, 13, 0, 0, 0, time.UTC), windows[1].From)
	require.Equal(t, time.Date(2024, 6, 15, 17, 0, 0, 0, time.UTC), windows[1].To)
}

// --- ActiveWindows: Cross-midnight ---

func TestActiveWindows_CrossMidnight_SingleDay(t *testing.T) {
	// 22:00–06:00 cross-midnight slot
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 22, 0, 6, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Query a full day — should get two pieces: 00:00–06:00 and 22:00–00:00
	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 2)
	require.Equal(t, time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 6, 0, 0, 0, time.UTC), windows[0].To)
	require.Equal(t, time.Date(2024, 6, 15, 22, 0, 0, 0, time.UTC), windows[1].From)
	require.Equal(t, time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC), windows[1].To)
}

func TestActiveWindows_CrossMidnight_TwoDays_Merges(t *testing.T) {
	// 22:00–06:00, query two days → should merge across midnight
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 22, 0, 6, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	// Day 15: 00:00–06:00, 22:00–00:00
	// Day 16: 00:00–06:00, 22:00–00:00
	// After merge: 00:00–06:00, 22:00–06:00 (merged across midnight), 22:00–00:00
	require.Len(t, windows, 3)
	require.Equal(t, time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 6, 0, 0, 0, time.UTC), windows[0].To)
	require.Equal(t, time.Date(2024, 6, 15, 22, 0, 0, 0, time.UTC), windows[1].From)
	require.Equal(t, time.Date(2024, 6, 16, 6, 0, 0, 0, time.UTC), windows[1].To)
	require.Equal(t, time.Date(2024, 6, 16, 22, 0, 0, 0, time.UTC), windows[2].From)
	require.Equal(t, time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC), windows[2].To)
}

// --- ActiveWindows: Multiple Days ---

func TestActiveWindows_MultipleDays_BusinessHours(t *testing.T) {
	// Weekdays 09:00-17:00
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday June 17 through Sunday June 23 (full week)
	from := time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC) // Monday
	to := time.Date(2024, 6, 24, 0, 0, 0, 0, time.UTC)   // Next Monday
	windows := s.ActiveWindows(from, to).MustCollect()

	// 5 weekdays × 1 window each
	require.Len(t, windows, 5)
	for i, w := range windows {
		expectedDay := 17 + i
		require.Equal(t, time.Date(2024, 6, expectedDay, 9, 0, 0, 0, time.UTC), w.From,
			"window %d From", i)
		require.Equal(t, time.Date(2024, 6, expectedDay, 17, 0, 0, 0, time.UTC), w.To,
			"window %d To", i)
	}
}

func TestActiveWindows_SkipsNonMatchingDays(t *testing.T) {
	// Only Mondays
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 10, 0, 11, 0)},
		[]int{1}, // Monday only
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Two weeks
	from := time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC) // Monday
	to := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)    // Monday
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 2) // June 17, June 24
	require.Equal(t, 17, windows[0].From.Day())
	require.Equal(t, 24, windows[1].From.Day())
}

// --- ActiveWindows: Exclude Conditions ---

func TestActiveWindows_Exclude_LunchHour(t *testing.T) {
	// Include: 09:00-17:00
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	// Exclude: 12:00-13:00 (lunch)
	excludeCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 12, 0, 13, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 2)
	require.Equal(t, time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 12, 0, 0, 0, time.UTC), windows[0].To)
	require.Equal(t, time.Date(2024, 6, 15, 13, 0, 0, 0, time.UTC), windows[1].From)
	require.Equal(t, time.Date(2024, 6, 15, 17, 0, 0, 0, time.UTC), windows[1].To)
}

func TestActiveWindows_Exclude_EntireDay(t *testing.T) {
	// Include: all days, 09:00-17:00
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	// Exclude: specific date
	excludeCond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-06-16"}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	// Three days: June 15-17
	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 18, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	// June 16 is fully excluded → only June 15 and 17
	require.Len(t, windows, 2)
	require.Equal(t, 15, windows[0].From.Day())
	require.Equal(t, 17, windows[1].From.Day())
}

// --- ActiveWindows: Bounds ---

func TestActiveWindows_Bounds_ClippedByStartTime(t *testing.T) {
	start := time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	// Should start at 10:30 (clipped by startTime), end at 17:00
	require.Len(t, windows, 1)
	require.Equal(t, time.Date(2024, 6, 15, 10, 30, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 17, 0, 0, 0, time.UTC), windows[0].To)
}

func TestActiveWindows_Bounds_ClippedByEndTime(t *testing.T) {
	end := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, &end, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	// Should start at 09:00, end at 14:00 (clipped by endTime)
	require.Len(t, windows, 1)
	require.Equal(t, time.Date(2024, 6, 15, 9, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC), windows[0].To)
}

func TestActiveWindows_Bounds_EntirelyOutside(t *testing.T) {
	start := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, &start, &end, time.UTC)

	// Query June — entirely before schedule bounds
	from := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	require.Empty(t, s.ActiveWindows(from, to).MustCollect())
}

// --- ActiveWindows: Partial day query ---

func TestActiveWindows_PartialDay_Afternoon(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Query 14:00-16:00 only
	from := time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 15, 16, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 1)
	require.Equal(t, from, windows[0].From)
	require.Equal(t, to, windows[0].To)
}

func TestActiveWindows_PartialDay_SpansBoundary(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Query 15:00-20:00 — partially inside slot
	from := time.Date(2024, 6, 15, 15, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 15, 20, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 1)
	require.Equal(t, time.Date(2024, 6, 15, 15, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 15, 17, 0, 0, 0, time.UTC), windows[0].To)
}

// --- ActiveWindows: Timezone ---

func TestActiveWindows_Timezone_NonUTC(t *testing.T) {
	// Schedule in UTC+3. Business hours 09:00-17:00 local.
	loc := time.FixedZone("UTC+3", 3*60*60)
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, loc)

	// Query in UTC: full day June 15
	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	// 09:00–17:00 UTC+3 = 06:00–14:00 UTC
	// But windows are returned in schedule tz (UTC+3).
	// The query range in UTC+3 is 03:00–03:00(+1).
	// Active: 09:00–17:00 in UTC+3, clipped to 03:00–03:00 → 09:00–17:00
	require.Len(t, windows, 1)
	require.Equal(t, 9, windows[0].From.Hour())
	require.Equal(t, 17, windows[0].To.Hour())
}

func TestActiveWindows_Timezone_Tokyo_Weekday(t *testing.T) {
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	// Weekdays 09:00-17:00 Tokyo time
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, tokyo)

	// Query: Friday June 14 2024 in Tokyo time
	from := time.Date(2024, 6, 14, 0, 0, 0, 0, tokyo)
	to := time.Date(2024, 6, 15, 0, 0, 0, 0, tokyo) // Saturday
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 1)
	require.Equal(t, time.Date(2024, 6, 14, 9, 0, 0, 0, tokyo), windows[0].From)
	require.Equal(t, time.Date(2024, 6, 14, 17, 0, 0, 0, tokyo), windows[0].To)

	// Query Saturday — no windows
	from2 := time.Date(2024, 6, 15, 0, 0, 0, 0, tokyo)
	to2 := time.Date(2024, 6, 16, 0, 0, 0, 0, tokyo)
	require.Empty(t, s.ActiveWindows(from2, to2).MustCollect())
}

func TestActiveWindows_Timezone_DST_SpringForward(t *testing.T) {
	// US Eastern: spring forward on March 10, 2024 at 02:00
	eastern, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 1, 0, 4, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, eastern)

	// Query the DST transition day
	from := time.Date(2024, 3, 10, 0, 0, 0, 0, eastern)
	to := time.Date(2024, 3, 11, 0, 0, 0, 0, eastern)
	windows := s.ActiveWindows(from, to).MustCollect()

	// 01:00-02:00 exists, 02:00-03:00 is skipped (spring forward), 03:00-04:00 exists
	// The schedule should still produce a window, though its duration may be shorter
	require.NotEmpty(t, windows)
	require.Equal(t, 1, windows[0].From.Hour())
}

// --- ActiveWindows: Per-condition excludes ---

func TestActiveWindows_PerConditionExclude_HolidayReducedHours(t *testing.T) {
	// Condition 1: Weekdays full hours, exclude Christmas
	weekdayCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil,
		nil, []string{"2024-12-25"},
	)
	require.NoError(t, err)
	// Condition 2: Christmas reduced hours
	holidayCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 10, 0, 14, 0)},
		nil, nil,
		[]string{"2024-12-25"},
		nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{weekdayCond, holidayCond}, nil, nil, nil, time.UTC)

	// Dec 25 (Wednesday) — weekday condition excluded, holiday condition applies
	from := time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 12, 26, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 1)
	require.Equal(t, time.Date(2024, 12, 25, 10, 0, 0, 0, time.UTC), windows[0].From)
	require.Equal(t, time.Date(2024, 12, 25, 14, 0, 0, 0, time.UTC), windows[0].To)

	// Dec 26 (Thursday) — normal weekday hours
	from2 := time.Date(2024, 12, 26, 0, 0, 0, 0, time.UTC)
	to2 := time.Date(2024, 12, 27, 0, 0, 0, 0, time.UTC)
	windows2 := s.ActiveWindows(from2, to2).MustCollect()

	require.Len(t, windows2, 1)
	require.Equal(t, time.Date(2024, 12, 26, 9, 0, 0, 0, time.UTC), windows2[0].From)
	require.Equal(t, time.Date(2024, 12, 26, 17, 0, 0, 0, time.UTC), windows2[0].To)
}

// --- ActiveWindows: Complex / realistic ---

func TestActiveWindows_EnergyPeakSchedule(t *testing.T) {
	// Peak: weekdays 14:00-18:00, summer only, exclude July 4
	peakCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 18, 0)},
		[]int{1, 2, 3, 4, 5},
		[]SchedulePeriod{mustPeriod(t, 6, 1, 9, 30)},
		nil,
		nil, []string{"2024-07-04"},
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{peakCond}, nil, nil, nil, time.UTC)

	// Week of July 1-7, 2024: Mon(1), Tue(2), Wed(3), Thu(4=excl), Fri(5), Sat(6), Sun(7)
	from := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 7, 8, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	// 4 weekdays (July 4 excluded, weekend excluded)
	require.Len(t, windows, 4)

	days := make([]int, len(windows))
	for i, w := range windows {
		days[i] = w.From.Day()
		require.Equal(t, 14, w.From.Hour())
		require.Equal(t, 18, w.To.Hour())
	}
	require.Equal(t, []int{1, 2, 3, 5}, days)
}

func TestActiveWindows_MultiWeek_OnlyMatchingDays(t *testing.T) {
	// Wednesday-only schedule, 08:00-12:00
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 8, 0, 12, 0)},
		[]int{3}, // Wednesday
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// June 2024: Wednesdays are 5, 12, 19, 26
	from := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 4)
	for i, w := range windows {
		require.Equal(t, time.Wednesday, w.From.Weekday(), "window %d", i)
		require.Equal(t, 8, w.From.Hour())
		require.Equal(t, 12, w.To.Hour())
	}
}

// --- ActiveWindows: len() == 0 as MatchesBetween ---

func TestActiveWindows_AsMatchesBetween(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Weekday range — has windows
	from := time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC) // Monday
	to := time.Date(2024, 6, 18, 0, 0, 0, 0, time.UTC)
	require.True(t, len(s.ActiveWindows(from, to).MustCollect()) > 0)

	// Weekend range — no windows
	from2 := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC) // Saturday
	to2 := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	require.True(t, len(s.ActiveWindows(from2, to2).MustCollect()) == 0)
}

// --- ActiveWindows: OR conditions union ---

func TestActiveWindows_ORConditions_Union(t *testing.T) {
	// Condition 1: morning 09:00-12:00
	cond1, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 12, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	// Condition 2: afternoon 14:00-17:00
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 14, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond1, cond2}, nil, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	require.Len(t, windows, 2)
	require.Equal(t, 9, windows[0].From.Hour())
	require.Equal(t, 12, windows[0].To.Hour())
	require.Equal(t, 14, windows[1].From.Hour())
	require.Equal(t, 17, windows[1].To.Hour())
}

func TestActiveWindows_ORConditions_Overlapping_Merged(t *testing.T) {
	// Condition 1: 09:00-14:00
	cond1, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 14, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	// Condition 2: 12:00-17:00 (overlaps)
	cond2, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 12, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond1, cond2}, nil, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	// Overlapping → merged into single 09:00-17:00
	require.Len(t, windows, 1)
	require.Equal(t, 9, windows[0].From.Hour())
	require.Equal(t, 17, windows[0].To.Hour())
}

// --- ActiveWindows: Duration calculation use-case ---

func TestActiveWindows_TotalDuration(t *testing.T) {
	// Business hours with lunch break excluded
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	excludeCond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 12, 0, 13, 0)},
		nil, nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	windows := s.ActiveWindows(from, to).MustCollect()

	var totalMinutes int
	for _, w := range windows {
		totalMinutes += int(w.To.Sub(w.From).Minutes())
	}
	// 3h morning (09-12) + 4h afternoon (13-17) = 7h = 420 min
	require.Equal(t, 420, totalMinutes)
}

// =============================================================================
// MatchesPeriod Tests
// =============================================================================

func TestMatchesPeriod_BasicMatch(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Monday — has active time
	from := time.Date(2024, 6, 17, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 18, 0, 0, 0, 0, time.UTC)
	require.True(t, s.MatchesPeriod(from, to))
}

func TestMatchesPeriod_NoMatch(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Saturday — no active time
	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	to := time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC)
	require.False(t, s.MatchesPeriod(from, to))
}

func TestMatchesPeriod_WeekRange_HasWeekday(t *testing.T) {
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// Full week — contains weekdays → true
	from := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC) // Saturday
	to := time.Date(2024, 6, 22, 0, 0, 0, 0, time.UTC)
	require.True(t, s.MatchesPeriod(from, to))
}

func TestMatchesPeriod_MonthRange(t *testing.T) {
	// Summer only: Jun–Aug
	cond, err := NewScheduleCondition(nil, nil,
		[]SchedulePeriod{mustPeriod(t, 6, 1, 8, 31)},
		nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// July — within summer
	require.True(t, s.MatchesPeriod(
		time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC),
	))

	// February — outside summer
	require.False(t, s.MatchesPeriod(
		time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
	))
}

func TestMatchesPeriod_EmptyRange(t *testing.T) {
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	ts := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	require.False(t, s.MatchesPeriod(ts, ts)) // equal
}

func TestMatchesPeriod_NoConditions(t *testing.T) {
	s := NewSchedule(nil, nil, nil, nil, time.UTC)
	require.False(t, s.MatchesPeriod(
		time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC),
	))
}

func TestMatchesPeriod_Timezone_NonUTC(t *testing.T) {
	// Schedule in UTC+9 (Tokyo). Weekdays only.
	tokyo, err := time.LoadLocation("Asia/Tokyo")
	require.NoError(t, err)
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, tokyo)

	// Friday in Tokyo time — should match
	require.True(t, s.MatchesPeriod(
		time.Date(2024, 6, 14, 0, 0, 0, 0, tokyo),
		time.Date(2024, 6, 15, 0, 0, 0, 0, tokyo),
	))

	// Saturday in Tokyo time — should not match
	require.False(t, s.MatchesPeriod(
		time.Date(2024, 6, 15, 0, 0, 0, 0, tokyo),
		time.Date(2024, 6, 16, 0, 0, 0, 0, tokyo),
	))
}

func TestMatchesPeriod_WithExcludes(t *testing.T) {
	// Weekdays, but exclude Christmas
	cond, err := NewScheduleCondition(
		[]ScheduleTimeSlot{mustTimeSlot(t, 9, 0, 17, 0)},
		[]int{1, 2, 3, 4, 5},
		nil, nil, nil, nil,
	)
	require.NoError(t, err)
	excludeCond, err := NewScheduleCondition(nil, nil, nil, []string{"2024-12-25"}, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, []ScheduleCondition{excludeCond}, nil, nil, time.UTC)

	// Christmas day only (Wednesday) — excluded
	require.False(t, s.MatchesPeriod(
		time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 12, 26, 0, 0, 0, 0, time.UTC),
	))

	// Christmas week — other weekdays still match
	require.True(t, s.MatchesPeriod(
		time.Date(2024, 12, 23, 0, 0, 0, 0, time.UTC),
		time.Date(2024, 12, 28, 0, 0, 0, 0, time.UTC),
	))
}

func TestMatchesPeriod_Efficient_StopsEarly(t *testing.T) {
	// Large range but first day matches — should return quickly
	cond, err := NewScheduleCondition(nil, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	s := NewSchedule([]ScheduleCondition{cond}, nil, nil, nil, time.UTC)

	// One year range — but should return true almost instantly
	require.True(t, s.MatchesPeriod(
		time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
	))
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
