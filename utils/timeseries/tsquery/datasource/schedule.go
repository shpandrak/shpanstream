package datasource

import (
	"fmt"
	"time"
)

// ScheduleTimeSlot holds pre-computed minute-of-day boundaries for fast comparison.
type ScheduleTimeSlot struct {
	fromMinutes int // fromHourOfDay*60 + fromMinuteOfHour (0–1439)
	toMinutes   int // toHourOfDay*60 + toMinuteOfHour     (0–1439)
}

// SchedulePeriod holds month/day boundaries for calendar-period matching.
type SchedulePeriod struct {
	startMonth int // 1–12
	startDay   int // 1–31
	endMonth   int // 1–12
	endDay     int // 1–31
}

// maxDaysInMonth returns the maximum valid day for a given month (1–12).
// Feb allows 29 to accommodate leap years.
func maxDaysInMonth(month int) int {
	switch month {
	case 2:
		return 29
	case 4, 6, 9, 11:
		return 30
	default:
		return 31
	}
}

// NewScheduleTimeSlot creates a validated time slot from hour/minute components.
func NewScheduleTimeSlot(fromHour, fromMinute, toHour, toMinute int) (ScheduleTimeSlot, error) {
	if fromHour < 0 || fromHour > 23 {
		return ScheduleTimeSlot{}, fmt.Errorf("invalid fromHour: %d, must be 0–23", fromHour)
	}
	if fromMinute < 0 || fromMinute > 59 {
		return ScheduleTimeSlot{}, fmt.Errorf("invalid fromMinute: %d, must be 0–59", fromMinute)
	}
	if toHour < 0 || toHour > 23 {
		return ScheduleTimeSlot{}, fmt.Errorf("invalid toHour: %d, must be 0–23", toHour)
	}
	if toMinute < 0 || toMinute > 59 {
		return ScheduleTimeSlot{}, fmt.Errorf("invalid toMinute: %d, must be 0–59", toMinute)
	}
	fromMinutes := fromHour*60 + fromMinute
	toMinutes := toHour*60 + toMinute
	if fromMinutes == toMinutes {
		return ScheduleTimeSlot{}, fmt.Errorf("zero-length time slot: from %02d:%02d equals to %02d:%02d", fromHour, fromMinute, toHour, toMinute)
	}
	return ScheduleTimeSlot{fromMinutes: fromMinutes, toMinutes: toMinutes}, nil
}

// NewSchedulePeriod creates a validated schedule period from month/day components.
func NewSchedulePeriod(startMonth, startDay, endMonth, endDay int) (SchedulePeriod, error) {
	if startMonth < 1 || startMonth > 12 {
		return SchedulePeriod{}, fmt.Errorf("invalid startMonth: %d, must be 1–12", startMonth)
	}
	if endMonth < 1 || endMonth > 12 {
		return SchedulePeriod{}, fmt.Errorf("invalid endMonth: %d, must be 1–12", endMonth)
	}
	if startDay < 1 || startDay > maxDaysInMonth(startMonth) {
		return SchedulePeriod{}, fmt.Errorf("invalid startDay: %d for month %d, must be 1–%d", startDay, startMonth, maxDaysInMonth(startMonth))
	}
	if endDay < 1 || endDay > maxDaysInMonth(endMonth) {
		return SchedulePeriod{}, fmt.Errorf("invalid endDay: %d for month %d, must be 1–%d", endDay, endMonth, maxDaysInMonth(endMonth))
	}
	return SchedulePeriod{startMonth: startMonth, startDay: startDay, endMonth: endMonth, endDay: endDay}, nil
}

// ScheduleCondition is the pre-processed form of a single condition.
// Within a condition all specified field-types are AND'd.
// Items within a field-type (multiple timeSlots, multiple periods) are OR'd.
type ScheduleCondition struct {
	timeSlots     []ScheduleTimeSlot
	daysOfWeek    [7]bool // indexed by time.Weekday (0=Sunday .. 6=Saturday)
	hasDaysOfWeek bool    // true if daysOfWeek constraint is active
	periods       []SchedulePeriod
	dates         map[string]bool // set of "2006-01-02" strings for O(1) lookup
}

// Schedule is the pre-processed, ready-to-evaluate schedule.
type Schedule struct {
	conditions        []ScheduleCondition
	excludeConditions []ScheduleCondition
	startTime         *time.Time
	endTime           *time.Time
	location          *time.Location // pre-loaded; nil means UTC
}

// NewSchedule creates a new Schedule with pre-processed conditions.
func NewSchedule(
	conditions []ScheduleCondition,
	excludeConditions []ScheduleCondition,
	startTime *time.Time,
	endTime *time.Time,
	location *time.Location,
) Schedule {
	if location == nil {
		location = time.UTC
	}
	return Schedule{
		conditions:        conditions,
		excludeConditions: excludeConditions,
		startTime:         startTime,
		endTime:           endTime,
		location:          location,
	}
}

// NewScheduleCondition creates a pre-processed schedule condition.
// daysOfWeek values must be 0–6 (matching time.Weekday). dates must be "2006-01-02" format.
func NewScheduleCondition(
	timeSlots []ScheduleTimeSlot,
	daysOfWeek []int,
	periods []SchedulePeriod,
	dates []string,
) (ScheduleCondition, error) {
	cond := ScheduleCondition{}

	cond.timeSlots = timeSlots

	if len(daysOfWeek) > 0 {
		cond.hasDaysOfWeek = true
		for _, d := range daysOfWeek {
			if d < 0 || d > 6 {
				return ScheduleCondition{}, fmt.Errorf("invalid day of week: %d, must be 0–6", d)
			}
			cond.daysOfWeek[d] = true
		}
	}

	cond.periods = periods

	if len(dates) > 0 {
		cond.dates = make(map[string]bool, len(dates))
		for _, d := range dates {
			if _, err := time.Parse("2006-01-02", d); err != nil {
				return ScheduleCondition{}, fmt.Errorf("invalid date format %q, expected YYYY-MM-DD: %w", d, err)
			}
			cond.dates[d] = true
		}
	}

	return cond, nil
}

// Matches returns true if the given timestamp falls within the schedule.
func (s Schedule) Matches(ts time.Time) bool {
	// Check schedule bounds
	if s.startTime != nil && ts.Before(*s.startTime) {
		return false
	}
	if s.endTime != nil && !ts.Before(*s.endTime) {
		return false
	}

	// Convert to schedule timezone
	localTime := ts.In(s.location)

	// At least one include condition must match (no conditions → no match)
	matched := false
	for i := range s.conditions {
		if conditionMatches(&s.conditions[i], localTime) {
			matched = true
			break
		}
	}
	if !matched {
		return false
	}

	// No exclude condition may match
	for i := range s.excludeConditions {
		if conditionMatches(&s.excludeConditions[i], localTime) {
			return false
		}
	}

	return true
}

// conditionMatches checks if a single condition matches the given local time.
// All specified field-types are AND'd; items within a field-type are OR'd.
func conditionMatches(cond *ScheduleCondition, localTime time.Time) bool {
	// Day of week check (AND)
	if cond.hasDaysOfWeek && !cond.daysOfWeek[localTime.Weekday()] {
		return false
	}

	// Time slot check (OR across slots, AND with other fields)
	if len(cond.timeSlots) > 0 {
		currentMinutes := localTime.Hour()*60 + localTime.Minute()
		matched := false
		for i := range cond.timeSlots {
			if timeSlotMatches(&cond.timeSlots[i], currentMinutes) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Period check (OR across periods, AND with other fields)
	if len(cond.periods) > 0 {
		month := int(localTime.Month())
		day := localTime.Day()
		matched := false
		for i := range cond.periods {
			if periodMatches(&cond.periods[i], month, day) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	// Date check (AND with other fields)
	if len(cond.dates) > 0 {
		dateStr := localTime.Format("2006-01-02")
		if !cond.dates[dateStr] {
			return false
		}
	}

	return true
}

// timeSlotMatches checks if the current minute-of-day falls within the time slot.
// from is inclusive, to is exclusive. Supports cross-midnight slots.
func timeSlotMatches(slot *ScheduleTimeSlot, currentMinutes int) bool {
	if slot.fromMinutes <= slot.toMinutes {
		// Normal slot: e.g. 09:00–17:00
		return currentMinutes >= slot.fromMinutes && currentMinutes < slot.toMinutes
	}
	// Cross-midnight slot: e.g. 22:00–06:00
	return currentMinutes >= slot.fromMinutes || currentMinutes < slot.toMinutes
}

// periodMatches checks if the given month/day falls within the period.
// Both start and end are inclusive. Supports cross-year periods.
func periodMatches(period *SchedulePeriod, month, day int) bool {
	current := month*100 + day
	start := period.startMonth*100 + period.startDay
	end := period.endMonth*100 + period.endDay

	if start <= end {
		// Normal period: e.g. Jan 1 – Jun 30
		return current >= start && current <= end
	}
	// Cross-year period: e.g. Nov 1 – Feb 28
	return current >= start || current <= end
}
