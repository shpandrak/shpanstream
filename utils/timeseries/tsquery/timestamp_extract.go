package tsquery

import (
	"fmt"
	"time"
)

type TimestampExtractComponent string

const (
	TimestampExtractYear       TimestampExtractComponent = "year"
	TimestampExtractQuarter    TimestampExtractComponent = "quarter"
	TimestampExtractMonth      TimestampExtractComponent = "month"
	TimestampExtractDayOfYear  TimestampExtractComponent = "dayOfYear"
	TimestampExtractDayOfMonth TimestampExtractComponent = "dayOfMonth"
	TimestampExtractDayOfWeek  TimestampExtractComponent = "dayOfWeek"
	TimestampExtractHour       TimestampExtractComponent = "hour"
	TimestampExtractMinute     TimestampExtractComponent = "minute"
	TimestampExtractEpochMilli TimestampExtractComponent = "epochMillis"
)

func (c TimestampExtractComponent) Validate() error {
	switch c {
	case TimestampExtractYear, TimestampExtractQuarter, TimestampExtractMonth,
		TimestampExtractDayOfYear, TimestampExtractDayOfMonth, TimestampExtractDayOfWeek,
		TimestampExtractHour, TimestampExtractMinute, TimestampExtractEpochMilli:
		return nil
	}
	return fmt.Errorf("invalid timestamp extract component: %s", c)
}

// ExtractTimestampComponent extracts a calendar component from a time.Time value.
func ExtractTimestampComponent(t time.Time, component TimestampExtractComponent) (int64, error) {
	switch component {
	case TimestampExtractYear:
		return int64(t.Year()), nil
	case TimestampExtractQuarter:
		return int64((int(t.Month())-1)/3 + 1), nil
	case TimestampExtractMonth:
		return int64(t.Month()), nil
	case TimestampExtractDayOfYear:
		return int64(t.YearDay()), nil
	case TimestampExtractDayOfMonth:
		return int64(t.Day()), nil
	case TimestampExtractDayOfWeek:
		// ISO weekday: Monday=1 .. Sunday=7
		day := t.Weekday()
		if day == time.Sunday {
			return 7, nil
		}
		return int64(day), nil
	case TimestampExtractHour:
		return int64(t.Hour()), nil
	case TimestampExtractMinute:
		return int64(t.Minute()), nil
	case TimestampExtractEpochMilli:
		return t.UnixMilli(), nil
	default:
		return 0, fmt.Errorf("unsupported timestamp extract component: %s", component)
	}
}
