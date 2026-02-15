package timeseries

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"io"
	"time"
)

type CalendarPeriod string

type AlignmentPeriod interface {
	// GetStartTime returns the start time of the period.
	GetStartTime(time time.Time) time.Time
	// GetEndTime returns the end time of the period.
	GetEndTime(time time.Time) time.Time
}

func AlignmentPeriodClassifierFunc[T any](ap AlignmentPeriod) func(a TsRecord[T]) time.Time {
	return func(a TsRecord[T]) time.Time { return ap.GetStartTime(a.Timestamp) }
}

// AlignedTimestampsStream generates a lazy stream of aligned period start timestamps
// from 'from' to 'to' (exclusive). The stream generates timestamps on-the-fly without
// materializing them in memory.
func AlignedTimestampsStream(ap AlignmentPeriod, from, to time.Time) stream.Stream[time.Time] {
	// Align 'from' to the start of its period
	current := ap.GetStartTime(from)

	return stream.NewSimpleStream(
		func(ctx context.Context) (time.Time, error) {
			if ctx.Err() != nil {
				return time.Time{}, ctx.Err()
			}
			if !current.Before(to) {
				return time.Time{}, io.EOF
			}
			result := current
			current = ap.GetEndTime(current) // advance to next period
			return result, nil
		},
	)
}

//clusterClassifierFunc

func NewDayAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return DayAlignmentPeriod{Location: location}
}

func NewWeekAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return WeekAlignmentPeriod{Location: location}
}

func NewMonthAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return MonthAlignmentPeriod{Location: location}
}

func NewQuarterAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return QuarterAlignmentPeriod{Location: location}
}

func NewYearAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return YearAlignmentPeriod{Location: location}
}

func NewHalfYearAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return HalfYearAlignmentPeriod{Location: location}
}

func NewFixedAlignmentPeriod(duration time.Duration, location *time.Location) AlignmentPeriod {
	if duration <= 0 {
		panic("invalid fixed duration for alignment of timeseries stream")
	}
	if location == nil {
		location = time.UTC
	}
	return FixedAlignmentPeriod{Duration: duration, Location: location}
}

type DayAlignmentPeriod struct {
	*time.Location
}
type WeekAlignmentPeriod struct {
	*time.Location
}
type MonthAlignmentPeriod struct {
	*time.Location
}
type QuarterAlignmentPeriod struct {
	*time.Location
}
type YearAlignmentPeriod struct {
	*time.Location
}
type HalfYearAlignmentPeriod struct {
	*time.Location
}

type FixedAlignmentPeriod struct {
	Duration time.Duration
	Location *time.Location
}

func (f FixedAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(f.Location)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, f.Location)
	since := t.Sub(epoch)
	aligned := since.Truncate(f.Duration)
	return epoch.Add(aligned)
}

func (f FixedAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return f.GetStartTime(t).Add(f.Duration)
}
func (d DayAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(d.Location)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, d.Location)
}

func (d DayAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return d.GetStartTime(t).Add(24 * time.Hour)
}

func (w WeekAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(w.Location)
	weekday := int(t.Weekday())
	if weekday == 0 {
		weekday = 7 // Sunday = 0 in Go, shift to 7 for ISO-like Monday-first
	}
	start := t.AddDate(0, 0, -weekday+1) // go back to Monday
	return time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, w.Location)
}

func (w WeekAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return w.GetStartTime(t).AddDate(0, 0, 7)
}

func (m MonthAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(m.Location)
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, m.Location)
}

func (m MonthAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return m.GetStartTime(t).AddDate(0, 1, 0)
}

func (q QuarterAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(q.Location)
	month := ((int(t.Month())-1)/3)*3 + 1
	return time.Date(t.Year(), time.Month(month), 1, 0, 0, 0, 0, q.Location)
}

func (q QuarterAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return q.GetStartTime(t).AddDate(0, 3, 0)
}

func (h HalfYearAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(h.Location)
	month := 1
	if t.Month() >= 7 {
		month = 7
	}
	return time.Date(t.Year(), time.Month(month), 1, 0, 0, 0, 0, h.Location)
}

func (h HalfYearAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return h.GetStartTime(t).AddDate(0, 6, 0)
}

func (y YearAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(y.Location)
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, y.Location)
}

func (y YearAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return y.GetStartTime(t).AddDate(1, 0, 0)
}
