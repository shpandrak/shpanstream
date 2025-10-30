package timeseries

import "time"

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

//clusterClassifierFunc

func NewDayAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return dayAlignmentPeriod{Location: location}
}

func NewWeekAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return weekAlignmentPeriod{Location: location}
}

func NewMonthAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return monthAlignmentPeriod{Location: location}
}

func NewQuarterAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return quarterAlignmentPeriod{Location: location}
}

func NewYearAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return yearAlignmentPeriod{Location: location}
}

func NewHalfYearAlignmentPeriod(location *time.Location) AlignmentPeriod {
	return halfYearAlignmentPeriod{Location: location}
}

func NewFixedAlignmentPeriod(duration time.Duration, location *time.Location) AlignmentPeriod {
	if duration <= 0 {
		panic("invalid fixed duration for alignment of timeseries stream")
	}
	if location == nil {
		location = time.UTC
	}
	return fixedAlignmentPeriod{Duration: duration, Location: location}
}

type dayAlignmentPeriod struct {
	*time.Location
}
type weekAlignmentPeriod struct {
	*time.Location
}
type monthAlignmentPeriod struct {
	*time.Location
}
type quarterAlignmentPeriod struct {
	*time.Location
}
type yearAlignmentPeriod struct {
	*time.Location
}
type halfYearAlignmentPeriod struct {
	*time.Location
}

type fixedAlignmentPeriod struct {
	Duration time.Duration
	Location *time.Location
}

func (f fixedAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(f.Location)
	epoch := time.Date(1970, 1, 1, 0, 0, 0, 0, f.Location)
	since := t.Sub(epoch)
	aligned := since.Truncate(f.Duration)
	return epoch.Add(aligned)
}

func (f fixedAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return f.GetStartTime(t).Add(f.Duration)
}
func (d dayAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(d.Location)
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, d.Location)
}

func (d dayAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return d.GetStartTime(t).Add(24 * time.Hour)
}

func (w weekAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(w.Location)
	weekday := int(t.Weekday())
	if weekday == 0 {
		weekday = 7 // Sunday = 0 in Go, shift to 7 for ISO-like Monday-first
	}
	start := t.AddDate(0, 0, -weekday+1) // go back to Monday
	return time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, w.Location)
}

func (w weekAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return w.GetStartTime(t).AddDate(0, 0, 7)
}

func (m monthAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(m.Location)
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, m.Location)
}

func (m monthAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return m.GetStartTime(t).AddDate(0, 1, 0)
}

func (q quarterAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(q.Location)
	month := ((int(t.Month())-1)/3)*3 + 1
	return time.Date(t.Year(), time.Month(month), 1, 0, 0, 0, 0, q.Location)
}

func (q quarterAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return q.GetStartTime(t).AddDate(0, 3, 0)
}

func (h halfYearAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(h.Location)
	month := 1
	if t.Month() >= 7 {
		month = 7
	}
	return time.Date(t.Year(), time.Month(month), 1, 0, 0, 0, 0, h.Location)
}

func (h halfYearAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return h.GetStartTime(t).AddDate(0, 6, 0)
}

func (y yearAlignmentPeriod) GetStartTime(t time.Time) time.Time {
	t = t.In(y.Location)
	return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, y.Location)
}

func (y yearAlignmentPeriod) GetEndTime(t time.Time) time.Time {
	return y.GetStartTime(t).AddDate(1, 0, 0)
}
