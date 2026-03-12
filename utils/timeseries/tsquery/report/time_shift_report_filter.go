package report

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"time"
)

var _ Filter = TimeShiftFilter{}

// TimeShiftFilter shifts all timestamps in the report stream by a fixed offset.
// Positive values shift forward, negative values shift backward.
type TimeShiftFilter struct {
	offset time.Duration
}

// NewTimeShiftFilter creates a new TimeShiftFilter with the given offset in seconds.
func NewTimeShiftFilter(offsetSeconds int64) TimeShiftFilter {
	return TimeShiftFilter{offset: time.Duration(offsetSeconds) * time.Second}
}

func (f TimeShiftFilter) Filter(_ context.Context, result Result) (Result, error) {
	return NewResult(result.FieldsMeta(), stream.Map(result.Stream(), func(record timeseries.TsRecord[[]any]) timeseries.TsRecord[[]any] {
		record.Timestamp = record.Timestamp.Add(f.offset)
		return record
	})), nil
}
