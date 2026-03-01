package report

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
)

var _ Filter = ScheduleFilter{}

// ScheduleFilter filters report rows based on a pre-processed Schedule.
// Records whose timestamp falls within the schedule are kept; others are dropped.
type ScheduleFilter struct {
	schedule datasource.Schedule
}

// NewScheduleFilter creates a new ScheduleFilter wrapping the given Schedule.
func NewScheduleFilter(schedule datasource.Schedule) ScheduleFilter {
	return ScheduleFilter{schedule: schedule}
}

func (sf ScheduleFilter) Filter(ctx context.Context, result Result) (Result, error) {
	filteredStream := result.Stream().FilterWithErAndCtx(
		func(ctx context.Context, record timeseries.TsRecord[[]any]) (bool, error) {
			return sf.schedule.Matches(record.Timestamp), nil
		})
	return NewResult(result.FieldsMeta(), filteredStream), nil
}
