package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
)

var _ Filter = ScheduleFilter{}

// ScheduleFilter filters time series records based on a pre-processed Schedule.
// Records whose timestamp falls within the schedule are kept; others are dropped.
type ScheduleFilter struct {
	schedule Schedule
}

// NewScheduleFilter creates a new ScheduleFilter wrapping the given Schedule.
func NewScheduleFilter(schedule Schedule) ScheduleFilter {
	return ScheduleFilter{schedule: schedule}
}

func (sf ScheduleFilter) Filter(ctx context.Context, result Result) (Result, error) {
	filteredStream := result.Data().FilterWithErAndCtx(
		func(ctx context.Context, record timeseries.TsRecord[any]) (bool, error) {
			return sf.schedule.Matches(record.Timestamp), nil
		})

	return Result{
		meta: result.Meta(),
		data: filteredStream,
	}, nil
}
