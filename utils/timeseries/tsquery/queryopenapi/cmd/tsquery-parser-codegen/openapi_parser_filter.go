//go:build ignore

package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"time"
)

func ParseFilter(pCtx *ParsingContext, rawFilter ApiQueryFilter) (datasource.Filter, error) {
	rawFilterType, err := rawFilter.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedFilter := rawFilterType.(type) {
	case ApiAlignerFilter:
		return ParseAlignerFilter(typedFilter)
	case ApiConditionFilter:
		return parseConditionFilter(pCtx, typedFilter)
	case ApiFieldValueFilter:
		return parseFieldValueFilter(pCtx, typedFilter)
	case ApiOverrideFieldMetadataFilter:
		return parseOverrideFieldMetadataFilter(typedFilter)
	case ApiDeltaFilter:
		return parseDeltaFilter(typedFilter)
	case ApiRateFilter:
		return parseRateFilter(typedFilter)
	case ApiScheduleFilter:
		return parseScheduleFilter(typedFilter)
	case ApiTimeShiftFilter:
		return parseTimeShiftFilter(typedFilter)
	}
	return wrapAndReturn(pCtx.plugin.ParseFilter(pCtx, rawFilter))("failed parsing filter with plugin parser")
}

func parseConditionFilter(pCtx *ParsingContext, conditionFilter ApiConditionFilter) (datasource.Filter, error) {
	booleanField, err := ParseQueryField(pCtx, conditionFilter.BooleanField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse boolean field for condition filter: %w", err)
	}
	return datasource.NewConditionFilter(booleanField), nil
}

func parseFieldValueFilter(pCtx *ParsingContext, fieldValueFilter ApiFieldValueFilter) (datasource.Filter, error) {
	f, err := ParseQueryField(pCtx, fieldValueFilter.FieldValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field for field value filter: %w", err)
	}
	addFieldMeta := ParseAddFieldMeta(fieldValueFilter.FieldMeta)
	return datasource.NewFieldValueFilter(f, addFieldMeta), nil
}

func parseOverrideFieldMetadataFilter(overrideFilter ApiOverrideFieldMetadataFilter) (datasource.Filter, error) {
	var optUpdatedUrn *string
	if overrideFilter.UpdatedUrn != "" {
		optUpdatedUrn = &overrideFilter.UpdatedUrn
	}

	var optUpdatedUnit *string
	if overrideFilter.UpdatedUnit != "" {
		optUpdatedUnit = &overrideFilter.UpdatedUnit
	}

	return datasource.NewOverrideFieldMetadataFilter(
		optUpdatedUrn,
		optUpdatedUnit,
		overrideFilter.UpdatedCustomMeta,
	), nil
}

func parseDeltaFilter(deltaFilter ApiDeltaFilter) (datasource.Filter, error) {
	if deltaFilter.MaxCounterValue > 0 && !deltaFilter.NonNegative {
		return nil, badInputErrorf(deltaFilter, "maxCounterValue can only be used when nonNegative is true")
	}
	if deltaFilter.EmitOnReset && !deltaFilter.NonNegative {
		return nil, badInputErrorf(deltaFilter, "emitOnReset can only be used when nonNegative is true")
	}
	if deltaFilter.EmitOnReset && deltaFilter.MaxCounterValue > 0 {
		return nil, badInputErrorf(deltaFilter, "emitOnReset and maxCounterValue are mutually exclusive")
	}
	return datasource.NewDeltaFilter(deltaFilter.NonNegative, deltaFilter.MaxCounterValue, deltaFilter.EmitOnReset), nil
}

func parseRateFilter(rateFilter ApiRateFilter) (datasource.Filter, error) {
	if rateFilter.MaxCounterValue > 0 && !rateFilter.NonNegative {
		return nil, badInputErrorf(rateFilter, "maxCounterValue can only be used when nonNegative is true")
	}
	if rateFilter.EmitOnReset && !rateFilter.NonNegative {
		return nil, badInputErrorf(rateFilter, "emitOnReset can only be used when nonNegative is true")
	}
	if rateFilter.EmitOnReset && rateFilter.MaxCounterValue > 0 {
		return nil, badInputErrorf(rateFilter, "emitOnReset and maxCounterValue are mutually exclusive")
	}
	var perSeconds int
	if rateFilter.PerSeconds != nil {
		perSeconds = *rateFilter.PerSeconds
	}
	return datasource.NewRateFilter(rateFilter.OverrideUnit, perSeconds, rateFilter.NonNegative, rateFilter.MaxCounterValue, rateFilter.EmitOnReset), nil
}

func ParseAlignerFilter(apiAlignerFilter ApiAlignerFilter) (datasource.AlignerFilter, error) {
	alignerPeriod, err := ParseAlignmentPeriod(apiAlignerFilter.AlignerPeriod)
	if err != nil {
		return datasource.AlignerFilter{}, err
	}
	var af datasource.AlignerFilter
	if apiAlignerFilter.FillMode != nil {
		switch *apiAlignerFilter.FillMode {
		case timeseries.FillModeLinear, timeseries.FillModeForwardFill:
			// valid
		default:
			return datasource.AlignerFilter{}, fmt.Errorf("unsupported fill mode: %q", *apiAlignerFilter.FillMode)
		}
		af = datasource.NewInterpolatingAlignerFilter(alignerPeriod, *apiAlignerFilter.FillMode)
	} else {
		af = datasource.NewAlignerFilter(alignerPeriod)
	}
	if apiAlignerFilter.BucketReduction != nil {
		af = af.WithBucketReduction(*apiAlignerFilter.BucketReduction)
	}
	return af, nil
}

func ParseAlignmentPeriod(ap ApiAlignmentPeriod) (timeseries.AlignmentPeriod, error) {
	periodValue, err := ap.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedPeriod := periodValue.(type) {
	case ApiCustomAlignmentPeriod:
		return parseCustomAlignmentPeriod(typedPeriod)
	case ApiCalendarAlignmentPeriod:
		return parseCalendarAlignmentPeriod(typedPeriod)
	default:
		return nil, fmt.Errorf("unsupported alignment period type %T", typedPeriod)

	}
}

func parseCalendarAlignmentPeriod(period ApiCalendarAlignmentPeriod) (timeseries.AlignmentPeriod, error) {
	loc, err := time.LoadLocation(period.ZoneId)
	if err != nil {
		return nil, badInputError(period, err)
	}
	switch period.AlignmentPeriodType {
	case ApiCalendarPeriodTypeMonth:
		return timeseries.NewMonthAlignmentPeriod(loc), nil
	case ApiCalendarPeriodTypeWeek:
		return timeseries.NewWeekAlignmentPeriod(loc), nil
	case ApiCalendarPeriodTypeDay:
		return timeseries.NewDayAlignmentPeriod(loc), nil
	case ApiCalendarPeriodTypeHour:
		return timeseries.NewFixedAlignmentPeriod(time.Hour, loc), nil
	case ApiCalendarPeriodTypeQuarterHour:
		return timeseries.NewFixedAlignmentPeriod(time.Minute*15, loc), nil
	case ApiCalendarPeriodTypeQuarter:
		return timeseries.NewQuarterAlignmentPeriod(loc), nil
	case ApiCalendarPeriodTypeYear:
		return timeseries.NewYearAlignmentPeriod(loc), nil
	case ApiCalendarPeriodTypeHalfYear:
		return timeseries.NewHalfYearAlignmentPeriod(loc), nil
	default:
		return nil, badInputErrorf(period, "unsupported calendar alignment period type %v", period.AlignmentPeriodType)
	}

}

func parseCustomAlignmentPeriod(period ApiCustomAlignmentPeriod) (timeseries.AlignmentPeriod, error) {
	loc, err := time.LoadLocation(period.ZoneId)
	if err != nil {
		return nil, badInputError(period, err)
	}
	if period.DurationInMillis <= 0 {
		return nil, badInputErrorf(period, "duration must be positive")
	}
	if period.OffsetInMillis < 0 {
		return nil, badInputErrorf(period, "offsetInMillis must be non-negative")
	}
	duration := time.Duration(period.DurationInMillis) * time.Millisecond
	if period.OffsetInMillis > 0 {
		offset := time.Duration(period.OffsetInMillis) * time.Millisecond
		return timeseries.NewFixedAlignmentPeriodWithOffset(duration, offset, loc), nil
	}
	return timeseries.NewFixedAlignmentPeriod(duration, loc), nil
}

func ParseAddFieldMeta(apiMeta ApiAddFieldMeta) tsquery.AddFieldMeta {
	return tsquery.AddFieldMeta{
		Urn:          apiMeta.Uri,
		CustomMeta:   apiMeta.CustomMetadata,
		OverrideUnit: apiMeta.OverrideUnit,
	}
}

func parseTimeShiftFilter(tf ApiTimeShiftFilter) (datasource.Filter, error) {
	if tf.OffsetSeconds == 0 {
		return nil, badInputErrorf(tf, "offsetSeconds must be non-zero")
	}
	return datasource.NewTimeShiftFilter(tf.OffsetSeconds), nil
}

func parseScheduleFilter(sf ApiScheduleFilter) (datasource.Filter, error) {
	schedule, err := ParseSchedule(sf.Schedule)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schedule filter: %w", err)
	}
	return datasource.NewScheduleFilter(schedule), nil
}

func ParseSchedule(apiSchedule ApiSchedule) (datasource.Schedule, error) {
	var location *time.Location
	if apiSchedule.CustomTimezone != nil {
		loc, err := time.LoadLocation(*apiSchedule.CustomTimezone)
		if err != nil {
			return datasource.Schedule{}, badInputError(apiSchedule, err)
		}
		location = loc
	}

	conditions, err := parseScheduleConditions(apiSchedule.Conditions)
	if err != nil {
		return datasource.Schedule{}, err
	}
	excludeConditions, err := parseScheduleConditions(apiSchedule.ExcludeConditions)
	if err != nil {
		return datasource.Schedule{}, err
	}

	return datasource.NewSchedule(conditions, excludeConditions,
		apiSchedule.StartTime, apiSchedule.EndTime, location), nil
}

func parseScheduleConditions(apiConds []ApiScheduleCondition) ([]datasource.ScheduleCondition, error) {
	if len(apiConds) == 0 {
		return nil, nil
	}
	conditions := make([]datasource.ScheduleCondition, 0, len(apiConds))
	for _, apiCond := range apiConds {
		cond, err := parseScheduleCondition(apiCond)
		if err != nil {
			return nil, err
		}
		conditions = append(conditions, cond)
	}
	return conditions, nil
}

func parseScheduleCondition(apiCond ApiScheduleCondition) (datasource.ScheduleCondition, error) {
	timeSlots := make([]datasource.ScheduleTimeSlot, 0, len(apiCond.TimeSlots))
	for _, ts := range apiCond.TimeSlots {
		slot, err := datasource.NewScheduleTimeSlot(ts.FromHourOfDay, ts.FromMinuteOfHour, ts.ToHourOfDay, ts.ToMinuteOfHour)
		if err != nil {
			return datasource.ScheduleCondition{}, fmt.Errorf("failed to parse schedule time slot: %w", err)
		}
		slot.Description = ts.Description
		timeSlots = append(timeSlots, slot)
	}

	periods := make([]datasource.SchedulePeriod, 0, len(apiCond.Periods))
	for _, p := range apiCond.Periods {
		period, err := datasource.NewSchedulePeriod(p.StartMonth, p.StartDayOfMonth, p.EndMonth, p.EndDayOfMonth)
		if err != nil {
			return datasource.ScheduleCondition{}, fmt.Errorf("failed to parse schedule period: %w", err)
		}
		period.Description = p.Description
		periods = append(periods, period)
	}

	excludePeriods := make([]datasource.SchedulePeriod, 0, len(apiCond.ExcludePeriods))
	for _, p := range apiCond.ExcludePeriods {
		period, err := datasource.NewSchedulePeriod(p.StartMonth, p.StartDayOfMonth, p.EndMonth, p.EndDayOfMonth)
		if err != nil {
			return datasource.ScheduleCondition{}, fmt.Errorf("failed to parse schedule exclude period: %w", err)
		}
		period.Description = p.Description
		excludePeriods = append(excludePeriods, period)
	}

	cond, err := datasource.NewScheduleCondition(timeSlots, apiCond.DaysOfWeek, periods, apiCond.Dates, excludePeriods, apiCond.ExcludeDates)
	if err != nil {
		return datasource.ScheduleCondition{}, err
	}
	cond.Description = apiCond.Description
	return cond, nil
}
