package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/filter"
	"time"
)

func ParseFilter(rawFilter ApiQueryFilter) (filter.Filter, error) {
	rawFilterType, err := rawFilter.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedFilter := rawFilterType.(type) {
	case ApiAlignerFilter:
		return parseAlignerFilter(typedFilter)
	case ApiAppendFieldFilter:
		return parseAppendFieldFilter(typedFilter)
	case ApiDropFieldFilter:
		return parseDropFieldFilter(typedFilter)
	case ApiReplaceFieldFilter:
		return parseReplaceFieldFilter(typedFilter)
	case ApiConditionFilter:
		return parseConditionFilter(typedFilter)
	case ApiSingleFieldFilter:
		return parseSingleFieldFilter(typedFilter)
	case ApiOverrideFieldMetadataFilter:
		return parseOverrideFieldMetadataFilter(typedFilter)
	}
	return nil, fmt.Errorf("filter type %T not supported", rawFilter)
}

func parseAppendFieldFilter(appendFieldFilter ApiAppendFieldFilter) (filter.Filter, error) {
	f, err := parseQueryField(appendFieldFilter.Field)
	if err != nil {
		return nil, fmt.Errorf("failed to parse append field filter: %w", err)
	}
	return filter.NewAppendFieldFilter(f), nil

}

func parseDropFieldFilter(dropFieldFilter ApiDropFieldFilter) (filter.Filter, error) {
	if len(dropFieldFilter.FieldUrns) == 0 {
		return nil, fmt.Errorf("drop field filter must have at least one field URN")
	}
	return filter.NewDropFieldsFilter(dropFieldFilter.FieldUrns...), nil
}

func parseReplaceFieldFilter(replaceFieldFilter ApiReplaceFieldFilter) (filter.Filter, error) {
	f, err := parseQueryField(replaceFieldFilter.Field)
	if err != nil {
		return nil, fmt.Errorf("failed to parse replace field filter: %w", err)
	}
	return filter.NewReplaceFieldFilter(replaceFieldFilter.FieldUrnToReplace, f), nil
}

func parseConditionFilter(conditionFilter ApiConditionFilter) (filter.Filter, error) {
	booleanField, err := parseQueryField(conditionFilter.BooleanField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse boolean field for condition filter: %w", err)
	}
	return filter.NewConditionFilter(booleanField), nil
}

func parseSingleFieldFilter(singleFieldFilter ApiSingleFieldFilter) (filter.Filter, error) {
	f, err := parseQueryField(singleFieldFilter.Field)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field for single field filter: %w", err)
	}
	return filter.NewSingleFieldFilter(f), nil
}

func parseOverrideFieldMetadataFilter(overrideFilter ApiOverrideFieldMetadataFilter) (filter.Filter, error) {
	var optUpdatedUrn *string
	if overrideFilter.UpdatedUrn != "" {
		optUpdatedUrn = &overrideFilter.UpdatedUrn
	}

	var optUpdatedUnit *string
	if overrideFilter.UpdatedUnit != "" {
		optUpdatedUnit = &overrideFilter.UpdatedUnit
	}

	return filter.NewOverrideFieldMetadataFilter(
		overrideFilter.FieldUrn,
		optUpdatedUrn,
		optUpdatedUnit,
		overrideFilter.UpdatedCustomMeta,
	), nil
}

func parseAlignerFilter(apiAlignerFilter ApiAlignerFilter) (filter.Filter, error) {
	alignerPeriod, err := parseAlignmentPeriod(apiAlignerFilter.AlignerPeriod)
	if err != nil {
		return nil, err
	}
	switch apiAlignerFilter.AlignmentFunction {
	case Avg:
		return filter.NewAlignerFilter(alignerPeriod), nil
	default:
		return nil, fmt.Errorf("aligner function %s is not yet supported", apiAlignerFilter.AlignmentFunction)
	}
}

func parseAlignmentPeriod(ap ApiAlignmentPeriod) (timeseries.AlignmentPeriod, error) {
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
	case Month:
		return timeseries.NewMonthAlignmentPeriod(loc), nil
	case Week:
		return timeseries.NewWeekAlignmentPeriod(loc), nil
	case Day:
		return timeseries.NewDayAlignmentPeriod(loc), nil
	case Hour:
		return timeseries.NewFixedAlignmentPeriod(time.Hour, loc), nil
	case QuarterHour:
		return timeseries.NewFixedAlignmentPeriod(time.Minute*15, loc), nil
	case Quarter:
		return timeseries.NewQuarterAlignmentPeriod(loc), nil
	case Year:
		return timeseries.NewYearAlignmentPeriod(loc), nil
	case HalfYear:
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
	return timeseries.NewFixedAlignmentPeriod(time.Duration(period.DurationInMillis)*time.Millisecond, loc), nil
}
