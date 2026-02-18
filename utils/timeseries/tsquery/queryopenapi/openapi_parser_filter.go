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
		return parseAlignerFilter(typedFilter)
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
	return datasource.NewDeltaFilter(deltaFilter.NonNegative, deltaFilter.MaxCounterValue), nil
}

func parseRateFilter(rateFilter ApiRateFilter) (datasource.Filter, error) {
	return datasource.NewRateFilter(rateFilter.OverrideUnit), nil
}

func parseAlignerFilter(apiAlignerFilter ApiAlignerFilter) (datasource.AlignerFilter, error) {
	alignerPeriod, err := ParseAlignmentPeriod(apiAlignerFilter.AlignerPeriod)
	if err != nil {
		return datasource.AlignerFilter{}, err
	}
	if apiAlignerFilter.FillMode != nil {
		switch *apiAlignerFilter.FillMode {
		case timeseries.FillModeLinear, timeseries.FillModeForwardFill:
			// valid
		default:
			return datasource.AlignerFilter{}, fmt.Errorf("unsupported fill mode: %q", *apiAlignerFilter.FillMode)
		}
		return datasource.NewInterpolatingAlignerFilter(alignerPeriod, *apiAlignerFilter.FillMode), nil
	}
	return datasource.NewAlignerFilter(alignerPeriod), nil
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
	return timeseries.NewFixedAlignmentPeriod(time.Duration(period.DurationInMillis)*time.Millisecond, loc), nil
}

func ParseAddFieldMeta(apiMeta ApiAddFieldMeta) tsquery.AddFieldMeta {
	return tsquery.AddFieldMeta{
		Urn:          apiMeta.Uri,
		CustomMeta:   apiMeta.CustomMetadata,
		OverrideUnit: apiMeta.OverrideUnit,
	}
}
