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
	}
	return wrapAndReturn(pCtx.ParseFilter(pCtx, rawFilter))("failed parsing filter with plugin parser")
}

func parseConditionFilter(pCtx *ParsingContext, conditionFilter ApiConditionFilter) (datasource.Filter, error) {
	booleanField, err := parseQueryField(pCtx, conditionFilter.BooleanField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse boolean field for condition filter: %w", err)
	}
	return datasource.NewConditionFilter(booleanField), nil
}

func parseFieldValueFilter(pCtx *ParsingContext, fieldValueFilter ApiFieldValueFilter) (datasource.Filter, error) {
	f, err := parseQueryField(pCtx, fieldValueFilter.FieldValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field for field value filter: %w", err)
	}
	addFieldMeta := parseAddFieldMeta(fieldValueFilter.FieldMeta)
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

func parseAlignerFilter(apiAlignerFilter ApiAlignerFilter) (datasource.Filter, error) {
	alignerPeriod, err := parseAlignmentPeriod(apiAlignerFilter.AlignerPeriod)
	if err != nil {
		return nil, err
	}
	// Note: AlignmentFunction is present in the API, but datasource.AlignerFilter doesn't use it
	// as it always uses time-weighted interpolation
	return datasource.NewAlignerFilter(alignerPeriod), nil
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

func parseAddFieldMeta(apiMeta ApiAddFieldMeta) tsquery.AddFieldMeta {
	return tsquery.AddFieldMeta{
		Urn:          apiMeta.Uri,
		CustomMeta:   apiMeta.CustomMetadata,
		OverrideUnit: apiMeta.OverrideUnit,
	}
}
