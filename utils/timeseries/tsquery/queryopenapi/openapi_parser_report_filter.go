package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"time"
)

func ParseReportFilter(pCtx *ParsingContext, rawFilter ApiReportFilter) (report.Filter, error) {
	rawFilterType, err := rawFilter.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedFilter := rawFilterType.(type) {
	case ApiReportAlignerFilter:
		return parseReportAlignerFilter(typedFilter)
	case ApiConditionReportFilter:
		return parseConditionReportFilter(pCtx, typedFilter)
	case ApiAppendFieldReportFilter:
		return parseAppendFieldReportFilter(pCtx, typedFilter)
	case ApiDropFieldsReportFilter:
		return parseDropFieldsReportFilter(typedFilter)
	case ApiSingleFieldReportFilter:
		return parseSingleFieldReportFilter(pCtx, typedFilter)
	case ApiProjectionReportFilter:
		return parseProjectionReportFilter(typedFilter)
	case ApiOverrideFieldMetadataReportFilter:
		return parseOverrideFieldMetadataReportFilter(typedFilter)
	case ApiScheduleFilter:
		return parseScheduleReportFilter(typedFilter)
	case ApiTimeShiftFilter:
		return parseTimeShiftReportFilter(typedFilter)
	}
	return wrapAndReturnReportFilter(pCtx.plugin.ParseReportFilter(pCtx, rawFilter))("failed parsing report filter with plugin parser")
}

func parseOverrideFieldMetadataReportFilter(filter ApiOverrideFieldMetadataReportFilter) (report.Filter, error) {
	if filter.FieldUrn == "" {
		return nil, badInputErrorf(filter, "fieldUrn is required for overrideFieldMetadata report filter")
	}

	var optUpdatedUrn *string
	if filter.UpdatedUrn != "" {
		optUpdatedUrn = &filter.UpdatedUrn
	}

	var optUpdatedUnit *string
	if filter.UpdatedUnit != "" {
		optUpdatedUnit = &filter.UpdatedUnit
	}

	var optUpdatedMetricKind *tsquery.MetricKind
	if filter.UpdatedMetricKind != "" {
		optUpdatedMetricKind = &filter.UpdatedMetricKind
	}

	var optUpdatedSamplePeriod *time.Duration
	if filter.UpdatedSamplePeriod != "" {
		sp, err := time.ParseDuration(filter.UpdatedSamplePeriod)
		if err != nil {
			return nil, badInputErrorWrap(filter, err, "invalid updatedSamplePeriod")
		}
		optUpdatedSamplePeriod = &sp
	}

	return report.NewOverrideFieldMetadataFilter(
		filter.FieldUrn,
		optUpdatedUrn,
		optUpdatedUnit,
		optUpdatedMetricKind,
		optUpdatedSamplePeriod,
		filter.UpdatedCustomMeta,
	), nil
}

func parseConditionReportFilter(pCtx *ParsingContext, filter ApiConditionReportFilter) (report.Filter, error) {
	booleanField, err := ParseReportField(pCtx, filter.BooleanField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse boolean field for condition report filter: %w", err)
	}
	return report.NewConditionFilter(booleanField), nil
}

func parseAppendFieldReportFilter(pCtx *ParsingContext, filter ApiAppendFieldReportFilter) (report.Filter, error) {
	fieldValue, err := ParseReportField(pCtx, filter.FieldValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field value for append field report filter: %w", err)
	}
	return report.NewAppendFieldFilter(fieldValue, ParseAddFieldMeta(filter.FieldMeta)), nil
}

func parseDropFieldsReportFilter(filter ApiDropFieldsReportFilter) (report.Filter, error) {
	if len(filter.FieldUrns) == 0 {
		return nil, badInputErrorf(filter, "drop fields filter requires at least one field URN")
	}
	return report.NewDropFieldsFilter(filter.FieldUrns...), nil
}

func parseSingleFieldReportFilter(pCtx *ParsingContext, filter ApiSingleFieldReportFilter) (report.Filter, error) {
	fieldValue, err := ParseReportField(pCtx, filter.FieldValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field value for single field report filter: %w", err)
	}
	return report.NewSingleFieldFilter(fieldValue, ParseAddFieldMeta(filter.FieldMeta)), nil
}

func parseProjectionReportFilter(filter ApiProjectionReportFilter) (report.Filter, error) {
	if len(filter.FieldUrns) == 0 {
		return nil, badInputErrorf(filter, "projection filter requires at least one field URN")
	}

	selectedFields := make([]report.SelectedField, 0, len(filter.FieldUrns))
	for _, urn := range filter.FieldUrns {
		selectedFields = append(selectedFields, report.SelectedField{
			Value: report.NewRefFieldValue(urn),
			Meta:  tsquery.AddFieldMeta{Urn: urn},
		})
	}
	return report.NewSelectFieldsFilter(selectedFields), nil
}

func parseReportAlignerFilter(apiAlignerFilter ApiReportAlignerFilter) (report.Filter, error) {
	alignerPeriod, err := ParseAlignmentPeriod(apiAlignerFilter.AlignerPeriod)
	if err != nil {
		return nil, err
	}

	var af report.AlignerFilter
	if apiAlignerFilter.FillMode != nil {
		if err := apiAlignerFilter.FillMode.Validate(); err != nil {
			return nil, badInputErrorWrap(apiAlignerFilter, err, "invalid fillMode for aligner report filter")
		}
		af = report.NewInterpolatingAlignerFilter(alignerPeriod, *apiAlignerFilter.FillMode)
	} else {
		af = report.NewAlignerFilter(alignerPeriod)
	}

	if apiAlignerFilter.FieldAlignments != nil {
		for urn, fa := range *apiAlignerFilter.FieldAlignments {
			if urn == "" {
				return nil, badInputErrorf(apiAlignerFilter, "fieldAlignments contains an empty field urn")
			}
			if fa.Reduction != nil {
				if err := fa.Reduction.Validate(); err != nil {
					return nil, badInputErrorWrap(apiAlignerFilter, err, "invalid reduction for field %q", urn)
				}
				af = af.WithFieldReduction(urn, *fa.Reduction)
			}
			if fa.FillMode != nil {
				if err := fa.FillMode.Validate(); err != nil {
					return nil, badInputErrorWrap(apiAlignerFilter, err, "invalid fillMode for field %q", urn)
				}
				af = af.WithFieldFillMode(urn, *fa.FillMode)
			}
		}
	}

	return af, nil
}

func parseTimeShiftReportFilter(tf ApiTimeShiftFilter) (report.Filter, error) {
	if tf.OffsetSeconds == 0 {
		return nil, badInputErrorf(tf, "offsetSeconds must be non-zero")
	}
	return report.NewTimeShiftFilter(tf.OffsetSeconds), nil
}

func parseScheduleReportFilter(sf ApiScheduleFilter) (report.Filter, error) {
	schedule, err := ParseSchedule(sf.Schedule)
	if err != nil {
		return nil, fmt.Errorf("failed to parse schedule report filter: %w", err)
	}
	return report.NewScheduleFilter(schedule), nil
}

func wrapAndReturnReportFilter(v report.Filter, err error) func(format string, a ...any) (report.Filter, error) {
	return func(format string, a ...any) (report.Filter, error) {
		if err != nil {
			return nil, fmt.Errorf(format+": %w", append(a, err)...)
		}
		return v, nil
	}
}
