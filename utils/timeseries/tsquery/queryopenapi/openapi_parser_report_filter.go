package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
)

func ParseReportFilter(pCtx *ParsingContext, rawFilter ApiReportFilter) (report.Filter, error) {
	rawFilterType, err := rawFilter.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedFilter := rawFilterType.(type) {
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
	}
	return wrapAndReturnReportFilter(pCtx.ParseReportFilter(pCtx, rawFilter))("failed parsing report filter with plugin parser")
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
	return report.NewAppendFieldFilter(fieldValue, parseAddFieldMeta(filter.FieldMeta)), nil
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
	return report.NewSingleFieldFilter(fieldValue, parseAddFieldMeta(filter.FieldMeta)), nil
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

func wrapAndReturnReportFilter(v report.Filter, err error) func(format string, a ...any) (report.Filter, error) {
	return func(format string, a ...any) (report.Filter, error) {
		if err != nil {
			return nil, fmt.Errorf(format+": %w", append(a, err)...)
		}
		return v, nil
	}
}
