package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/aggregation"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
)

func ParseAggregation(
	pCtx *ParsingContext,
	apiAggregation ApiAggregation,
) (aggregation.Aggregator, error) {
	rawAgg, err := apiAggregation.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedAgg := rawAgg.(type) {
	case ApiFromDatasourceAggregation:
		return parseFromDatasourceAggregation(pCtx, typedAgg)
	case ApiFromReportAggregation:
		return parseFromReportAggregation(pCtx, typedAgg)
	case ApiCompositeAggregation:
		return parseCompositeAggregation(pCtx, typedAgg)
	default:
		return nil, fmt.Errorf("unsupported aggregation type: %T", rawAgg)
	}
}

func parseFromDatasourceAggregation(
	pCtx *ParsingContext,
	apiAgg ApiFromDatasourceAggregation,
) (*aggregation.FromDatasourceAggregation, error) {
	// Parse the datasource
	ds, err := ParseDatasource(pCtx, apiAgg.Datasource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse datasource for aggregation: %w", err)
	}

	// Parse aggregation fields
	fields := make([]aggregation.AggregationFieldDef, len(apiAgg.Fields))
	for i, apiField := range apiAgg.Fields {
		field, err := parseAggregationField(pCtx, apiField)
		if err != nil {
			return nil, fmt.Errorf("failed to parse aggregation field %d: %w", i, err)
		}
		fields[i] = field
	}

	return aggregation.NewFromDatasourceAggregation(ds, fields), nil
}

func parseAggregationField(pCtx *ParsingContext, apiField ApiAggregationField) (aggregation.AggregationFieldDef, error) {
	field := aggregation.AggregationFieldDef{
		ReductionType: apiField.ReductionType,
	}

	// Parse optional field metadata
	if apiField.FieldMeta != nil {
		addFieldMeta := ParseAddFieldMeta(*apiField.FieldMeta)
		field.AddFieldMeta = &addFieldMeta
	}

	// Parse optional empty value
	if apiField.EmptyValue != nil {
		emptyValue, err := ParseQueryField(pCtx, *apiField.EmptyValue)
		if err != nil {
			return aggregation.AggregationFieldDef{}, fmt.Errorf("failed to parse emptyValue: %w", err)
		}
		staticValue, ok := emptyValue.(datasource.StaticValue)
		if !ok {
			return aggregation.AggregationFieldDef{}, fmt.Errorf("emptyValue must be a static value (e.g., constant), got %T", emptyValue)
		}
		field.EmptyValue = staticValue
	}

	return field, nil
}

func parseFromReportAggregation(
	pCtx *ParsingContext,
	apiAgg ApiFromReportAggregation,
) (*aggregation.FromReportAggregation, error) {
	// Parse the report datasource
	reportDs, err := ParseReportDatasource(pCtx, apiAgg.ReportDatasource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse report datasource for aggregation: %w", err)
	}

	// Parse report aggregation fields
	fields := make([]aggregation.ReportAggregationFieldDef, len(apiAgg.Fields))
	for i, apiField := range apiAgg.Fields {
		field := aggregation.ReportAggregationFieldDef{
			ReductionType:  apiField.ReductionType,
			SourceFieldUrn: apiField.SourceFieldUrn,
		}

		// Parse optional field metadata
		if apiField.FieldMeta != nil {
			addFieldMeta := ParseAddFieldMeta(*apiField.FieldMeta)
			field.AddFieldMeta = &addFieldMeta
		}

		// Parse optional empty value
		if apiField.EmptyValue != nil {
			emptyValue, err := ParseQueryField(pCtx, *apiField.EmptyValue)
			if err != nil {
				return nil, fmt.Errorf("failed to parse emptyValue for report aggregation field %d: %w", i, err)
			}
			staticValue, ok := emptyValue.(datasource.StaticValue)
			if !ok {
				return nil, fmt.Errorf("emptyValue must be a static value (e.g., constant), got %T", emptyValue)
			}
			field.EmptyValue = staticValue
		}

		fields[i] = field
	}

	return aggregation.NewFromReportAggregation(reportDs, fields), nil
}

func parseCompositeAggregation(
	pCtx *ParsingContext,
	apiAgg ApiCompositeAggregation,
) (*aggregation.CompositeAggregation, error) {
	aggregators := make([]aggregation.Aggregator, len(apiAgg.Aggregations))
	for i, subApiAgg := range apiAgg.Aggregations {
		parsed, err := ParseAggregation(pCtx, subApiAgg)
		if err != nil {
			return nil, fmt.Errorf("failed to parse composite aggregation[%d]: %w", i, err)
		}
		aggregators[i] = parsed
	}

	return aggregation.NewCompositeAggregation(aggregators...), nil
}
