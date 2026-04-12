//go:build ignore

package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
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
	case ApiExpressionAggregation:
		return parseExpressionAggregation(pCtx, typedAgg)
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

		// Validate and parse compareFieldUrn for paired reductions
		if apiField.ReductionType.IsPaired() {
			if apiField.CompareFieldUrn == nil || *apiField.CompareFieldUrn == "" {
				return nil, fmt.Errorf("report aggregation field %d: paired reduction %q requires compareFieldUrn to identify the predicted/forecast field",
					i, apiField.ReductionType)
			}
			field.CompareFieldUrn = *apiField.CompareFieldUrn
		} else if apiField.CompareFieldUrn != nil && *apiField.CompareFieldUrn != "" {
			return nil, fmt.Errorf("report aggregation field %d: compareFieldUrn must not be set for non-paired reduction %q (only paired reductions like mae, rmse, mbe, mape, pearson, r2 use it)",
				i, apiField.ReductionType)
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

func parseExpressionAggregation(
	pCtx *ParsingContext,
	apiAgg ApiExpressionAggregation,
) (*aggregation.ExpressionAggregation, error) {
	// Parse source aggregation
	source, err := ParseAggregation(pCtx, apiAgg.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source aggregation for expression: %w", err)
	}

	// Parse expression fields
	fields := make([]aggregation.ExpressionAggregationFieldDef, len(apiAgg.Fields))
	for i, apiField := range apiAgg.Fields {
		value, err := ParseAggregationFieldValue(apiField.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to parse expression aggregation field %d: %w", i, err)
		}

		addFieldMeta := ParseAddFieldMeta(apiField.FieldMeta)
		fields[i] = aggregation.ExpressionAggregationFieldDef{
			AddFieldMeta: addFieldMeta,
			Value:        value,
		}
	}

	return aggregation.NewExpressionAggregation(source, fields), nil
}

// ParseAggregationFieldValue parses an ApiAggregationFieldValue discriminated union into
// a domain AggregationValue expression tree node.
func ParseAggregationFieldValue(apiField ApiAggregationFieldValue) (aggregation.AggregationValue, error) {
	raw, err := apiField.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedField := raw.(type) {
	case ApiRefAggregationFieldValue:
		return aggregation.NewRefAggregationValue(typedField.Urn), nil

	case ApiConstantAggregationFieldValue:
		dataType := tsquery.DataType(typedField.DataType)
		if err := dataType.Validate(); err != nil {
			return nil, fmt.Errorf("invalid data type for constant aggregation field value: %w", err)
		}
		typedValue, err := dataType.ForceCastAndValidate(typedField.FieldValue)
		if err != nil {
			return nil, fmt.Errorf("failed to cast constant value to %s: %w", dataType, err)
		}
		return aggregation.NewConstantAggregationValue(dataType, typedValue), nil

	case ApiNumericExpressionAggregationFieldValue:
		op1, err := ParseAggregationFieldValue(typedField.Op1)
		if err != nil {
			return nil, fmt.Errorf("failed to parse op1 for numeric expression: %w", err)
		}
		op2, err := ParseAggregationFieldValue(typedField.Op2)
		if err != nil {
			return nil, fmt.Errorf("failed to parse op2 for numeric expression: %w", err)
		}
		if err := typedField.Op.Validate(); err != nil {
			return nil, fmt.Errorf("invalid operator for numeric expression: %w", err)
		}
		return aggregation.NewNumericExpressionAggregationValue(op1, typedField.Op, op2), nil

	case ApiUnaryNumericOperatorAggregationFieldValue:
		operand, err := ParseAggregationFieldValue(typedField.Operand)
		if err != nil {
			return nil, fmt.Errorf("failed to parse operand for unary operator: %w", err)
		}
		if err := typedField.Op.Validate(); err != nil {
			return nil, fmt.Errorf("invalid operator for unary numeric expression: %w", err)
		}
		return aggregation.NewUnaryNumericOperatorAggregationValue(operand, typedField.Op), nil

	case ApiCastAggregationFieldValue:
		source, err := ParseAggregationFieldValue(typedField.Source)
		if err != nil {
			return nil, fmt.Errorf("failed to parse source for cast: %w", err)
		}
		targetType := tsquery.DataType(typedField.TargetType)
		if err := targetType.Validate(); err != nil {
			return nil, fmt.Errorf("invalid target type for cast: %w", err)
		}
		return aggregation.NewCastAggregationValue(source, targetType), nil

	default:
		return nil, fmt.Errorf("unsupported aggregation field value type: %T", raw)
	}
}
