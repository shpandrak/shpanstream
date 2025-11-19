package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
)

func parseQueryField(queryField ApiQueryFieldValue) (report.Value, error) {
	queryFieldValue, err := queryField.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedField := queryFieldValue.(type) {
	case ApiConstantQueryFieldValue:
		return parseConstantQueryFieldValue(typedField)
	case ApiConditionQueryFieldValue:
		return parseConditionQueryFieldValue(typedField)
	case ApiLogicalExpressionQueryFieldValue:
		return parseLogicalExpressionQueryFieldValue(typedField)
	case ApiRefQueryFieldValue:
		return parseRefQueryFieldValue(typedField)
	case ApiSelectorQueryFieldValue:
		return parseSelectorQueryFieldValue(typedField)
	case ApiNvlQueryFieldValue:
		return parseNvlQueryFieldValue(typedField)
	case ApiCastQueryFieldValue:
		return parseCastQueryFieldValue(typedField)
	case ApiNumericExpressionQueryFieldValue:
		return parseNumericExpressionQueryFieldValue(typedField)
	case ApiUnaryNumericOperatorQueryFieldValue:
		return parseUnaryNumericOperatorQueryFieldValue(typedField)
	case ApiReduceQueryFieldValue:
		return parseReduceQueryFieldValue(typedField)
	default:
		return nil, fmt.Errorf("unsupported query field type %T", typedField)
	}

}

func parseConstantQueryFieldValue(cqf ApiConstantQueryFieldValue) (report.Value, error) {
	valueMeta := tsquery.ValueMeta{
		DataType: cqf.DataType,
		Required: cqf.Required,
		Unit:     cqf.Unit,
	}
	return report.NewConstantFieldValue(valueMeta, cqf.FieldValue), nil
}

func parseConditionQueryFieldValue(cqf ApiConditionQueryFieldValue) (report.Value, error) {
	operand1, err := parseQueryField(cqf.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for condition query field: %w", err)
	}
	operand2, err := parseQueryField(cqf.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for condition query field: %w", err)
	}
	return report.NewConditionFieldValue(cqf.OperatorType, operand1, operand2), nil
}

func parseLogicalExpressionQueryFieldValue(lef ApiLogicalExpressionQueryFieldValue) (report.Value, error) {
	operand1, err := parseQueryField(lef.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for logical expression query field: %w", err)
	}
	operand2, err := parseQueryField(lef.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for logical expression query field: %w", err)
	}
	return report.NewLogicalExpressionFieldValue(lef.LogicalOperatorType, operand1, operand2), nil
}

func parseRefQueryFieldValue(ref ApiRefQueryFieldValue) (report.Value, error) {
	return report.NewRefFieldValue(ref.Urn), nil
}

func parseSelectorQueryFieldValue(sqf ApiSelectorQueryFieldValue) (report.Value, error) {
	selectorField, err := parseQueryField(sqf.SelectorBooleanField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse selector boolean field: %w", err)
	}
	trueField, err := parseQueryField(sqf.TrueField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse true field: %w", err)
	}
	falseField, err := parseQueryField(sqf.FalseField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse false field: %w", err)
	}
	return report.NewSelectorFieldValue(selectorField, trueField, falseField), nil
}

func parseNvlQueryFieldValue(nvl ApiNvlQueryFieldValue) (report.Value, error) {
	source, err := parseQueryField(nvl.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for nvl: %w", err)
	}
	altField, err := parseQueryField(nvl.AltField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse alt field for nvl: %w", err)
	}
	return report.NewNvlFieldValue(source, altField), nil
}

func parseCastQueryFieldValue(cast ApiCastQueryFieldValue) (report.Value, error) {
	source, err := parseQueryField(cast.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for cast: %w", err)
	}
	return report.NewCastFieldValue(source, cast.TargetType), nil
}

func parseNumericExpressionQueryFieldValue(nef ApiNumericExpressionQueryFieldValue) (report.Value, error) {
	op1, err := parseQueryField(nef.Op1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op1 field for numeric expression: %w", err)
	}
	op2, err := parseQueryField(nef.Op2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op2 field for numeric expression: %w", err)
	}
	return report.NewNumericExpressionFieldValue(op1, nef.Op, op2), nil
}

func parseUnaryNumericOperatorQueryFieldValue(ufv ApiUnaryNumericOperatorQueryFieldValue) (report.Value, error) {
	operand, err := parseQueryField(ufv.Operand)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand field for unary numeric operator: %w", err)
	}
	return report.NewUnaryNumericOperatorFieldValue(operand, ufv.Op), nil
}

func parseReduceQueryFieldValue(reduce ApiReduceQueryFieldValue) (report.Value, error) {
	if reduce.FieldUrnsToReduce == nil || len(reduce.FieldUrnsToReduce) == 0 {
		// Reduce all fields
		return report.NewReduceAllFieldValues(reduce.ReductionType), nil
	}
	// Reduce specific fields
	return report.NewReduceFieldValues(reduce.FieldUrnsToReduce, reduce.ReductionType), nil
}
