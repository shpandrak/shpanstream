//go:build ignore

package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
)

func parseQueryField(queryField ApiQueryFieldValue) (datasource.Value, error) {
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
	default:
		return nil, fmt.Errorf("unsupported query field type %T", typedField)
	}

}

func parseConstantQueryFieldValue(cqf ApiConstantQueryFieldValue) (datasource.Value, error) {
	valueMeta := tsquery.ValueMeta{
		DataType: cqf.DataType,
		Required: cqf.Required,
		Unit:     cqf.Unit,
	}
	return datasource.NewConstantFieldValue(valueMeta, cqf.FieldValue), nil
}

func parseConditionQueryFieldValue(cqf ApiConditionQueryFieldValue) (datasource.Value, error) {
	operand1, err := parseQueryField(cqf.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for condition query field: %w", err)
	}
	operand2, err := parseQueryField(cqf.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for condition query field: %w", err)
	}
	return datasource.NewConditionFieldValue(cqf.OperatorType, operand1, operand2), nil
}

func parseLogicalExpressionQueryFieldValue(lef ApiLogicalExpressionQueryFieldValue) (datasource.Value, error) {
	operand1, err := parseQueryField(lef.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for logical expression query field: %w", err)
	}
	operand2, err := parseQueryField(lef.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for logical expression query field: %w", err)
	}
	return datasource.NewLogicalExpressionFieldValue(lef.LogicalOperatorType, operand1, operand2), nil
}

func parseRefQueryFieldValue(_ ApiRefQueryFieldValue) (datasource.Value, error) {
	return datasource.NewRefFieldValue(), nil
}

func parseSelectorQueryFieldValue(sqf ApiSelectorQueryFieldValue) (datasource.Value, error) {
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
	return datasource.NewSelectorFieldValue(selectorField, trueField, falseField), nil
}

func parseNvlQueryFieldValue(nvl ApiNvlQueryFieldValue) (datasource.Value, error) {
	source, err := parseQueryField(nvl.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for nvl: %w", err)
	}
	altField, err := parseQueryField(nvl.AltField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse alt field for nvl: %w", err)
	}
	return datasource.NewNvlFieldValue(source, altField), nil
}

func parseCastQueryFieldValue(cast ApiCastQueryFieldValue) (datasource.Value, error) {
	source, err := parseQueryField(cast.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for cast: %w", err)
	}
	return datasource.NewCastFieldValue(source, cast.TargetType), nil
}

func parseNumericExpressionQueryFieldValue(nef ApiNumericExpressionQueryFieldValue) (datasource.Value, error) {
	op1, err := parseQueryField(nef.Op1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op1 field for numeric expression: %w", err)
	}
	op2, err := parseQueryField(nef.Op2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op2 field for numeric expression: %w", err)
	}
	return datasource.NewNumericExpressionFieldValue(op1, nef.Op, op2), nil
}

func parseUnaryNumericOperatorQueryFieldValue(ufv ApiUnaryNumericOperatorQueryFieldValue) (datasource.Value, error) {
	operand, err := parseQueryField(ufv.Operand)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand field for unary numeric operator: %w", err)
	}
	return datasource.NewUnaryNumericOperatorFieldValue(operand, ufv.Op), nil
}
