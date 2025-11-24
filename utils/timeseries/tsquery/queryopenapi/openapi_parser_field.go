package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
)

func parseQueryField(pCtx *ParsingContext, queryField ApiQueryFieldValue) (datasource.Value, error) {
	queryFieldValue, err := queryField.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedField := queryFieldValue.(type) {
	case ApiConstantQueryFieldValue:
		return parseConstantQueryFieldValue(typedField)
	case ApiConditionQueryFieldValue:
		return parseConditionQueryFieldValue(pCtx, typedField)
	case ApiLogicalExpressionQueryFieldValue:
		return parseLogicalExpressionQueryFieldValue(pCtx, typedField)
	case ApiRefQueryFieldValue:
		return parseRefQueryFieldValue(typedField)
	case ApiSelectorQueryFieldValue:
		return parseSelectorQueryFieldValue(pCtx, typedField)
	case ApiNvlQueryFieldValue:
		return parseNvlQueryFieldValue(pCtx, typedField)
	case ApiCastQueryFieldValue:
		return parseCastQueryFieldValue(pCtx, typedField)
	case ApiNumericExpressionQueryFieldValue:
		return parseNumericExpressionQueryFieldValue(pCtx, typedField)
	case ApiUnaryNumericOperatorQueryFieldValue:
		return parseUnaryNumericOperatorQueryFieldValue(pCtx, typedField)
	default:
		return wrapAndReturn(pCtx.ParseFieldValue(pCtx, queryField))("failed parsing query field with plugin parser")

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

func parseConditionQueryFieldValue(
	pCtx *ParsingContext,
	cqf ApiConditionQueryFieldValue,
) (datasource.Value, error) {
	operand1, err := parseQueryField(pCtx, cqf.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for condition query field: %w", err)
	}
	operand2, err := parseQueryField(pCtx, cqf.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for condition query field: %w", err)
	}
	return datasource.NewConditionFieldValue(cqf.OperatorType, operand1, operand2), nil
}

func parseLogicalExpressionQueryFieldValue(
	pCtx *ParsingContext,
	lef ApiLogicalExpressionQueryFieldValue,
) (datasource.Value, error) {
	operand1, err := parseQueryField(pCtx, lef.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for logical expression query field: %w", err)
	}
	operand2, err := parseQueryField(pCtx, lef.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for logical expression query field: %w", err)
	}
	return datasource.NewLogicalExpressionFieldValue(lef.LogicalOperatorType, operand1, operand2), nil
}

func parseRefQueryFieldValue(_ ApiRefQueryFieldValue) (datasource.Value, error) {
	return datasource.NewRefFieldValue(), nil
}

func parseSelectorQueryFieldValue(
	pCtx *ParsingContext,
	sqf ApiSelectorQueryFieldValue,
) (datasource.Value, error) {
	selectorField, err := parseQueryField(pCtx, sqf.SelectorBooleanField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse selector boolean field: %w", err)
	}
	trueField, err := parseQueryField(pCtx, sqf.TrueField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse true field: %w", err)
	}
	falseField, err := parseQueryField(pCtx, sqf.FalseField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse false field: %w", err)
	}
	return datasource.NewSelectorFieldValue(selectorField, trueField, falseField), nil
}

func parseNvlQueryFieldValue(pCtx *ParsingContext, nvl ApiNvlQueryFieldValue) (datasource.Value, error) {
	source, err := parseQueryField(pCtx, nvl.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for nvl: %w", err)
	}
	altField, err := parseQueryField(pCtx, nvl.AltField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse alt field for nvl: %w", err)
	}
	return datasource.NewNvlFieldValue(source, altField), nil
}

func parseCastQueryFieldValue(pCtx *ParsingContext, cast ApiCastQueryFieldValue) (datasource.Value, error) {
	source, err := parseQueryField(pCtx, cast.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for cast: %w", err)
	}
	return datasource.NewCastFieldValue(source, cast.TargetType), nil
}

func parseNumericExpressionQueryFieldValue(
	pCtx *ParsingContext,
	nef ApiNumericExpressionQueryFieldValue,
) (datasource.Value, error) {
	op1, err := parseQueryField(pCtx, nef.Op1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op1 field for numeric expression: %w", err)
	}
	op2, err := parseQueryField(pCtx, nef.Op2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op2 field for numeric expression: %w", err)
	}
	return datasource.NewNumericExpressionFieldValue(op1, nef.Op, op2), nil
}

func parseUnaryNumericOperatorQueryFieldValue(
	pCtx *ParsingContext,
	ufv ApiUnaryNumericOperatorQueryFieldValue,
) (datasource.Value, error) {
	operand, err := parseQueryField(pCtx, ufv.Operand)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand field for unary numeric operator: %w", err)
	}
	return datasource.NewUnaryNumericOperatorFieldValue(operand, ufv.Op), nil
}
