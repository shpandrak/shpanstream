package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
)

// ParseReportField parses an ApiReportFieldValue into a report.Value
func ParseReportField(pCtx *ParsingContext, reportField ApiReportFieldValue) (report.Value, error) {
	reportFieldValue, err := reportField.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedField := reportFieldValue.(type) {
	case ApiConstantReportFieldValue:
		return parseConstantReportFieldValue(typedField)
	case ApiConditionReportFieldValue:
		return parseConditionReportFieldValue(pCtx, typedField)
	case ApiLogicalExpressionReportFieldValue:
		return parseLogicalExpressionReportFieldValue(pCtx, typedField)
	case ApiRefReportFieldValue:
		return parseRefReportFieldValue(typedField)
	case ApiSelectorReportFieldValue:
		return parseSelectorReportFieldValue(pCtx, typedField)
	case ApiNvlReportFieldValue:
		return parseNvlReportFieldValue(pCtx, typedField)
	case ApiCastReportFieldValue:
		return parseCastReportFieldValue(pCtx, typedField)
	case ApiNumericExpressionReportFieldValue:
		return parseNumericExpressionReportFieldValue(pCtx, typedField)
	case ApiUnaryNumericOperatorReportFieldValue:
		return parseUnaryNumericOperatorReportFieldValue(pCtx, typedField)
	case ApiReduceReportFieldValue:
		return parseReduceReportFieldValue(typedField)
	default:
		return wrapAndReturnReportField(pCtx.ParseReportFieldValue(pCtx, reportField))("failed parsing report field with plugin parser")
	}
}

func parseConstantReportFieldValue(crf ApiConstantReportFieldValue) (report.Value, error) {
	valueMeta := tsquery.ValueMeta{
		DataType: crf.DataType,
		Required: crf.Required,
		Unit:     crf.Unit,
	}
	return report.NewConstantFieldValue(valueMeta, crf.FieldValue), nil
}

func parseConditionReportFieldValue(
	pCtx *ParsingContext,
	crf ApiConditionReportFieldValue,
) (report.Value, error) {
	operand1, err := ParseReportField(pCtx, crf.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for condition report field: %w", err)
	}
	operand2, err := ParseReportField(pCtx, crf.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for condition report field: %w", err)
	}
	return report.NewConditionFieldValue(crf.OperatorType, operand1, operand2), nil
}

func parseLogicalExpressionReportFieldValue(
	pCtx *ParsingContext,
	lef ApiLogicalExpressionReportFieldValue,
) (report.Value, error) {
	operand1, err := ParseReportField(pCtx, lef.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for logical expression report field: %w", err)
	}
	operand2, err := ParseReportField(pCtx, lef.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for logical expression report field: %w", err)
	}
	return report.NewLogicalExpressionFieldValue(lef.LogicalOperatorType, operand1, operand2), nil
}

func parseRefReportFieldValue(rrf ApiRefReportFieldValue) (report.Value, error) {
	return report.NewRefFieldValue(rrf.Urn), nil
}

func parseSelectorReportFieldValue(
	pCtx *ParsingContext,
	srf ApiSelectorReportFieldValue,
) (report.Value, error) {
	selectorField, err := ParseReportField(pCtx, srf.SelectorBooleanField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse selector boolean field: %w", err)
	}
	trueField, err := ParseReportField(pCtx, srf.TrueField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse true field: %w", err)
	}
	falseField, err := ParseReportField(pCtx, srf.FalseField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse false field: %w", err)
	}
	return report.NewSelectorFieldValue(selectorField, trueField, falseField), nil
}

func parseNvlReportFieldValue(pCtx *ParsingContext, nvl ApiNvlReportFieldValue) (report.Value, error) {
	source, err := ParseReportField(pCtx, nvl.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for nvl: %w", err)
	}
	altField, err := ParseReportField(pCtx, nvl.AltField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse alt field for nvl: %w", err)
	}
	return report.NewNvlFieldValue(source, altField), nil
}

func parseCastReportFieldValue(pCtx *ParsingContext, cast ApiCastReportFieldValue) (report.Value, error) {
	source, err := ParseReportField(pCtx, cast.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for cast: %w", err)
	}
	return report.NewCastFieldValue(source, cast.TargetType), nil
}

func parseNumericExpressionReportFieldValue(
	pCtx *ParsingContext,
	nef ApiNumericExpressionReportFieldValue,
) (report.Value, error) {
	op1, err := ParseReportField(pCtx, nef.Op1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op1 field for numeric expression: %w", err)
	}
	op2, err := ParseReportField(pCtx, nef.Op2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op2 field for numeric expression: %w", err)
	}
	return report.NewNumericExpressionFieldValue(op1, nef.Op, op2), nil
}

func parseUnaryNumericOperatorReportFieldValue(
	pCtx *ParsingContext,
	ufv ApiUnaryNumericOperatorReportFieldValue,
) (report.Value, error) {
	operand, err := ParseReportField(pCtx, ufv.Operand)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand field for unary numeric operator: %w", err)
	}
	return report.NewUnaryNumericOperatorFieldValue(operand, ufv.Op), nil
}

func parseReduceReportFieldValue(rrf ApiReduceReportFieldValue) (report.Value, error) {
	if len(rrf.FieldUrns) == 0 {
		return report.NewReduceAllFieldValues(rrf.ReductionType), nil
	}
	return report.NewReduceFieldValues(rrf.FieldUrns, rrf.ReductionType), nil
}

// wrapAndReturnReportField is a helper function to wrap errors for report field values
func wrapAndReturnReportField(result report.Value, err error) func(wrapMsg string) (report.Value, error) {
	return func(wrapMsg string) (report.Value, error) {
		if err != nil {
			return nil, fmt.Errorf("%s: %w", wrapMsg, err)
		}
		return result, nil
	}
}
