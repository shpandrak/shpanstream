package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
)

func parseQueryField(queryField ApiQueryField) (field.Field, error) {
	queryFieldValue, err := queryField.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedField := queryFieldValue.(type) {
	case ApiConstantQueryField:
		return parseConstantQueryField(typedField)
	case ApiConditionQueryField:
		return parseConditionQueryField(typedField)
	case ApiLogicalExpressionQueryField:
		return parseLogicalExpressionQueryField(typedField)
	case ApiRefQueryField:
		return parseRefQueryField(typedField)
	case ApiSelectorQueryField:
		return parseSelectorQueryField(typedField)
	case ApiNvlQueryField:
		return parseNvlQueryField(typedField)
	case ApiCastQueryField:
		return parseCastQueryField(typedField)
	case ApiNumericExpressionQueryField:
		return parseNumericExpressionQueryField(typedField)
	case ApiUnaryNumericOperatorQueryField:
		return parseUnaryNumericOperatorQueryField(typedField)
	case ApiReduceQueryField:
		return parseReduceQueryField(typedField)
	default:
		return nil, fmt.Errorf("unsupported query field type %T", typedField)
	}

}

func parseConstantQueryField(cqf ApiConstantQueryField) (field.Field, error) {
	fm, err := parseFieldMeta(cqf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse field meta for constant query field: %w", err)
	}
	return field.NewConstantField(*fm, cqf.FieldValue), nil

}

func parseConditionQueryField(cqf ApiConditionQueryField) (field.Field, error) {
	operand1, err := parseQueryField(cqf.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for condition query field: %w", err)
	}
	operand2, err := parseQueryField(cqf.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for condition query field: %w", err)
	}
	return field.NewConditionField(cqf.Urn, cqf.OperatorType, operand1, operand2), nil
}

func parseLogicalExpressionQueryField(lef ApiLogicalExpressionQueryField) (field.Field, error) {
	operand1, err := parseQueryField(lef.Operand1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand1 for logical expression query field: %w", err)
	}
	operand2, err := parseQueryField(lef.Operand2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand2 for logical expression query field: %w", err)
	}
	return field.NewLogicalExpressionField(lef.Urn, lef.LogicalOperatorType, operand1, operand2), nil
}

func parseRefQueryField(ref ApiRefQueryField) (field.Field, error) {
	return field.NewRefField(ref.Urn), nil
}

func parseSelectorQueryField(sqf ApiSelectorQueryField) (field.Field, error) {
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
	return field.NewSelectorField(sqf.TargetFieldUrn, selectorField, trueField, falseField), nil
}

func parseNvlQueryField(nvl ApiNvlQueryField) (field.Field, error) {
	source, err := parseQueryField(nvl.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for nvl: %w", err)
	}
	altField, err := parseQueryField(nvl.AltField)
	if err != nil {
		return nil, fmt.Errorf("failed to parse alt field for nvl: %w", err)
	}
	return field.NewNvlField(nvl.FieldUrn, source, altField), nil
}

func parseCastQueryField(cast ApiCastQueryField) (field.Field, error) {
	source, err := parseQueryField(cast.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to parse source field for cast: %w", err)
	}
	return field.NewCastField(cast.FieldUrn, source, cast.TargetType), nil
}

func parseNumericExpressionQueryField(nef ApiNumericExpressionQueryField) (field.Field, error) {
	op1, err := parseQueryField(nef.Op1)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op1 field for numeric expression: %w", err)
	}
	op2, err := parseQueryField(nef.Op2)
	if err != nil {
		return nil, fmt.Errorf("failed to parse op2 field for numeric expression: %w", err)
	}
	return field.NewNumericExpressionField(nef.FieldUrn, op1, nef.Op, op2), nil
}

func parseUnaryNumericOperatorQueryField(unof ApiUnaryNumericOperatorQueryField) (field.Field, error) {
	operand, err := parseQueryField(unof.Operand)
	if err != nil {
		return nil, fmt.Errorf("failed to parse operand field for unary numeric operator: %w", err)
	}
	return field.NewUnaryNumericOperatorField(unof.FieldUrn, operand, unof.Op), nil
}

func parseReduceQueryField(reduce ApiReduceQueryField) (field.Field, error) {
	if reduce.FieldUrnsToReduce == nil || len(reduce.FieldUrnsToReduce) == 0 {
		// Reduce all fields
		return field.NewReduceAllFields(reduce.ResultUrn, reduce.ReductionType, reduce.ResultCustomMeta), nil
	}
	// Reduce specific fields
	return field.NewReduceFields(reduce.ResultUrn, reduce.FieldUrnsToReduce, reduce.ReductionType, reduce.ResultCustomMeta), nil
}

func parseFieldMeta(cqf ApiConstantQueryField) (*tsquery.FieldMeta, error) {
	return tsquery.NewFieldMetaWithCustomData(
		cqf.FieldMeta.Uri,
		cqf.FieldMeta.DataType,
		cqf.FieldMeta.Required,
		cqf.FieldMeta.Unit,
		cqf.FieldMeta.CustomMetadata,
	)
}
