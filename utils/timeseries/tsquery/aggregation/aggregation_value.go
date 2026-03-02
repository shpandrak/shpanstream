package aggregation

import (
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

// ResolvedField holds a scalar value and its data type from a source aggregation result.
type ResolvedField struct {
	Value    any
	DataType tsquery.DataType
}

// AggregationValue is the interface for expression tree nodes that evaluate over
// resolved aggregation scalar results.
type AggregationValue interface {
	Evaluate(resolved map[string]ResolvedField) (any, tsquery.DataType, error)
	// ResolveType statically resolves the data type of this expression node
	// from the source aggregation metadata, without evaluating any values.
	ResolveType(sourceTypes map[string]tsquery.DataType) (tsquery.DataType, error)
}

// --- RefAggregationValue ---

type RefAggregationValue struct {
	urn string
}

func NewRefAggregationValue(urn string) *RefAggregationValue {
	return &RefAggregationValue{urn: urn}
}

func (r *RefAggregationValue) Evaluate(resolved map[string]ResolvedField) (any, tsquery.DataType, error) {
	field, ok := resolved[r.urn]
	if !ok {
		return nil, "", fmt.Errorf("aggregation field %q not found in source results", r.urn)
	}
	return field.Value, field.DataType, nil
}

func (r *RefAggregationValue) ResolveType(sourceTypes map[string]tsquery.DataType) (tsquery.DataType, error) {
	dt, ok := sourceTypes[r.urn]
	if !ok {
		return "", fmt.Errorf("aggregation field %q not found in source types", r.urn)
	}
	return dt, nil
}

// --- ConstantAggregationValue ---

type ConstantAggregationValue struct {
	dataType tsquery.DataType
	value    any
}

func NewConstantAggregationValue(dataType tsquery.DataType, value any) *ConstantAggregationValue {
	return &ConstantAggregationValue{dataType: dataType, value: value}
}

func (c *ConstantAggregationValue) Evaluate(_ map[string]ResolvedField) (any, tsquery.DataType, error) {
	return c.value, c.dataType, nil
}

func (c *ConstantAggregationValue) ResolveType(_ map[string]tsquery.DataType) (tsquery.DataType, error) {
	return c.dataType, nil
}

// --- NumericExpressionAggregationValue ---

type NumericExpressionAggregationValue struct {
	op1 AggregationValue
	op  tsquery.BinaryNumericOperatorType
	op2 AggregationValue
}

func NewNumericExpressionAggregationValue(op1 AggregationValue, op tsquery.BinaryNumericOperatorType, op2 AggregationValue) *NumericExpressionAggregationValue {
	return &NumericExpressionAggregationValue{op1: op1, op: op, op2: op2}
}

func (n *NumericExpressionAggregationValue) Evaluate(resolved map[string]ResolvedField) (any, tsquery.DataType, error) {
	v1, dt1, err := n.op1.Evaluate(resolved)
	if err != nil {
		return nil, "", fmt.Errorf("failed to evaluate op1: %w", err)
	}
	v2, dt2, err := n.op2.Evaluate(resolved)
	if err != nil {
		return nil, "", fmt.Errorf("failed to evaluate op2: %w", err)
	}

	// Nil propagation: if either operand is nil, result is nil
	if v1 == nil || v2 == nil {
		return nil, dt1, nil
	}

	if dt1 != dt2 {
		return nil, "", fmt.Errorf("type mismatch in numeric expression: %s vs %s", dt1, dt2)
	}

	funcImpl, err := n.op.GetFuncImpl(dt1)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get function for operator %s on type %s: %w", n.op, dt1, err)
	}

	return funcImpl(v1, v2), dt1, nil
}

func (n *NumericExpressionAggregationValue) ResolveType(sourceTypes map[string]tsquery.DataType) (tsquery.DataType, error) {
	dt1, err := n.op1.ResolveType(sourceTypes)
	if err != nil {
		return "", fmt.Errorf("failed to resolve type for op1: %w", err)
	}
	dt2, err := n.op2.ResolveType(sourceTypes)
	if err != nil {
		return "", fmt.Errorf("failed to resolve type for op2: %w", err)
	}
	if dt1 != dt2 {
		return "", fmt.Errorf("type mismatch in numeric expression: %s vs %s", dt1, dt2)
	}
	return dt1, nil
}

// --- UnaryNumericOperatorAggregationValue ---

type UnaryNumericOperatorAggregationValue struct {
	operand AggregationValue
	op      tsquery.UnaryNumericOperatorType
}

func NewUnaryNumericOperatorAggregationValue(operand AggregationValue, op tsquery.UnaryNumericOperatorType) *UnaryNumericOperatorAggregationValue {
	return &UnaryNumericOperatorAggregationValue{operand: operand, op: op}
}

func (u *UnaryNumericOperatorAggregationValue) Evaluate(resolved map[string]ResolvedField) (any, tsquery.DataType, error) {
	v, dt, err := u.operand.Evaluate(resolved)
	if err != nil {
		return nil, "", fmt.Errorf("failed to evaluate operand: %w", err)
	}

	// Nil propagation
	if v == nil {
		return nil, dt, nil
	}

	funcImpl, err := u.op.GetFuncImpl(dt)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get function for unary operator %s on type %s: %w", u.op, dt, err)
	}

	return funcImpl(v), dt, nil
}

func (u *UnaryNumericOperatorAggregationValue) ResolveType(sourceTypes map[string]tsquery.DataType) (tsquery.DataType, error) {
	return u.operand.ResolveType(sourceTypes)
}

// --- CastAggregationValue ---

type CastAggregationValue struct {
	source     AggregationValue
	targetType tsquery.DataType
}

func NewCastAggregationValue(source AggregationValue, targetType tsquery.DataType) *CastAggregationValue {
	return &CastAggregationValue{source: source, targetType: targetType}
}

func (c *CastAggregationValue) Evaluate(resolved map[string]ResolvedField) (any, tsquery.DataType, error) {
	v, dt, err := c.source.Evaluate(resolved)
	if err != nil {
		return nil, "", fmt.Errorf("failed to evaluate source for cast: %w", err)
	}

	// Nil propagation
	if v == nil {
		return nil, c.targetType, nil
	}

	castFunc, err := tsquery.GetCastFuncForDataType(dt, c.targetType)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get cast function from %s to %s: %w", dt, c.targetType, err)
	}

	result, err := castFunc(v)
	if err != nil {
		return nil, "", fmt.Errorf("cast from %s to %s failed: %w", dt, c.targetType, err)
	}

	return result, c.targetType, nil
}

func (c *CastAggregationValue) ResolveType(_ map[string]tsquery.DataType) (tsquery.DataType, error) {
	return c.targetType, nil
}
