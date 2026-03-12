package aggregation

import (
	"context"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func resolvedFields(fields ...struct {
	urn      string
	value    any
	dataType tsquery.DataType
}) map[string]ResolvedField {
	m := make(map[string]ResolvedField, len(fields))
	for _, f := range fields {
		m[f.urn] = ResolvedField{Value: f.value, DataType: f.dataType}
	}
	return m
}

func rf(urn string, value any, dataType tsquery.DataType) struct {
	urn      string
	value    any
	dataType tsquery.DataType
} {
	return struct {
		urn      string
		value    any
		dataType tsquery.DataType
	}{urn, value, dataType}
}

// --- RefAggregationValue tests ---

func TestRefAggregationValue(t *testing.T) {
	resolved := resolvedFields(
		rf("total", int64(42), tsquery.DataTypeInteger),
	)

	ref := NewRefAggregationValue("total")
	val, dt, err := ref.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, int64(42), val)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

func TestRefAggregationValue_MissingUrn(t *testing.T) {
	resolved := resolvedFields()

	ref := NewRefAggregationValue("nonexistent")
	_, _, err := ref.Evaluate(resolved)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

// --- ConstantAggregationValue tests ---

func TestConstantAggregationValue_Integer(t *testing.T) {
	c := NewConstantAggregationValue(tsquery.DataTypeInteger, int64(100))
	val, dt, err := c.Evaluate(nil)
	require.NoError(t, err)
	require.Equal(t, int64(100), val)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

func TestConstantAggregationValue_Decimal(t *testing.T) {
	c := NewConstantAggregationValue(tsquery.DataTypeDecimal, 3.14)
	val, dt, err := c.Evaluate(nil)
	require.NoError(t, err)
	require.Equal(t, 3.14, val)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

// --- NumericExpressionAggregationValue tests ---

func TestNumericExpression_Add_Integer(t *testing.T) {
	resolved := resolvedFields(
		rf("a", int64(10), tsquery.DataTypeInteger),
		rf("b", int64(20), tsquery.DataTypeInteger),
	)

	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)

	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, int64(30), val)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

func TestNumericExpression_Mul_Decimal(t *testing.T) {
	resolved := resolvedFields(
		rf("a", 5.0, tsquery.DataTypeDecimal),
	)

	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorMul,
		NewConstantAggregationValue(tsquery.DataTypeDecimal, 100.0),
	)

	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, 500.0, val)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestNumericExpression_Div_Decimal(t *testing.T) {
	resolved := resolvedFields(
		rf("a", 100.0, tsquery.DataTypeDecimal),
		rf("b", 4.0, tsquery.DataTypeDecimal),
	)

	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorDiv,
		NewRefAggregationValue("b"),
	)

	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, 25.0, val)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestNumericExpression_Power(t *testing.T) {
	resolved := resolvedFields(
		rf("a", int64(2), tsquery.DataTypeInteger),
	)

	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorPow,
		NewConstantAggregationValue(tsquery.DataTypeInteger, int64(3)),
	)

	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, int64(8), val)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

func TestNumericExpression_NilPropagation(t *testing.T) {
	resolved := resolvedFields(
		rf("a", nil, tsquery.DataTypeInteger),
		rf("b", int64(5), tsquery.DataTypeInteger),
	)

	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)

	val, _, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Nil(t, val)
}

func TestNumericExpression_IntDecimalPromotion(t *testing.T) {
	tests := []struct {
		name     string
		v1       any
		dt1      tsquery.DataType
		v2       any
		dt2      tsquery.DataType
		op       tsquery.BinaryNumericOperatorType
		expected float64
	}{
		{"Add", int64(10), tsquery.DataTypeInteger, 5.5, tsquery.DataTypeDecimal, tsquery.BinaryNumericOperatorAdd, 15.5},
		{"Sub", int64(10), tsquery.DataTypeInteger, 5.5, tsquery.DataTypeDecimal, tsquery.BinaryNumericOperatorSub, 4.5},
		{"Mul", int64(10), tsquery.DataTypeInteger, 2.5, tsquery.DataTypeDecimal, tsquery.BinaryNumericOperatorMul, 25.0},
		{"Div", int64(10), tsquery.DataTypeInteger, 4.0, tsquery.DataTypeDecimal, tsquery.BinaryNumericOperatorDiv, 2.5},
		{"Pow", int64(2), tsquery.DataTypeInteger, 3.0, tsquery.DataTypeDecimal, tsquery.BinaryNumericOperatorPow, 8.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved := resolvedFields(
				rf("a", tt.v1, tt.dt1),
				rf("b", tt.v2, tt.dt2),
			)
			expr := NewNumericExpressionAggregationValue(
				NewRefAggregationValue("a"),
				tt.op,
				NewRefAggregationValue("b"),
			)
			val, dt, err := expr.Evaluate(resolved)
			require.NoError(t, err)
			require.Equal(t, tt.expected, val)
			require.Equal(t, tsquery.DataTypeDecimal, dt)
		})
	}
}

func TestNumericExpression_DecimalIntPromotion(t *testing.T) {
	tests := []struct {
		name     string
		v1       any
		dt1      tsquery.DataType
		v2       any
		dt2      tsquery.DataType
		op       tsquery.BinaryNumericOperatorType
		expected float64
	}{
		{"Add", 5.5, tsquery.DataTypeDecimal, int64(10), tsquery.DataTypeInteger, tsquery.BinaryNumericOperatorAdd, 15.5},
		{"Sub", 10.5, tsquery.DataTypeDecimal, int64(5), tsquery.DataTypeInteger, tsquery.BinaryNumericOperatorSub, 5.5},
		{"Mul", 2.5, tsquery.DataTypeDecimal, int64(10), tsquery.DataTypeInteger, tsquery.BinaryNumericOperatorMul, 25.0},
		{"Div", 10.0, tsquery.DataTypeDecimal, int64(4), tsquery.DataTypeInteger, tsquery.BinaryNumericOperatorDiv, 2.5},
		{"Pow", 2.0, tsquery.DataTypeDecimal, int64(3), tsquery.DataTypeInteger, tsquery.BinaryNumericOperatorPow, 8.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved := resolvedFields(
				rf("a", tt.v1, tt.dt1),
				rf("b", tt.v2, tt.dt2),
			)
			expr := NewNumericExpressionAggregationValue(
				NewRefAggregationValue("a"),
				tt.op,
				NewRefAggregationValue("b"),
			)
			val, dt, err := expr.Evaluate(resolved)
			require.NoError(t, err)
			require.Equal(t, tt.expected, val)
			require.Equal(t, tsquery.DataTypeDecimal, dt)
		})
	}
}

func TestNumericExpression_IntDecimalPromotion_OutputType(t *testing.T) {
	resolved := resolvedFields(
		rf("a", int64(10), tsquery.DataTypeInteger),
		rf("b", 5.0, tsquery.DataTypeDecimal),
	)
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)
	_, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestNumericExpression_IntDecimalModulo_Error(t *testing.T) {
	resolved := resolvedFields(
		rf("a", int64(10), tsquery.DataTypeInteger),
		rf("b", 3.0, tsquery.DataTypeDecimal),
	)
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorMod,
		NewRefAggregationValue("b"),
	)
	_, _, err := expr.Evaluate(resolved)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

func TestNumericExpression_IntDecimalPromotion_NilPropagation(t *testing.T) {
	resolved := resolvedFields(
		rf("a", nil, tsquery.DataTypeInteger),
		rf("b", 5.0, tsquery.DataTypeDecimal),
	)
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)
	val, _, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Nil(t, val)
}

// --- UnaryNumericOperatorAggregationValue tests ---

func TestUnaryOperator_Abs(t *testing.T) {
	resolved := resolvedFields(
		rf("a", int64(-42), tsquery.DataTypeInteger),
	)

	expr := NewUnaryNumericOperatorAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.UnaryNumericOperatorAbs,
	)

	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, int64(42), val)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

func TestUnaryOperator_Negate(t *testing.T) {
	resolved := resolvedFields(
		rf("a", 3.14, tsquery.DataTypeDecimal),
	)

	expr := NewUnaryNumericOperatorAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.UnaryNumericOperatorNegate,
	)

	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, -3.14, val)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestUnaryOperator_NilPropagation(t *testing.T) {
	resolved := resolvedFields(
		rf("a", nil, tsquery.DataTypeDecimal),
	)

	expr := NewUnaryNumericOperatorAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.UnaryNumericOperatorAbs,
	)

	val, _, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Nil(t, val)
}

// --- CastAggregationValue tests ---

func TestCast_IntToDecimal(t *testing.T) {
	resolved := resolvedFields(
		rf("a", int64(42), tsquery.DataTypeInteger),
	)

	cast := NewCastAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.DataTypeDecimal,
	)

	val, dt, err := cast.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, float64(42), val)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestCast_DecimalToInt(t *testing.T) {
	resolved := resolvedFields(
		rf("a", 42.9, tsquery.DataTypeDecimal),
	)

	cast := NewCastAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.DataTypeInteger,
	)

	val, dt, err := cast.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, int64(42), val)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

// --- Compound expression test ---

func TestCompoundExpression(t *testing.T) {
	// (ref(a) * constant(100)) / cast(ref(b), decimal)
	resolved := resolvedFields(
		rf("a", 5.0, tsquery.DataTypeDecimal),
		rf("b", int64(2), tsquery.DataTypeInteger),
	)

	expr := NewNumericExpressionAggregationValue(
		NewNumericExpressionAggregationValue(
			NewRefAggregationValue("a"),
			tsquery.BinaryNumericOperatorMul,
			NewConstantAggregationValue(tsquery.DataTypeDecimal, 100.0),
		),
		tsquery.BinaryNumericOperatorDiv,
		NewCastAggregationValue(
			NewRefAggregationValue("b"),
			tsquery.DataTypeDecimal,
		),
	)

	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Equal(t, 250.0, val)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

// --- ExpressionAggregation.Execute tests ---

// stubAggregator returns a fixed result for testing.
type stubAggregator struct {
	result Result
}

func (s *stubAggregator) Execute(_ context.Context, _, _ time.Time) (Result, error) {
	return s.result, nil
}

func newStubAggregator(fieldsMeta []tsquery.FieldMeta, values []AggregatedValue) *stubAggregator {
	return &stubAggregator{
		result: NewResult(fieldsMeta, lazy.NewLazy(func(_ context.Context) ([]AggregatedValue, error) {
			return values, nil
		})),
	}
}

func TestExpressionAggregation_Execute(t *testing.T) {
	ctx := context.Background()

	sumMeta, _ := tsquery.NewFieldMetaWithCustomData("total", tsquery.DataTypeInteger, true, "", nil)
	countMeta, _ := tsquery.NewFieldMetaWithCustomData("cnt", tsquery.DataTypeInteger, true, "", nil)

	source := newStubAggregator(
		[]tsquery.FieldMeta{*sumMeta, *countMeta},
		[]AggregatedValue{
			{Value: int64(100)},
			{Value: int64(4)},
		},
	)

	// Compute: cast(total, decimal) / cast(cnt, decimal)
	exprAgg := NewExpressionAggregation(source, []ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "avg"},
			Value: NewNumericExpressionAggregationValue(
				NewCastAggregationValue(NewRefAggregationValue("total"), tsquery.DataTypeDecimal),
				tsquery.BinaryNumericOperatorDiv,
				NewCastAggregationValue(NewRefAggregationValue("cnt"), tsquery.DataTypeDecimal),
			),
		},
	})

	result, err := exprAgg.Execute(ctx, time.Time{}, time.Now())
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1)
	require.Equal(t, "avg", metas[0].Urn())

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, 25.0, fields[0].Value)
}

func TestExpressionAggregation_PassthroughRef(t *testing.T) {
	ctx := context.Background()

	totalMeta, _ := tsquery.NewFieldMetaWithCustomData("total", tsquery.DataTypeInteger, true, "kWh", nil)

	source := newStubAggregator(
		[]tsquery.FieldMeta{*totalMeta},
		[]AggregatedValue{{Value: int64(42)}},
	)

	// Pass through source field via ref
	exprAgg := NewExpressionAggregation(source, []ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "total"},
			Value:        NewRefAggregationValue("total"),
		},
	})

	result, err := exprAgg.Execute(ctx, time.Time{}, time.Now())
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Equal(t, int64(42), fields[0].Value)
}

// --- ResolveType tests ---

func TestResolveType_Ref(t *testing.T) {
	sourceTypes := map[string]tsquery.DataType{
		"total": tsquery.DataTypeInteger,
	}
	ref := NewRefAggregationValue("total")
	dt, err := ref.ResolveType(sourceTypes)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

func TestResolveType_Ref_Missing(t *testing.T) {
	ref := NewRefAggregationValue("nonexistent")
	_, err := ref.ResolveType(map[string]tsquery.DataType{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
}

func TestResolveType_Constant(t *testing.T) {
	c := NewConstantAggregationValue(tsquery.DataTypeDecimal, 3.14)
	dt, err := c.ResolveType(nil)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestResolveType_NumericExpression_SameTypes(t *testing.T) {
	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeInteger,
		"b": tsquery.DataTypeInteger,
	}
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)
	dt, err := expr.ResolveType(sourceTypes)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeInteger, dt)
}

func TestResolveType_IntDecimalPromotion(t *testing.T) {
	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeInteger,
		"b": tsquery.DataTypeDecimal,
	}
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)
	dt, err := expr.ResolveType(sourceTypes)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestResolveType_IntDecimalModulo_Error(t *testing.T) {
	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeInteger,
		"b": tsquery.DataTypeDecimal,
	}
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorMod,
		NewRefAggregationValue("b"),
	)
	_, err := expr.ResolveType(sourceTypes)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

func TestResolveType_UnaryOperator(t *testing.T) {
	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeDecimal,
	}
	expr := NewUnaryNumericOperatorAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.UnaryNumericOperatorAbs,
	)
	dt, err := expr.ResolveType(sourceTypes)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestResolveType_Cast(t *testing.T) {
	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeInteger,
	}
	cast := NewCastAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.DataTypeDecimal,
	)
	dt, err := cast.ResolveType(sourceTypes)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

func TestResolveType_Compound(t *testing.T) {
	// (ref(a) * constant(decimal)) / cast(ref(b), decimal) => decimal
	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeDecimal,
		"b": tsquery.DataTypeInteger,
	}
	expr := NewNumericExpressionAggregationValue(
		NewNumericExpressionAggregationValue(
			NewRefAggregationValue("a"),
			tsquery.BinaryNumericOperatorMul,
			NewConstantAggregationValue(tsquery.DataTypeDecimal, 100.0),
		),
		tsquery.BinaryNumericOperatorDiv,
		NewCastAggregationValue(
			NewRefAggregationValue("b"),
			tsquery.DataTypeDecimal,
		),
	)
	dt, err := expr.ResolveType(sourceTypes)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}

// --- ExpressionAggregation metadata type tests ---

func TestExpressionAggregation_MetadataType_Integer(t *testing.T) {
	ctx := context.Background()

	totalMeta, _ := tsquery.NewFieldMetaWithCustomData("total", tsquery.DataTypeInteger, true, "", nil)

	source := newStubAggregator(
		[]tsquery.FieldMeta{*totalMeta},
		[]AggregatedValue{{Value: int64(42)}},
	)

	// ref(total) * constant(integer) => should report integer metadata, not decimal
	exprAgg := NewExpressionAggregation(source, []ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "result"},
			Value: NewNumericExpressionAggregationValue(
				NewRefAggregationValue("total"),
				tsquery.BinaryNumericOperatorMul,
				NewConstantAggregationValue(tsquery.DataTypeInteger, int64(2)),
			),
		},
	})

	result, err := exprAgg.Execute(ctx, time.Time{}, time.Now())
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1)
	require.Equal(t, tsquery.DataTypeInteger, metas[0].DataType(), "metadata should report integer, not decimal")

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(84), fields[0].Value)
}

func TestExpressionAggregation_MetadataType_Cast(t *testing.T) {
	ctx := context.Background()

	totalMeta, _ := tsquery.NewFieldMetaWithCustomData("total", tsquery.DataTypeInteger, true, "", nil)

	source := newStubAggregator(
		[]tsquery.FieldMeta{*totalMeta},
		[]AggregatedValue{{Value: int64(42)}},
	)

	// cast(ref(total), decimal) => metadata should report decimal
	exprAgg := NewExpressionAggregation(source, []ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "result"},
			Value: NewCastAggregationValue(
				NewRefAggregationValue("total"),
				tsquery.DataTypeDecimal,
			),
		},
	})

	result, err := exprAgg.Execute(ctx, time.Time{}, time.Now())
	require.NoError(t, err)

	metas := result.FieldsMeta()
	require.Len(t, metas, 1)
	require.Equal(t, tsquery.DataTypeDecimal, metas[0].DataType(), "metadata should report decimal for cast target type")

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Equal(t, float64(42), fields[0].Value)
}

func TestExpressionAggregation_EmptySource(t *testing.T) {
	ctx := context.Background()

	totalMeta, _ := tsquery.NewFieldMetaWithCustomData("total", tsquery.DataTypeInteger, true, "", nil)

	source := newStubAggregator(
		[]tsquery.FieldMeta{*totalMeta},
		[]AggregatedValue{{Value: nil}}, // nil value from empty stream
	)

	// ref(total) * constant(100) — should propagate nil
	exprAgg := NewExpressionAggregation(source, []ExpressionAggregationFieldDef{
		{
			AddFieldMeta: tsquery.AddFieldMeta{Urn: "result"},
			Value: NewNumericExpressionAggregationValue(
				NewRefAggregationValue("total"),
				tsquery.BinaryNumericOperatorMul,
				NewConstantAggregationValue(tsquery.DataTypeInteger, int64(100)),
			),
		},
	})

	result, err := exprAgg.Execute(ctx, time.Time{}, time.Now())
	require.NoError(t, err)

	fields, err := result.Fields().Get(ctx)
	require.NoError(t, err)
	require.Len(t, fields, 1)
	require.Nil(t, fields[0].Value, "nil source value should propagate through expression")
}

// --- Nil propagation type correctness (MISSING 1) ---

func TestNumericExpression_IntDecimalPromotion_NilType(t *testing.T) {
	// When nil propagates through a mixed int+decimal expression,
	// the returned DataType must be decimal (matching ResolveType).
	tests := []struct {
		name string
		v1   any
		dt1  tsquery.DataType
		v2   any
		dt2  tsquery.DataType
	}{
		{"int_nil+decimal", nil, tsquery.DataTypeInteger, 5.0, tsquery.DataTypeDecimal},
		{"int+decimal_nil", int64(5), tsquery.DataTypeInteger, nil, tsquery.DataTypeDecimal},
		{"decimal_nil+int", nil, tsquery.DataTypeDecimal, int64(5), tsquery.DataTypeInteger},
		{"decimal+int_nil", 5.0, tsquery.DataTypeDecimal, nil, tsquery.DataTypeInteger},
		{"both_nil_int_decimal", nil, tsquery.DataTypeInteger, nil, tsquery.DataTypeDecimal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resolved := resolvedFields(
				rf("a", tt.v1, tt.dt1),
				rf("b", tt.v2, tt.dt2),
			)
			expr := NewNumericExpressionAggregationValue(
				NewRefAggregationValue("a"),
				tsquery.BinaryNumericOperatorAdd,
				NewRefAggregationValue("b"),
			)
			val, dt, err := expr.Evaluate(resolved)
			require.NoError(t, err)
			require.Nil(t, val)
			require.Equal(t, tsquery.DataTypeDecimal, dt, "nil propagation must return promoted type")
		})
	}
}

// --- Modulo error tests (MISSING 2 & 3) ---

func TestNumericExpression_DecimalIntModulo_Error(t *testing.T) {
	resolved := resolvedFields(
		rf("a", 10.0, tsquery.DataTypeDecimal),
		rf("b", int64(3), tsquery.DataTypeInteger),
	)
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorMod,
		NewRefAggregationValue("b"),
	)
	_, _, err := expr.Evaluate(resolved)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

func TestNumericExpression_DecimalDecimalModulo_Error(t *testing.T) {
	resolved := resolvedFields(
		rf("a", 10.0, tsquery.DataTypeDecimal),
		rf("b", 3.0, tsquery.DataTypeDecimal),
	)
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorMod,
		NewRefAggregationValue("b"),
	)
	_, _, err := expr.Evaluate(resolved)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mod operator is only supported for integer fields")
}

// --- Evaluate/ResolveType consistency (MISSING 4) ---

func TestNumericExpression_EvaluateResolveTypeConsistency(t *testing.T) {
	// For each operator with mixed int+decimal operands, Evaluate and ResolveType
	// must agree on the returned DataType.
	operators := []tsquery.BinaryNumericOperatorType{
		tsquery.BinaryNumericOperatorAdd,
		tsquery.BinaryNumericOperatorSub,
		tsquery.BinaryNumericOperatorMul,
		tsquery.BinaryNumericOperatorDiv,
		tsquery.BinaryNumericOperatorPow,
	}

	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeInteger,
		"b": tsquery.DataTypeDecimal,
	}
	resolved := resolvedFields(
		rf("a", int64(10), tsquery.DataTypeInteger),
		rf("b", 5.0, tsquery.DataTypeDecimal),
	)

	for _, op := range operators {
		t.Run(string(op), func(t *testing.T) {
			expr := NewNumericExpressionAggregationValue(
				NewRefAggregationValue("a"),
				op,
				NewRefAggregationValue("b"),
			)

			resolvedType, err := expr.ResolveType(sourceTypes)
			require.NoError(t, err)

			_, evalType, err := expr.Evaluate(resolved)
			require.NoError(t, err)

			require.Equal(t, resolvedType, evalType, "Evaluate and ResolveType must return the same DataType")
		})
	}
}

// --- Reviewer suggestions: same-type nil propagation type assertion ---

func TestNumericExpression_NilPropagation_ReturnsCorrectType(t *testing.T) {
	resolved := resolvedFields(
		rf("a", nil, tsquery.DataTypeInteger),
		rf("b", int64(5), tsquery.DataTypeInteger),
	)
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)
	val, dt, err := expr.Evaluate(resolved)
	require.NoError(t, err)
	require.Nil(t, val)
	require.Equal(t, tsquery.DataTypeInteger, dt, "same-type nil propagation should return integer")
}

// --- Reviewer suggestion: ResolveType decimal+int (reverse direction) ---

func TestResolveType_DecimalIntPromotion(t *testing.T) {
	sourceTypes := map[string]tsquery.DataType{
		"a": tsquery.DataTypeDecimal,
		"b": tsquery.DataTypeInteger,
	}
	expr := NewNumericExpressionAggregationValue(
		NewRefAggregationValue("a"),
		tsquery.BinaryNumericOperatorAdd,
		NewRefAggregationValue("b"),
	)
	dt, err := expr.ResolveType(sourceTypes)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeDecimal, dt)
}
