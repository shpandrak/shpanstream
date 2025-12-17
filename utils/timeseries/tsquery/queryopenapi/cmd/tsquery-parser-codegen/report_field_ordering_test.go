//go:build ignore

package queryopenapi

import (
	"testing"

	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetReferencedUrns_Ref(t *testing.T) {
	var fv ApiReportFieldValue
	err := fv.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "field:a"})
	require.NoError(t, err)

	urns := fv.GetReferencedUrns()
	assert.Equal(t, []string{"field:a"}, urns)
}

func TestGetReferencedUrns_Constant(t *testing.T) {
	var fv ApiReportFieldValue
	err := fv.FromApiConstantReportFieldValue(ApiConstantReportFieldValue{
		DataType:   tsquery.DataTypeInteger,
		FieldValue: 42,
		Required:   true,
	})
	require.NoError(t, err)

	urns := fv.GetReferencedUrns()
	assert.Nil(t, urns)
}

func TestGetReferencedUrns_NumericExpression(t *testing.T) {
	// Create ref1 -> "field:a"
	var ref1 ApiReportFieldValue
	err := ref1.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "field:a"})
	require.NoError(t, err)

	// Create ref2 -> "field:b"
	var ref2 ApiReportFieldValue
	err = ref2.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "field:b"})
	require.NoError(t, err)

	// Create numeric expression: ref1 + ref2
	var expr ApiReportFieldValue
	err = expr.FromApiNumericExpressionReportFieldValue(ApiNumericExpressionReportFieldValue{
		Op:  tsquery.BinaryNumericOperatorAdd,
		Op1: ref1,
		Op2: ref2,
	})
	require.NoError(t, err)

	urns := expr.GetReferencedUrns()
	assert.ElementsMatch(t, []string{"field:a", "field:b"}, urns)
}

func TestGetReferencedUrns_NestedSelector(t *testing.T) {
	// selector(ref:bool, ref:true, ref:false)
	var boolRef ApiReportFieldValue
	err := boolRef.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "field:bool"})
	require.NoError(t, err)

	var trueRef ApiReportFieldValue
	err = trueRef.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "field:true"})
	require.NoError(t, err)

	var falseRef ApiReportFieldValue
	err = falseRef.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "field:false"})
	require.NoError(t, err)

	var selector ApiReportFieldValue
	err = selector.FromApiSelectorReportFieldValue(ApiSelectorReportFieldValue{
		SelectorBooleanField: boolRef,
		TrueField:            trueRef,
		FalseField:           falseRef,
	})
	require.NoError(t, err)

	urns := selector.GetReferencedUrns()
	assert.ElementsMatch(t, []string{"field:bool", "field:true", "field:false"}, urns)
}

func TestGetReferencedUrns_Reduce(t *testing.T) {
	var fv ApiReportFieldValue
	err := fv.FromApiReduceReportFieldValue(ApiReduceReportFieldValue{
		FieldUrns:     []string{"field:a", "field:b", "field:c"},
		ReductionType: tsquery.ReductionTypeSum,
	})
	require.NoError(t, err)

	urns := fv.GetReferencedUrns()
	assert.ElementsMatch(t, []string{"field:a", "field:b", "field:c"}, urns)
}

func TestOrderReportFieldUrnsByDependency_NoDeps(t *testing.T) {
	// Three independent fields
	var const1 ApiReportFieldValue
	err := const1.FromApiConstantReportFieldValue(ApiConstantReportFieldValue{
		DataType: tsquery.DataTypeInteger, FieldValue: 1, Required: true,
	})
	require.NoError(t, err)

	var const2 ApiReportFieldValue
	err = const2.FromApiConstantReportFieldValue(ApiConstantReportFieldValue{
		DataType: tsquery.DataTypeInteger, FieldValue: 2, Required: true,
	})
	require.NoError(t, err)

	fields := []ReportFieldForOrdering{
		{Uri: "a", Value: const1},
		{Uri: "b", Value: const2},
	}

	ordered, err := OrderReportFieldUrnsByDependency(fields)
	require.NoError(t, err)
	assert.Len(t, ordered, 2)
	assert.Contains(t, ordered, "a")
	assert.Contains(t, ordered, "b")
}

func TestOrderReportFieldUrnsByDependency_SimpleChain(t *testing.T) {
	// a = constant
	// b = ref(a)
	// c = ref(b)
	var constA ApiReportFieldValue
	err := constA.FromApiConstantReportFieldValue(ApiConstantReportFieldValue{
		DataType: tsquery.DataTypeInteger, FieldValue: 1, Required: true,
	})
	require.NoError(t, err)

	var refA ApiReportFieldValue
	err = refA.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "a"})
	require.NoError(t, err)

	var refB ApiReportFieldValue
	err = refB.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "b"})
	require.NoError(t, err)

	fields := []ReportFieldForOrdering{
		{Uri: "c", Value: refB}, // depends on b
		{Uri: "a", Value: constA},
		{Uri: "b", Value: refA}, // depends on a
	}

	ordered, err := OrderReportFieldUrnsByDependency(fields)
	require.NoError(t, err)
	require.Len(t, ordered, 3)

	// a must come before b, b must come before c
	aIdx := indexOf(ordered, "a")
	bIdx := indexOf(ordered, "b")
	cIdx := indexOf(ordered, "c")

	assert.Less(t, aIdx, bIdx, "a should come before b")
	assert.Less(t, bIdx, cIdx, "b should come before c")
}

func TestOrderReportFieldUrnsByDependency_Diamond(t *testing.T) {
	// a = constant
	// b = ref(a)
	// c = ref(a)
	// d = ref(b) + ref(c)
	var constA ApiReportFieldValue
	err := constA.FromApiConstantReportFieldValue(ApiConstantReportFieldValue{
		DataType: tsquery.DataTypeInteger, FieldValue: 1, Required: true,
	})
	require.NoError(t, err)

	var refA ApiReportFieldValue
	err = refA.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "a"})
	require.NoError(t, err)

	var refB ApiReportFieldValue
	err = refB.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "b"})
	require.NoError(t, err)

	var refC ApiReportFieldValue
	err = refC.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "c"})
	require.NoError(t, err)

	var exprD ApiReportFieldValue
	err = exprD.FromApiNumericExpressionReportFieldValue(ApiNumericExpressionReportFieldValue{
		Op: tsquery.BinaryNumericOperatorAdd, Op1: refB, Op2: refC,
	})
	require.NoError(t, err)

	fields := []ReportFieldForOrdering{
		{Uri: "d", Value: exprD},
		{Uri: "b", Value: refA},
		{Uri: "c", Value: refA},
		{Uri: "a", Value: constA},
	}

	ordered, err := OrderReportFieldUrnsByDependency(fields)
	require.NoError(t, err)
	require.Len(t, ordered, 4)

	aIdx := indexOf(ordered, "a")
	bIdx := indexOf(ordered, "b")
	cIdx := indexOf(ordered, "c")
	dIdx := indexOf(ordered, "d")

	assert.Less(t, aIdx, bIdx, "a should come before b")
	assert.Less(t, aIdx, cIdx, "a should come before c")
	assert.Less(t, bIdx, dIdx, "b should come before d")
	assert.Less(t, cIdx, dIdx, "c should come before d")
}

func TestOrderReportFieldUrnsByDependency_CircularError(t *testing.T) {
	// a = ref(b)
	// b = ref(a)
	var refB ApiReportFieldValue
	err := refB.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "b"})
	require.NoError(t, err)

	var refA ApiReportFieldValue
	err = refA.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "a"})
	require.NoError(t, err)

	fields := []ReportFieldForOrdering{
		{Uri: "a", Value: refB},
		{Uri: "b", Value: refA},
	}

	_, err = OrderReportFieldUrnsByDependency(fields)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "circular dependency")
}

func TestOrderReportFieldUrnsByDependency_ExternalRefsIgnored(t *testing.T) {
	// a = constant
	// b = ref(a) + ref(external) - external is not in our list
	var constA ApiReportFieldValue
	err := constA.FromApiConstantReportFieldValue(ApiConstantReportFieldValue{
		DataType: tsquery.DataTypeInteger, FieldValue: 1, Required: true,
	})
	require.NoError(t, err)

	var refA ApiReportFieldValue
	err = refA.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "a"})
	require.NoError(t, err)

	var refExternal ApiReportFieldValue
	err = refExternal.FromApiRefReportFieldValue(ApiRefReportFieldValue{Urn: "external"})
	require.NoError(t, err)

	var exprB ApiReportFieldValue
	err = exprB.FromApiNumericExpressionReportFieldValue(ApiNumericExpressionReportFieldValue{
		Op: tsquery.BinaryNumericOperatorAdd, Op1: refA, Op2: refExternal,
	})
	require.NoError(t, err)

	fields := []ReportFieldForOrdering{
		{Uri: "b", Value: exprB},
		{Uri: "a", Value: constA},
	}

	ordered, err := OrderReportFieldUrnsByDependency(fields)
	require.NoError(t, err)
	require.Len(t, ordered, 2)

	aIdx := indexOf(ordered, "a")
	bIdx := indexOf(ordered, "b")
	assert.Less(t, aIdx, bIdx, "a should come before b (external ref ignored)")
}

func TestOrderReportFieldUrnsByDependency_Empty(t *testing.T) {
	ordered, err := OrderReportFieldUrnsByDependency(nil)
	require.NoError(t, err)
	assert.Nil(t, ordered)

	ordered, err = OrderReportFieldUrnsByDependency([]ReportFieldForOrdering{})
	require.NoError(t, err)
	assert.Nil(t, ordered)
}

func indexOf(slice []string, s string) int {
	for i, v := range slice {
		if v == s {
			return i
		}
	}
	return -1
}
