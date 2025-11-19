package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Helper function to execute a logical expression field and get its value
func executeLogicalFieldValue(t *testing.T, f Value, ctx context.Context) (any, error) {
	_, valueSupplier, err := f.Execute(tsquery.FieldMeta{})
	if err != nil {
		return nil, err
	}
	dummyRow := timeseries.TsRecord[any]{Timestamp: time.Now(), Value: nil}
	return valueSupplier(ctx, dummyRow)
}

// Helper function to execute a logical expression field and get its metadata
func executeLogicalFieldValueMeta(t *testing.T, f Value) (tsquery.ValueMeta, error) {
	meta, _, err := f.Execute(tsquery.FieldMeta{})
	return meta, err
}

func TestLogicalExpressionField_AndOperator(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	tests := []struct {
		name     string
		val1     bool
		val2     bool
		expected bool
	}{
		{"True AND True", true, true, true},
		{"True AND False", true, false, false},
		{"False AND True", false, true, false},
		{"False AND False", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(boolMeta, tt.val1)
			field2 := NewConstantFieldValue(boolMeta, tt.val2)

			logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, field1, field2)
			require.NotNil(t, logicalField)

			result, err := executeLogicalFieldValue(t, logicalField, ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLogicalExpressionField_OrOperator(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	tests := []struct {
		name     string
		val1     bool
		val2     bool
		expected bool
	}{
		{"True OR True", true, true, true},
		{"True OR False", true, false, true},
		{"False OR True", false, true, true},
		{"False OR False", false, false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			field1 := NewConstantFieldValue(boolMeta, tt.val1)
			field2 := NewConstantFieldValue(boolMeta, tt.val2)

			logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorOr, field1, field2)
			require.NotNil(t, logicalField)

			result, err := executeLogicalFieldValue(t, logicalField, ctx)
			require.NoError(t, err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestLogicalExpressionField_UnsupportedOperator(t *testing.T) {
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	field1 := NewConstantFieldValue(boolMeta, true)
	field2 := NewConstantFieldValue(boolMeta, false)

	// Try to use an unsupported operator (e.g., "xor" which doesn't exist)
	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorType("xor"), field1, field2)

	_, _, err := logicalField.Execute(tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported logical operator")
}

func TestLogicalExpressionField_NonBooleanOperand1(t *testing.T) {
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	field1 := NewConstantFieldValue(intMeta, int64(42))
	field2 := NewConstantFieldValue(boolMeta, true)

	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, field1, field2)

	_, _, err := logicalField.Execute(tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "operand 1")
	require.Contains(t, err.Error(), "boolean type")
}

func TestLogicalExpressionField_NonBooleanOperand2(t *testing.T) {
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	stringMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeString, Required: true}

	field1 := NewConstantFieldValue(boolMeta, true)
	field2 := NewConstantFieldValue(stringMeta, "hello")

	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, field1, field2)

	_, _, err := logicalField.Execute(tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "operand 2")
	require.Contains(t, err.Error(), "boolean type")
}

func TestLogicalExpressionField_OptionalOperand1(t *testing.T) {
	optionalBoolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: false}
	requiredBoolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	field1 := NewConstantFieldValue(optionalBoolMeta, true)
	field2 := NewConstantFieldValue(requiredBoolMeta, true)

	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, field1, field2)

	_, _, err := logicalField.Execute(tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "operand 1")
	require.Contains(t, err.Error(), "required")
}

func TestLogicalExpressionField_OptionalOperand2(t *testing.T) {
	requiredBoolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	optionalBoolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: false}

	field1 := NewConstantFieldValue(requiredBoolMeta, true)
	field2 := NewConstantFieldValue(optionalBoolMeta, true)

	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, field1, field2)

	_, _, err := logicalField.Execute(tsquery.FieldMeta{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "operand 2")
	require.Contains(t, err.Error(), "required")
}

func TestLogicalExpressionField_MetadataCorrect(t *testing.T) {
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	field1 := NewConstantFieldValue(boolMeta, true)
	field2 := NewConstantFieldValue(boolMeta, false)

	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, field1, field2)

	meta, err := executeLogicalFieldValueMeta(t, logicalField)
	require.NoError(t, err)
	require.Equal(t, tsquery.DataTypeBoolean, meta.DataType)
	require.True(t, meta.Required)
}

func TestLogicalExpressionField_ErrorPropagationOperand1(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	// Create an error field for operand 1
	errorField1 := &testErrorField{meta: boolMeta, err: fmt.Errorf("operand1 error")}
	field2 := NewConstantFieldValue(boolMeta, true)

	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, errorField1, field2)

	_, err := executeLogicalFieldValue(t, logicalField, ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "operand 1")
}

func TestLogicalExpressionField_ErrorPropagationOperand2(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	field1 := NewConstantFieldValue(boolMeta, true)
	// Create an error field for operand 2
	errorField2 := &testErrorField{meta: boolMeta, err: fmt.Errorf("operand2 error")}

	logicalField := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, field1, errorField2)

	_, err := executeLogicalFieldValue(t, logicalField, ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "operand 2")
}

func TestLogicalExpressionField_ComplexExpression(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	// Create: (true AND false) OR (false OR true)
	// = false OR true
	// = true

	trueField := NewConstantFieldValue(boolMeta, true)
	falseField := NewConstantFieldValue(boolMeta, false)

	// (true AND false) = false
	andExpr := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, trueField, falseField)

	// Create new instances for the second part
	falseField2 := NewConstantFieldValue(boolMeta, false)
	trueField2 := NewConstantFieldValue(boolMeta, true)

	// (false OR true) = true
	orExpr2 := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorOr, falseField2, trueField2)

	// false OR true = true
	orExpr := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorOr, andExpr, orExpr2)

	result, err := executeLogicalFieldValue(t, orExpr, ctx)
	require.NoError(t, err)
	require.Equal(t, true, result)
}

func TestLogicalExpressionField_ChainedAndOperations(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	// Create: true AND true AND true = true
	trueField1 := NewConstantFieldValue(boolMeta, true)
	trueField2 := NewConstantFieldValue(boolMeta, true)
	trueField3 := NewConstantFieldValue(boolMeta, true)

	// (true AND true)
	andExpr1 := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, trueField1, trueField2)

	// (true AND true) AND true = true
	andExpr2 := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, andExpr1, trueField3)

	result, err := executeLogicalFieldValue(t, andExpr2, ctx)
	require.NoError(t, err)
	require.Equal(t, true, result)
}

func TestLogicalExpressionField_ChainedOrOperations(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	// Create: false OR false OR true = true
	falseField1 := NewConstantFieldValue(boolMeta, false)
	falseField2 := NewConstantFieldValue(boolMeta, false)
	trueField := NewConstantFieldValue(boolMeta, true)

	// (false OR false)
	orExpr1 := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorOr, falseField1, falseField2)

	// (false OR false) OR true = true
	orExpr2 := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorOr, orExpr1, trueField)

	result, err := executeLogicalFieldValue(t, orExpr2, ctx)
	require.NoError(t, err)
	require.Equal(t, true, result)
}

func TestLogicalExpressionField_DeMorgansLaw(t *testing.T) {
	ctx := context.Background()

	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	// Test De Morgan's Law: NOT(A AND B) = NOT(A) OR NOT(B)
	// Since we don't have NOT operator, we can at least verify the truth table

	tests := []struct {
		name string
		a    bool
		b    bool
	}{
		{"Both True", true, true},
		{"A True, B False", true, false},
		{"A False, B True", false, true},
		{"Both False", false, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fieldA := NewConstantFieldValue(boolMeta, tt.a)
			fieldB := NewConstantFieldValue(boolMeta, tt.b)

			andExpr := NewLogicalExpressionFieldValue(tsquery.LogicalOperatorAnd, fieldA, fieldB)

			result, err := executeLogicalFieldValue(t, andExpr, ctx)
			require.NoError(t, err)

			// Verify the AND result
			expected := tt.a && tt.b
			require.Equal(t, expected, result)
		})
	}
}
