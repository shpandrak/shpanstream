package tsquery

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBinaryNumericOperatorType_Validate_Valid(t *testing.T) {
	validOps := []BinaryNumericOperatorType{
		BinaryNumericOperatorAdd,
		BinaryNumericOperatorSub,
		BinaryNumericOperatorMul,
		BinaryNumericOperatorDiv,
		BinaryNumericOperatorMod,
		BinaryNumericOperatorPow,
	}
	for _, op := range validOps {
		if err := op.Validate(); err != nil {
			t.Errorf("expected valid operator %q to pass validation, got error: %v", op, err)
		}
	}
}

func TestBinaryNumericOperatorType_Validate_Invalid(t *testing.T) {
	invalidOps := []BinaryNumericOperatorType{
		"",
		"invalid",
		"plus",
		"&&",
	}
	for _, op := range invalidOps {
		if err := op.Validate(); err == nil {
			t.Errorf("expected invalid operator %q to fail validation, but got nil", op)
		}
	}
}

func TestUnaryNumericOperatorType_Validate_Valid(t *testing.T) {
	validOps := []UnaryNumericOperatorType{
		UnaryNumericOperatorAbs,
		UnaryNumericOperatorNegate,
		UnaryNumericOperatorSqrt,
		UnaryNumericOperatorCeil,
		UnaryNumericOperatorFloor,
		UnaryNumericOperatorRound,
		UnaryNumericOperatorLog,
		UnaryNumericOperatorLog10,
		UnaryNumericOperatorExp,
		UnaryNumericOperatorSin,
		UnaryNumericOperatorCos,
		UnaryNumericOperatorTan,
	}
	for _, op := range validOps {
		if err := op.Validate(); err != nil {
			t.Errorf("expected valid operator %q to pass validation, got error: %v", op, err)
		}
	}
}

func TestUnaryNumericOperatorType_Validate_Invalid(t *testing.T) {
	invalidOps := []UnaryNumericOperatorType{
		"",
		"invalid",
		"ln",
		"power",
	}
	for _, op := range invalidOps {
		if err := op.Validate(); err == nil {
			t.Errorf("expected invalid operator %q to fail validation, but got nil", op)
		}
	}
}

func TestDivInt_DivisionByZero_Panics(t *testing.T) {
	require.PanicsWithValue(t, DivisionByZeroError{}, func() {
		DivInt(int64(10), int64(0))
	})
}

func TestDivDecimal_DivisionByZero_Panics(t *testing.T) {
	require.PanicsWithValue(t, DivisionByZeroError{}, func() {
		DivDecimal(float64(10), float64(0))
	})
}

func TestModInt_DivisionByZero_Panics(t *testing.T) {
	require.PanicsWithValue(t, DivisionByZeroError{}, func() {
		ModInt(int64(10), int64(0))
	})
}

func TestDivInt_ValidDivision(t *testing.T) {
	result := DivInt(int64(10), int64(3))
	require.Equal(t, int64(3), result)
}

func TestDivDecimal_ValidDivision(t *testing.T) {
	result := DivDecimal(float64(10), float64(3))
	require.InDelta(t, 3.3333333, result, 0.0001)
}

func TestModInt_ValidMod(t *testing.T) {
	result := ModInt(int64(10), int64(3))
	require.Equal(t, int64(1), result)
}
