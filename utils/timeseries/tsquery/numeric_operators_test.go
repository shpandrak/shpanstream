package tsquery

import (
	"testing"
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
