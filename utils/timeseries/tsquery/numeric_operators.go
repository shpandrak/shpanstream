package tsquery

import (
	"fmt"
	"math"
	"strconv"
)

type BinaryNumericOperatorType string

// Integer operations

func AddInt(v1, v2 any) any {
	return v1.(int64) + v2.(int64)
}

func SubInt(v1, v2 any) any {
	return v1.(int64) - v2.(int64)
}

func MulInt(v1, v2 any) any {
	return v1.(int64) * v2.(int64)
}

func DivInt(v1, v2 any) any {
	return v1.(int64) / v2.(int64)
}

func ModInt(v1, v2 any) any {
	return v1.(int64) % v2.(int64)
}

// Decimal operations

func AddDecimal(v1, v2 any) any {
	return v1.(float64) + v2.(float64)
}

func SubDecimal(v1, v2 any) any {
	return v1.(float64) - v2.(float64)
}

func MulDecimal(v1, v2 any) any {
	return v1.(float64) * v2.(float64)
}

func DivDecimal(v1, v2 any) any {
	return v1.(float64) / v2.(float64)
}

func (bno BinaryNumericOperatorType) GetFuncImpl(forDataType DataType) (func(v1, v2 any) any, error) {
	switch forDataType {
	case DataTypeInteger:
		switch bno {
		case BinaryNumericOperatorAdd:
			return AddInt, nil
		case BinaryNumericOperatorSub:
			return SubInt, nil
		case BinaryNumericOperatorMul:
			return MulInt, nil
		case BinaryNumericOperatorDiv:
			return DivInt, nil
		case BinaryNumericOperatorMod:
			return ModInt, nil
		default:
			return nil, fmt.Errorf("unsupported operator %s for data type %s", bno, forDataType)
		}
	case DataTypeDecimal:
		switch bno {
		case BinaryNumericOperatorAdd:
			return AddDecimal, nil
		case BinaryNumericOperatorSub:
			return SubDecimal, nil
		case BinaryNumericOperatorMul:
			return MulDecimal, nil
		case BinaryNumericOperatorDiv:
			return DivDecimal, nil
		default:
			return nil, fmt.Errorf("unsupported operator %s for data type %s", bno, forDataType)
		}
	}
	return nil, fmt.Errorf("unsupported data type %s for numeric operations", forDataType)
}

const (
	BinaryNumericOperatorAdd BinaryNumericOperatorType = "+"
	BinaryNumericOperatorSub BinaryNumericOperatorType = "-"
	BinaryNumericOperatorMul BinaryNumericOperatorType = "*"
	BinaryNumericOperatorDiv BinaryNumericOperatorType = "/"
	BinaryNumericOperatorMod BinaryNumericOperatorType = "%"
)

type UnaryNumericOperatorType string

const (
	UnaryNumericOperatorAbs    UnaryNumericOperatorType = "abs"
	UnaryNumericOperatorNegate UnaryNumericOperatorType = "negate"
	UnaryNumericOperatorSqrt   UnaryNumericOperatorType = "sqrt"
	UnaryNumericOperatorCeil   UnaryNumericOperatorType = "ceil"
	UnaryNumericOperatorFloor  UnaryNumericOperatorType = "floor"
	UnaryNumericOperatorRound  UnaryNumericOperatorType = "round"
	UnaryNumericOperatorLog    UnaryNumericOperatorType = "log"
	UnaryNumericOperatorLog10  UnaryNumericOperatorType = "log10"
	UnaryNumericOperatorExp    UnaryNumericOperatorType = "exp"
	UnaryNumericOperatorSin    UnaryNumericOperatorType = "sin"
	UnaryNumericOperatorCos    UnaryNumericOperatorType = "cos"
	UnaryNumericOperatorTan    UnaryNumericOperatorType = "tan"
)

// Integer operations
func absInt(v any) any {
	val := v.(int64)
	if val < 0 {
		return -val
	}
	return val
}

func negateInt(v any) any {
	return -v.(int64)
}

func sqrtInt(v any) any {
	return int64(math.Sqrt(float64(v.(int64))))
}

// Decimal operations
func absDecimal(v any) any {
	return math.Abs(v.(float64))
}

func negateDecimal(v any) any {
	return -v.(float64)
}

func sqrtDecimal(v any) any {
	return math.Sqrt(v.(float64))
}

func ceilDecimal(v any) any {
	return math.Ceil(v.(float64))
}

func floorDecimal(v any) any {
	return math.Floor(v.(float64))
}

func roundDecimal(v any) any {
	return math.Round(v.(float64))
}

func logDecimal(v any) any {
	return math.Log(v.(float64))
}

func log10Decimal(v any) any {
	return math.Log10(v.(float64))
}

func expDecimal(v any) any {
	return math.Exp(v.(float64))
}

func sinDecimal(v any) any {
	return math.Sin(v.(float64))
}

func cosDecimal(v any) any {
	return math.Cos(v.(float64))
}

func tanDecimal(v any) any {
	return math.Tan(v.(float64))
}

func (uno UnaryNumericOperatorType) GetFuncImpl(forDataType DataType) (func(v any) any, error) {
	switch forDataType {
	case DataTypeInteger:
		switch uno {
		case UnaryNumericOperatorAbs:
			return absInt, nil
		case UnaryNumericOperatorNegate:
			return negateInt, nil
		case UnaryNumericOperatorSqrt:
			return sqrtInt, nil
		default:
			return nil, fmt.Errorf("unsupported operator %s for data type %s", uno, forDataType)
		}
	case DataTypeDecimal:
		switch uno {
		case UnaryNumericOperatorAbs:
			return absDecimal, nil
		case UnaryNumericOperatorNegate:
			return negateDecimal, nil
		case UnaryNumericOperatorSqrt:
			return sqrtDecimal, nil
		case UnaryNumericOperatorCeil:
			return ceilDecimal, nil
		case UnaryNumericOperatorFloor:
			return floorDecimal, nil
		case UnaryNumericOperatorRound:
			return roundDecimal, nil
		case UnaryNumericOperatorLog:
			return logDecimal, nil
		case UnaryNumericOperatorLog10:
			return log10Decimal, nil
		case UnaryNumericOperatorExp:
			return expDecimal, nil
		case UnaryNumericOperatorSin:
			return sinDecimal, nil
		case UnaryNumericOperatorCos:
			return cosDecimal, nil
		case UnaryNumericOperatorTan:
			return tanDecimal, nil
		default:
			return nil, fmt.Errorf("unsupported operator %s for data type %s", uno, forDataType)
		}
	}
	return nil, fmt.Errorf("unsupported data type %s for unary numeric operations", forDataType)
}

// Integer conversions
func castIntToDecimal(v any) (any, error) {
	return float64(v.(int64)), nil
}

func castIntToString(v any) (any, error) {
	return strconv.FormatInt(v.(int64), 10), nil
}

// Decimal conversions
func castDecimalToInt(v any) (any, error) {
	return int64(v.(float64)), nil
}

func castDecimalToString(v any) (any, error) {
	return strconv.FormatFloat(v.(float64), 'f', -1, 64), nil
}

// String conversions
func castStringToInt(v any) (any, error) {
	val, err := strconv.ParseInt(v.(string), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse string to integer: %w", err)
	}
	return val, nil
}

func castStringToDecimal(v any) (any, error) {
	val, err := strconv.ParseFloat(v.(string), 64)
	if err != nil {
		return nil, fmt.Errorf("failed to parse string to decimal: %w", err)
	}
	return val, nil
}

// Identity function for same-type casts
func castIdentity(v any) (any, error) {
	return v, nil
}

func GetCastFuncForDataType(sourceType, targetType DataType) (func(any) (any, error), error) {
	// Same type - no conversion needed
	if sourceType == targetType {
		return castIdentity, nil
	}

	// Handle unsupported types
	if sourceType == DataTypeBoolean || targetType == DataTypeBoolean {
		return nil, fmt.Errorf("casting to/from boolean is not supported")
	}
	if sourceType == DataTypeTimestamp || targetType == DataTypeTimestamp {
		return nil, fmt.Errorf("casting to/from timestamp is not supported")
	}

	// Integer source conversions
	if sourceType == DataTypeInteger {
		switch targetType {
		case DataTypeDecimal:
			return castIntToDecimal, nil
		case DataTypeString:
			return castIntToString, nil
		}
	}

	// Decimal source conversions
	if sourceType == DataTypeDecimal {
		switch targetType {
		case DataTypeInteger:
			return castDecimalToInt, nil
		case DataTypeString:
			return castDecimalToString, nil
		}
	}

	// String source conversions
	if sourceType == DataTypeString {
		switch targetType {
		case DataTypeInteger:
			return castStringToInt, nil
		case DataTypeDecimal:
			return castStringToDecimal, nil
		}
	}

	return nil, fmt.Errorf("unsupported cast from %s to %s", sourceType, targetType)
}
