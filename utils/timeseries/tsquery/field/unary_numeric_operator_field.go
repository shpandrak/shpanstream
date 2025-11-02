package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"math"
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

func (uno UnaryNumericOperatorType) getFuncImpl(forDataType tsquery.DataType) (func(v any) any, error) {
	switch forDataType {
	case tsquery.DataTypeInteger:
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
	case tsquery.DataTypeDecimal:
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

type UnaryNumericOperatorField struct {
	operand   Field
	op        UnaryNumericOperatorType
	funcImpl  func(v any) any
	fieldMeta tsquery.FieldMeta
}

func NewUnaryNumericOperatorField(
	fieldUrn string,
	operand Field,
	op UnaryNumericOperatorType,
) (*UnaryNumericOperatorField, error) {
	dt := operand.Meta().DataType()

	// Check that operand is a numeric type
	if !dt.IsNumeric() {
		return nil, fmt.Errorf("operand field %s has non-numeric data type: %s", operand.Meta().Urn(), dt)
	}

	// Get the function implementation
	funcImpl, err := op.getFuncImpl(dt)
	if err != nil {
		return nil, fmt.Errorf("failed to get function implementation: %w", err)
	}

	// Create field metadata - unary operations maintain the same data type
	newFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		fieldUrn,
		dt,
		operand.Meta().Required(),
		operand.Meta().Unit(),
		operand.Meta().CustomMeta(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create field meta for unary operator field: %w", err)
	}

	// Allow nil in the case of optional fields
	if !newFieldMeta.Required() {
		originalFunc := funcImpl
		funcImpl = func(v any) any {
			if v == nil {
				return nil
			}
			return originalFunc(v)
		}
	}

	return &UnaryNumericOperatorField{
		operand:   operand,
		op:        op,
		funcImpl:  funcImpl,
		fieldMeta: *newFieldMeta,
	}, nil
}

func (unof *UnaryNumericOperatorField) Meta() tsquery.FieldMeta {
	return unof.fieldMeta
}

func (unof *UnaryNumericOperatorField) GetValue(ctx context.Context) (any, error) {
	value, err := unof.operand.GetValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting value for operand: %w", err)
	}
	return unof.funcImpl(value), nil
}
