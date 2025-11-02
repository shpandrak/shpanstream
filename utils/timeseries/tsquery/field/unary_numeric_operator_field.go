package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"math"
)

var _ Field = UnaryNumericOperatorField{}

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
	operand  Field
	op       UnaryNumericOperatorType
	fieldUrn string
}

func NewUnaryNumericOperatorField(
	fieldUrn string,
	operand Field,
	op UnaryNumericOperatorType,
) UnaryNumericOperatorField {
	return UnaryNumericOperatorField{
		operand:  operand,
		op:       op,
		fieldUrn: fieldUrn,
	}
}

func (unof UnaryNumericOperatorField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	// Execute operand to get metadata (lazy validation)
	operandMeta, operandValueSupplier, err := unof.operand.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed executing operand field: %w", err)
	}
	dt := operandMeta.DataType()

	// Check that operand is a numeric type
	if !dt.IsNumeric() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("operand field %s has non-numeric data type: %s", operandMeta.Urn(), dt)
	}

	// Get the function implementation
	funcImpl, err := unof.op.getFuncImpl(dt)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to get function implementation: %w", err)
	}

	// Create field metadata - unary operations maintain the same data type
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		unof.fieldUrn,
		dt,
		operandMeta.Required(),
		operandMeta.Unit(),
		operandMeta.CustomMeta(),
	)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to create field meta for unary operator field: %w", err)
	}

	// Allow nil in the case of optional fields
	if !fieldMeta.Required() {
		originalFunc := funcImpl
		funcImpl = func(v any) any {
			if v == nil {
				return nil
			}
			return originalFunc(v)
		}
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		value, err := operandValueSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed getting value for operand: %w", err)
		}
		return funcImpl(value), nil
	}

	return *fieldMeta, valueSupplier, nil
}
