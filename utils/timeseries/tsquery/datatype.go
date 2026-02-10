package tsquery

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"reflect"
)

type DataType string

const (
	DataTypeInteger   DataType = "integer"
	DataTypeDecimal   DataType = "decimal"
	DataTypeString    DataType = "string"
	DataTypeBoolean   DataType = "boolean"
	DataTypeTimestamp DataType = "timestamp"
)

func DataTypeByGoType(kind reflect.Kind) (DataType, error) {
	switch kind {
	case reflect.Bool:
		return DataTypeBoolean, nil
	case reflect.Int64:
		return DataTypeInteger, nil
	case reflect.Float64:
		return DataTypeDecimal, nil
	case reflect.String:
		return DataTypeString, nil
	default:
		return "", fmt.Errorf("unknown data type %v", kind)
	}

}

func (dt DataType) ValidateData(d any) error {
	switch dt {
	case DataTypeInteger:
		_, ok := d.(int64)
		if !ok {
			return fmt.Errorf("expected integer data type, got %T", d)
		}
		return nil
	case DataTypeDecimal:
		_, ok := d.(float64)
		if !ok {
			return fmt.Errorf("expected decimal data type, got %T", d)
		}
		return nil
	case DataTypeString:
		_, ok := d.(string)
		if !ok {
			return fmt.Errorf("expected string data type, got %T", d)
		}
		return nil
	case DataTypeBoolean:
		_, ok := d.(bool)
		if !ok {
			return fmt.Errorf("expected boolean data type, got %T", d)
		}
		return nil
	}
	return fmt.Errorf("unsupported data type: %s", dt)
}

func (dt DataType) Validate() error {
	switch dt {
	case DataTypeInteger, DataTypeDecimal, DataTypeString, DataTypeBoolean, DataTypeTimestamp:
		return nil
	}
	return fmt.Errorf("invalid data type: %s", dt)
}

func (dt DataType) IsNumeric() bool {
	return dt == DataTypeInteger || dt == DataTypeDecimal
}

func (dt DataType) ToFloat64(val any) (float64, error) {
	switch dt {
	case DataTypeInteger:
		switch v := val.(type) {
		case int64:
			return float64(v), nil
		case float64:
			return v, nil
		default:
			return 0, fmt.Errorf("unsupported type %T for integer-to-float64 conversion", val)
		}
	case DataTypeDecimal:
		return val.(float64), nil
	default:
		return 0, fmt.Errorf("unsupported data type for conversion to decimal: %s", dt)
	}
}

func (dt DataType) FromFloat64(val float64) (any, error) {
	switch dt {
	case DataTypeInteger:
		return int64(val), nil
	case DataTypeDecimal:
		return val, nil
	default:
		return 0, fmt.Errorf("unsupported data type for conversion from decimal: %s", dt)
	}
}

func (dt DataType) ForceCastAndValidate(value any) (any, error) {
	switch dt {
	case DataTypeInteger:
		return util.AnyToInt64(value)
	case DataTypeDecimal:
		return util.AnyToFloat64(value)
	case DataTypeString:
		return fmt.Sprintf("%s", value), nil
	case DataTypeBoolean:
		return util.AnyToBoolean(value)
	case DataTypeTimestamp:
		return util.AnyToTime(value)
	default:
		return nil, fmt.Errorf("unsupported data type for force cast: %s", dt)
	}
}
