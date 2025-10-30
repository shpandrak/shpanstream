package tsquery

import "fmt"

type DataType string

const (
	DataTypeInteger   DataType = "integer"
	DataTypeDecimal   DataType = "decimal"
	DataTypeString    DataType = "string"
	DataTypeBoolean   DataType = "boolean"
	DataTypeTimestamp DataType = "timestamp"
)

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
		return float64(val.(int64)), nil
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
