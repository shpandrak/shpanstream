package util

import (
	"fmt"
	"strconv"
	"time"
)

func AnyToFloat64(value any) (float64, error) {
	switch typedValue := value.(type) {
	case float64:
		return typedValue, nil
	case float32:
		return float64(typedValue), nil
	case uint:
		return float64(typedValue), nil
	case int64:
		return float64(typedValue), nil
	case uint64:
		return float64(typedValue), nil
	case int:
		return float64(typedValue), nil
	case uint16:
		return float64(typedValue), nil
	case int16:
		return float64(typedValue), nil
	case uint32:
		return float64(typedValue), nil
	case int32:
		return float64(typedValue), nil
	case uint8:
		return float64(typedValue), nil
	case int8:
		return float64(typedValue), nil
	case string:
		return strconv.ParseFloat(typedValue, 64)
	default:
		return 0, fmt.Errorf("value %v of type %T cannot be converted to float64", value, typedValue)
	}
}

func AnyToInt64(value any) (int64, error) {
	switch typedValue := value.(type) {
	case float64:
		return int64(typedValue), nil
	case float32:
		return int64(typedValue), nil
	case uint:
		return int64(typedValue), nil
	case int64:
		return typedValue, nil
	case uint64:
		return int64(typedValue), nil
	case int:
		return int64(typedValue), nil
	case uint16:
		return int64(typedValue), nil
	case int16:
		return int64(typedValue), nil
	case uint32:
		return int64(typedValue), nil
	case int32:
		return int64(typedValue), nil
	case uint8:
		return int64(typedValue), nil
	case int8:
		return int64(typedValue), nil
	case string:
		return strconv.ParseInt(typedValue, 10, 64)

	default:
		return 0, fmt.Errorf("value %v of type %T cannot be converted to int64", value, typedValue)
	}
}

func AnyToBoolean(value any) (bool, error) {
	switch typedValue := value.(type) {
	case bool:
		return typedValue, nil
	case string:
		return strconv.ParseBool(typedValue)
	default:
		return false, fmt.Errorf("value %v of type %T cannot be converted to boolean", value, typedValue)
	}
}

func AnyToTime(value any) (time.Time, error) {
	switch typedValue := value.(type) {
	case time.Time:
		return typedValue, nil
	case string:
		return time.Parse(time.RFC3339, typedValue)
	default:
		return time.Time{}, fmt.Errorf("value %v of type %T cannot be converted to type", value, typedValue)

	}
}

func CastNumericPtr[In ~int | ~int64 | ~float32 | ~float64, Out ~int | ~int64 | ~float32 | ~float64](v *In) *Out {
	if v == nil {
		return nil
	}
	val := Out(*v)
	return &val
}
