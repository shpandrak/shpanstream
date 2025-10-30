package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"strconv"
)

type CastField struct {
	source       Field
	targetType   tsquery.DataType
	castFunc     func(any) (any, error)
	fieldMeta    tsquery.FieldMeta
}

// Conversion functions for each type pair

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

func getCastFunc(sourceType, targetType tsquery.DataType) (func(any) (any, error), error) {
	// Same type - no conversion needed
	if sourceType == targetType {
		return castIdentity, nil
	}

	// Handle unsupported types
	if sourceType == tsquery.DataTypeBoolean || targetType == tsquery.DataTypeBoolean {
		return nil, fmt.Errorf("casting to/from boolean is not supported")
	}
	if sourceType == tsquery.DataTypeTimestamp || targetType == tsquery.DataTypeTimestamp {
		return nil, fmt.Errorf("casting to/from timestamp is not supported")
	}

	// Integer source conversions
	if sourceType == tsquery.DataTypeInteger {
		switch targetType {
		case tsquery.DataTypeDecimal:
			return castIntToDecimal, nil
		case tsquery.DataTypeString:
			return castIntToString, nil
		}
	}

	// Decimal source conversions
	if sourceType == tsquery.DataTypeDecimal {
		switch targetType {
		case tsquery.DataTypeInteger:
			return castDecimalToInt, nil
		case tsquery.DataTypeString:
			return castDecimalToString, nil
		}
	}

	// String source conversions
	if sourceType == tsquery.DataTypeString {
		switch targetType {
		case tsquery.DataTypeInteger:
			return castStringToInt, nil
		case tsquery.DataTypeDecimal:
			return castStringToDecimal, nil
		}
	}

	return nil, fmt.Errorf("unsupported cast from %s to %s", sourceType, targetType)
}

func NewCastField(
	fieldUrn string,
	source Field,
	targetType tsquery.DataType,
) (*CastField, error) {
	sourceType := source.Meta().DataType()

	// Get the cast function
	castFunc, err := getCastFunc(sourceType, targetType)
	if err != nil {
		return nil, fmt.Errorf("failed to get cast function: %w", err)
	}

	// Create field metadata for the cast result
	newFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		fieldUrn,
		targetType,
		source.Meta().Required(),
		source.Meta().CustomMeta(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create field meta for cast field: %w", err)
	}

	// Wrap cast function to handle nil for optional fields
	if !newFieldMeta.Required() {
		originalFunc := castFunc
		castFunc = func(v any) (any, error) {
			if v == nil {
				return nil, nil
			}
			return originalFunc(v)
		}
	}

	return &CastField{
		source:     source,
		targetType: targetType,
		castFunc:   castFunc,
		fieldMeta:  *newFieldMeta,
	}, nil
}

func (cf *CastField) Meta() tsquery.FieldMeta {
	return cf.fieldMeta
}

func (cf *CastField) GetValue(ctx context.Context) (any, error) {
	value, err := cf.source.GetValue(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed getting value from source field: %w", err)
	}

	result, err := cf.castFunc(value)
	if err != nil {
		return nil, fmt.Errorf("failed casting value: %w", err)
	}

	return result, nil
}
