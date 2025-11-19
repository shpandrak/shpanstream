package tsquery

import "fmt"

type ReductionType string

const (
	ReductionTypeSum   ReductionType = "sum"
	ReductionTypeAvg   ReductionType = "avg"
	ReductionTypeMin   ReductionType = "min"
	ReductionTypeMax   ReductionType = "max"
	ReductionTypeCount ReductionType = "count"
)

func (reductionType ReductionType) GetResultDataType(forDataType DataType) DataType {
	if reductionType == ReductionTypeAvg {
		// Average always returns decimal
		return DataTypeDecimal
	} else if reductionType == ReductionTypeCount {
		// Count always returns an integer
		return DataTypeInteger
	} else {
		// Sum, Min, Max preserve the input data type
		return forDataType
	}
}

// GetReducerFunc creates an optimized reducer function based on reduction type and data type
func (reductionType ReductionType) GetReducerFunc(forDataType DataType) (func([]any) any, error) {
	switch reductionType {
	case ReductionTypeSum:
		if forDataType == DataTypeInteger {
			return sumInt, nil
		}
		return sumDecimal, nil

	case ReductionTypeAvg:
		if forDataType == DataTypeInteger {
			return avgInt, nil
		}
		return avgDecimal, nil

	case ReductionTypeMin:
		if forDataType == DataTypeInteger {
			return minInt, nil
		}
		return minDecimal, nil

	case ReductionTypeMax:
		if forDataType == DataTypeInteger {
			return maxInt, nil
		}
		return maxDecimal, nil

	case ReductionTypeCount:
		// Count is independent of a data type
		return countValues, nil

	default:
		// This should never happen if validation is correct
		return nil, fmt.Errorf("unsupported reduction type: %s", reductionType)
	}
}

// Integer reduction functions
func sumInt(values []any) any {
	var sum int64
	for _, v := range values {
		sum += v.(int64)
	}
	return sum
}

func avgInt(values []any) any {
	var sum int64
	for _, v := range values {
		sum += v.(int64)
	}
	return float64(sum) / float64(len(values))
}

func minInt(values []any) any {
	minVal := values[0].(int64)
	for i := 1; i < len(values); i++ {
		val := values[i].(int64)
		if val < minVal {
			minVal = val
		}
	}
	return minVal
}

func maxInt(values []any) any {
	maxVal := values[0].(int64)
	for i := 1; i < len(values); i++ {
		val := values[i].(int64)
		if val > maxVal {
			maxVal = val
		}
	}
	return maxVal
}

// Decimal reduction functions
func sumDecimal(values []any) any {
	var sum float64
	for _, v := range values {
		sum += v.(float64)
	}
	return sum
}

func avgDecimal(values []any) any {
	var sum float64
	for _, v := range values {
		sum += v.(float64)
	}
	return sum / float64(len(values))
}

func minDecimal(values []any) any {
	minVal := values[0].(float64)
	for i := 1; i < len(values); i++ {
		val := values[i].(float64)
		if val < minVal {
			minVal = val
		}
	}
	return minVal
}

func maxDecimal(values []any) any {
	maxVal := values[0].(float64)
	for i := 1; i < len(values); i++ {
		val := values[i].(float64)
		if val > maxVal {
			maxVal = val
		}
	}
	return maxVal
}

// Count function (works for both integer and decimal)
func countValues(values []any) any {
	return int64(len(values))
}
