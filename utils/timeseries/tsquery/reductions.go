package tsquery

import (
	"fmt"
	"time"
)

type ReductionType string

const (
	ReductionTypeSum   ReductionType = "sum"
	ReductionTypeAvg   ReductionType = "avg"
	ReductionTypeMin   ReductionType = "min"
	ReductionTypeMax   ReductionType = "max"
	ReductionTypeCount ReductionType = "count"
	ReductionTypeFirst ReductionType = "first"
	ReductionTypeLast  ReductionType = "last"
)

// RequiresNumeric reports whether this reduction type requires a numeric input data type.
// count, first, and last work on any type; all others (sum, avg, min, max) require numeric.
func (reductionType ReductionType) RequiresNumeric() bool {
	switch reductionType {
	case ReductionTypeCount, ReductionTypeFirst, ReductionTypeLast:
		return false
	default:
		return true
	}
}

func (reductionType ReductionType) UseIdentityWhenSingleValue() bool {
	switch reductionType {
	case ReductionTypeSum, ReductionTypeAvg, ReductionTypeMax, ReductionTypeMin, ReductionTypeFirst, ReductionTypeLast:
		return true
	case ReductionTypeCount:
		return false
	default:
		return false
	}
}
func (reductionType ReductionType) GetResultDataType(forDataType DataType) DataType {
	if reductionType == ReductionTypeAvg {
		// Average always returns decimal
		return DataTypeDecimal
	} else if reductionType == ReductionTypeCount {
		// Count always returns an integer
		return DataTypeInteger
	} else {
		// Sum, Min, Max, First, Last preserve the input data type
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

	case ReductionTypeFirst:
		// First returns the first value in the slice
		return firstValue, nil

	case ReductionTypeLast:
		// Last returns the last value in the slice
		return lastValue, nil

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

// First returns the first value in the slice
func firstValue(values []any) any {
	return values[0]
}

// Last returns the last value in the slice
func lastValue(values []any) any {
	return values[len(values)-1]
}

// Accumulator provides streaming aggregation without collecting all values into memory.
type Accumulator interface {
	// Add feeds a value and its timestamp to the accumulator.
	// sum/avg/count ignore the timestamp; first/last/min/max track it.
	Add(value any, ts time.Time)
	// Result returns the aggregated scalar value, or nil if no values were added.
	Result() any
	// ResultTimestamp returns the timestamp associated with the result, or nil
	// for reductions where timestamp has no meaning (sum, avg, count).
	ResultTimestamp() *time.Time
}

// NewAccumulator creates a streaming accumulator for this reduction type and data type.
func (reductionType ReductionType) NewAccumulator(forDataType DataType) (Accumulator, error) {
	switch reductionType {
	case ReductionTypeSum:
		if forDataType == DataTypeInteger {
			return &sumIntAccumulator{}, nil
		}
		return &sumDecimalAccumulator{}, nil
	case ReductionTypeAvg:
		if forDataType == DataTypeInteger {
			return &avgIntAccumulator{}, nil
		}
		return &avgDecimalAccumulator{}, nil
	case ReductionTypeMin:
		if forDataType == DataTypeInteger {
			return &minIntAccumulator{}, nil
		}
		return &minDecimalAccumulator{}, nil
	case ReductionTypeMax:
		if forDataType == DataTypeInteger {
			return &maxIntAccumulator{}, nil
		}
		return &maxDecimalAccumulator{}, nil
	case ReductionTypeCount:
		return &countAccumulator{}, nil
	case ReductionTypeFirst:
		return &firstAccumulator{}, nil
	case ReductionTypeLast:
		return &lastAccumulator{}, nil
	default:
		return nil, fmt.Errorf("unsupported reduction type: %s", reductionType)
	}
}

// --- Accumulator implementations ---

// sumIntAccumulator accumulates integer sum.
type sumIntAccumulator struct {
	sum   int64
	hasAny bool
}

func (a *sumIntAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.sum += value.(int64)
	a.hasAny = true
}

func (a *sumIntAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.sum
}

func (a *sumIntAccumulator) ResultTimestamp() *time.Time { return nil }

// sumDecimalAccumulator accumulates decimal sum.
type sumDecimalAccumulator struct {
	sum    float64
	hasAny bool
}

func (a *sumDecimalAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.sum += value.(float64)
	a.hasAny = true
}

func (a *sumDecimalAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.sum
}

func (a *sumDecimalAccumulator) ResultTimestamp() *time.Time { return nil }

// avgIntAccumulator accumulates integer average (result is decimal).
type avgIntAccumulator struct {
	sum   int64
	count int64
}

func (a *avgIntAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.sum += value.(int64)
	a.count++
}

func (a *avgIntAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return float64(a.sum) / float64(a.count)
}

func (a *avgIntAccumulator) ResultTimestamp() *time.Time { return nil }

// avgDecimalAccumulator accumulates decimal average.
type avgDecimalAccumulator struct {
	sum   float64
	count int64
}

func (a *avgDecimalAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.sum += value.(float64)
	a.count++
}

func (a *avgDecimalAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return a.sum / float64(a.count)
}

func (a *avgDecimalAccumulator) ResultTimestamp() *time.Time { return nil }

// countAccumulator counts values (works for any data type).
// Note: Result() always returns int64(0) for empty streams (never nil),
// matching SQL semantics (COUNT(*) = 0 vs SUM() = NULL). This means the
// emptyValue fallback in aggregation will never trigger for count fields.
type countAccumulator struct {
	count int64
}

func (a *countAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.count++
}

func (a *countAccumulator) Result() any {
	return a.count
}

func (a *countAccumulator) ResultTimestamp() *time.Time { return nil }

// minIntAccumulator tracks integer minimum and its timestamp.
type minIntAccumulator struct {
	min    int64
	ts     time.Time
	hasAny bool
}

func (a *minIntAccumulator) Add(value any, ts time.Time) {
	if value == nil {
		return
	}
	v := value.(int64)
	if !a.hasAny || v < a.min {
		a.min = v
		a.ts = ts
	}
	a.hasAny = true
}

func (a *minIntAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.min
}

func (a *minIntAccumulator) ResultTimestamp() *time.Time {
	if !a.hasAny {
		return nil
	}
	return &a.ts
}

// minDecimalAccumulator tracks decimal minimum and its timestamp.
type minDecimalAccumulator struct {
	min    float64
	ts     time.Time
	hasAny bool
}

func (a *minDecimalAccumulator) Add(value any, ts time.Time) {
	if value == nil {
		return
	}
	v := value.(float64)
	if !a.hasAny || v < a.min {
		a.min = v
		a.ts = ts
	}
	a.hasAny = true
}

func (a *minDecimalAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.min
}

func (a *minDecimalAccumulator) ResultTimestamp() *time.Time {
	if !a.hasAny {
		return nil
	}
	return &a.ts
}

// maxIntAccumulator tracks integer maximum and its timestamp.
type maxIntAccumulator struct {
	max    int64
	ts     time.Time
	hasAny bool
}

func (a *maxIntAccumulator) Add(value any, ts time.Time) {
	if value == nil {
		return
	}
	v := value.(int64)
	if !a.hasAny || v > a.max {
		a.max = v
		a.ts = ts
	}
	a.hasAny = true
}

func (a *maxIntAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.max
}

func (a *maxIntAccumulator) ResultTimestamp() *time.Time {
	if !a.hasAny {
		return nil
	}
	return &a.ts
}

// maxDecimalAccumulator tracks decimal maximum and its timestamp.
type maxDecimalAccumulator struct {
	max    float64
	ts     time.Time
	hasAny bool
}

func (a *maxDecimalAccumulator) Add(value any, ts time.Time) {
	if value == nil {
		return
	}
	v := value.(float64)
	if !a.hasAny || v > a.max {
		a.max = v
		a.ts = ts
	}
	a.hasAny = true
}

func (a *maxDecimalAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.max
}

func (a *maxDecimalAccumulator) ResultTimestamp() *time.Time {
	if !a.hasAny {
		return nil
	}
	return &a.ts
}

// firstAccumulator keeps the first non-nil value and its timestamp.
type firstAccumulator struct {
	value  any
	ts     time.Time
	hasAny bool
}

func (a *firstAccumulator) Add(value any, ts time.Time) {
	if value == nil {
		return
	}
	if !a.hasAny {
		a.value = value
		a.ts = ts
		a.hasAny = true
	}
}

func (a *firstAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.value
}

func (a *firstAccumulator) ResultTimestamp() *time.Time {
	if !a.hasAny {
		return nil
	}
	return &a.ts
}

// lastAccumulator overwrites on each Add, keeping the last value and its timestamp.
type lastAccumulator struct {
	value  any
	ts     time.Time
	hasAny bool
}

func (a *lastAccumulator) Add(value any, ts time.Time) {
	if value == nil {
		return
	}
	a.value = value
	a.ts = ts
	a.hasAny = true
}

func (a *lastAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.value
}

func (a *lastAccumulator) ResultTimestamp() *time.Time {
	if !a.hasAny {
		return nil
	}
	return &a.ts
}
