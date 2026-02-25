package tsquery

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/shpandrak/shpanstream/utils/tdigest"
)

type ReductionType string

const (
	ReductionTypeSum      ReductionType = "sum"
	ReductionTypeAvg      ReductionType = "avg"
	ReductionTypeMin      ReductionType = "min"
	ReductionTypeMax      ReductionType = "max"
	ReductionTypeCount    ReductionType = "count"
	ReductionTypeFirst    ReductionType = "first"
	ReductionTypeLast     ReductionType = "last"
	ReductionTypeStddev   ReductionType = "stddev"
	ReductionTypeVariance ReductionType = "variance"
	ReductionTypeSpread   ReductionType = "spread"
	ReductionTypeP50      ReductionType = "p50"
	ReductionTypeP75      ReductionType = "p75"
	ReductionTypeP90      ReductionType = "p90"
	ReductionTypeP95      ReductionType = "p95"
	ReductionTypeP99      ReductionType = "p99"
	ReductionTypeP999     ReductionType = "p999"
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
	case ReductionTypeSum, ReductionTypeAvg, ReductionTypeMax, ReductionTypeMin, ReductionTypeFirst, ReductionTypeLast,
		ReductionTypeP50, ReductionTypeP75, ReductionTypeP90, ReductionTypeP95, ReductionTypeP99, ReductionTypeP999:
		return true
	case ReductionTypeCount:
		return false
	default:
		// stddev, variance, spread return false (single value → 0)
		return false
	}
}
func (reductionType ReductionType) GetResultDataType(forDataType DataType) DataType {
	switch reductionType {
	case ReductionTypeAvg, ReductionTypeStddev, ReductionTypeVariance,
		ReductionTypeP50, ReductionTypeP75, ReductionTypeP90, ReductionTypeP95, ReductionTypeP99, ReductionTypeP999:
		// These always return decimal (due to sqrt, division, or interpolation)
		return DataTypeDecimal
	case ReductionTypeCount:
		// Count always returns an integer
		return DataTypeInteger
	default:
		// Sum, Min, Max, First, Last, Spread preserve the input data type
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

	case ReductionTypeStddev:
		if forDataType == DataTypeInteger {
			return stddevInt, nil
		}
		return stddevDecimal, nil

	case ReductionTypeVariance:
		if forDataType == DataTypeInteger {
			return varianceInt, nil
		}
		return varianceDecimal, nil

	case ReductionTypeSpread:
		if forDataType == DataTypeInteger {
			return spreadInt, nil
		}
		return spreadDecimal, nil

	case ReductionTypeP50:
		return percentileReducerFunc(forDataType, 0.50), nil
	case ReductionTypeP75:
		return percentileReducerFunc(forDataType, 0.75), nil
	case ReductionTypeP90:
		return percentileReducerFunc(forDataType, 0.90), nil
	case ReductionTypeP95:
		return percentileReducerFunc(forDataType, 0.95), nil
	case ReductionTypeP99:
		return percentileReducerFunc(forDataType, 0.99), nil
	case ReductionTypeP999:
		return percentileReducerFunc(forDataType, 0.999), nil

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
	case ReductionTypeStddev:
		if forDataType == DataTypeInteger {
			return &stddevIntAccumulator{}, nil
		}
		return &stddevDecimalAccumulator{}, nil
	case ReductionTypeVariance:
		if forDataType == DataTypeInteger {
			return &varianceIntAccumulator{}, nil
		}
		return &varianceDecimalAccumulator{}, nil
	case ReductionTypeSpread:
		if forDataType == DataTypeInteger {
			return &spreadIntAccumulator{}, nil
		}
		return &spreadDecimalAccumulator{}, nil
	case ReductionTypeP50:
		return newPercentileAccumulator(forDataType, 0.50), nil
	case ReductionTypeP75:
		return newPercentileAccumulator(forDataType, 0.75), nil
	case ReductionTypeP90:
		return newPercentileAccumulator(forDataType, 0.90), nil
	case ReductionTypeP95:
		return newPercentileAccumulator(forDataType, 0.95), nil
	case ReductionTypeP99:
		return newPercentileAccumulator(forDataType, 0.99), nil
	case ReductionTypeP999:
		return newPercentileAccumulator(forDataType, 0.999), nil
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

// --- Variance / Stddev reducer functions (Welford's algorithm) ---

func varianceInt(values []any) any {
	var mean, m2 float64
	for i, v := range values {
		x := float64(v.(int64))
		delta := x - mean
		mean += delta / float64(i+1)
		delta2 := x - mean
		m2 += delta * delta2
	}
	return m2 / float64(len(values))
}

func varianceDecimal(values []any) any {
	var mean, m2 float64
	for i, v := range values {
		x := v.(float64)
		delta := x - mean
		mean += delta / float64(i+1)
		delta2 := x - mean
		m2 += delta * delta2
	}
	return m2 / float64(len(values))
}

func stddevInt(values []any) any {
	return math.Sqrt(varianceInt(values).(float64))
}

func stddevDecimal(values []any) any {
	return math.Sqrt(varianceDecimal(values).(float64))
}

// --- Spread reducer functions ---

func spreadInt(values []any) any {
	minVal := values[0].(int64)
	maxVal := minVal
	for i := 1; i < len(values); i++ {
		v := values[i].(int64)
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return maxVal - minVal
}

func spreadDecimal(values []any) any {
	minVal := values[0].(float64)
	maxVal := minVal
	for i := 1; i < len(values); i++ {
		v := values[i].(float64)
		if v < minVal {
			minVal = v
		}
		if v > maxVal {
			maxVal = v
		}
	}
	return maxVal - minVal
}

// --- Percentile reducer functions (linear interpolation, method 7) ---

func percentileReducerFunc(forDataType DataType, phi float64) func([]any) any {
	if forDataType == DataTypeInteger {
		return func(values []any) any {
			return percentileInt(values, phi)
		}
	}
	return func(values []any) any {
		return percentileDecimal(values, phi)
	}
}

func percentileInt(values []any, phi float64) any {
	n := len(values)
	sorted := make([]float64, n)
	for i, v := range values {
		sorted[i] = float64(v.(int64))
	}
	sort.Float64s(sorted)
	return interpolatePercentile(sorted, phi)
}

func percentileDecimal(values []any, phi float64) any {
	n := len(values)
	sorted := make([]float64, n)
	for i, v := range values {
		sorted[i] = v.(float64)
	}
	sort.Float64s(sorted)
	return interpolatePercentile(sorted, phi)
}

// interpolatePercentile computes a percentile using linear interpolation (numpy default / method 7).
// The virtual index is: idx = phi * (n - 1). We interpolate between sorted[floor(idx)] and sorted[ceil(idx)].
func interpolatePercentile(sorted []float64, phi float64) float64 {
	n := len(sorted)
	if n == 1 {
		return sorted[0]
	}
	idx := phi * float64(n-1)
	lo := int(math.Floor(idx))
	hi := int(math.Ceil(idx))
	if lo == hi {
		return sorted[lo]
	}
	frac := idx - float64(lo)
	return sorted[lo] + frac*(sorted[hi]-sorted[lo])
}

// --- Variance accumulator (Welford's online algorithm, population variance) ---

type varianceIntAccumulator struct {
	count int64
	mean  float64
	m2    float64
}

func (a *varianceIntAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.count++
	x := float64(value.(int64))
	delta := x - a.mean
	a.mean += delta / float64(a.count)
	delta2 := x - a.mean
	a.m2 += delta * delta2
}

func (a *varianceIntAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return a.m2 / float64(a.count)
}

func (a *varianceIntAccumulator) ResultTimestamp() *time.Time { return nil }

type varianceDecimalAccumulator struct {
	count int64
	mean  float64
	m2    float64
}

func (a *varianceDecimalAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.count++
	x := value.(float64)
	delta := x - a.mean
	a.mean += delta / float64(a.count)
	delta2 := x - a.mean
	a.m2 += delta * delta2
}

func (a *varianceDecimalAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return a.m2 / float64(a.count)
}

func (a *varianceDecimalAccumulator) ResultTimestamp() *time.Time { return nil }

// --- Stddev accumulator (sqrt of variance) ---

type stddevIntAccumulator struct {
	varianceIntAccumulator
}

func (a *stddevIntAccumulator) Result() any {
	v := a.varianceIntAccumulator.Result()
	if v == nil {
		return nil
	}
	return math.Sqrt(v.(float64))
}

type stddevDecimalAccumulator struct {
	varianceDecimalAccumulator
}

func (a *stddevDecimalAccumulator) Result() any {
	v := a.varianceDecimalAccumulator.Result()
	if v == nil {
		return nil
	}
	return math.Sqrt(v.(float64))
}

// --- Spread accumulator (max - min) ---

type spreadIntAccumulator struct {
	min    int64
	max    int64
	hasAny bool
}

func (a *spreadIntAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	v := value.(int64)
	if !a.hasAny {
		a.min = v
		a.max = v
		a.hasAny = true
	} else {
		if v < a.min {
			a.min = v
		}
		if v > a.max {
			a.max = v
		}
	}
}

func (a *spreadIntAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.max - a.min
}

func (a *spreadIntAccumulator) ResultTimestamp() *time.Time { return nil }

type spreadDecimalAccumulator struct {
	min    float64
	max    float64
	hasAny bool
}

func (a *spreadDecimalAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	v := value.(float64)
	if !a.hasAny {
		a.min = v
		a.max = v
		a.hasAny = true
	} else {
		if v < a.min {
			a.min = v
		}
		if v > a.max {
			a.max = v
		}
	}
}

func (a *spreadDecimalAccumulator) Result() any {
	if !a.hasAny {
		return nil
	}
	return a.max - a.min
}

func (a *spreadDecimalAccumulator) ResultTimestamp() *time.Time { return nil }

// --- Percentile accumulator (streaming t-digest, O(1) amortized add) ---

func newPercentileAccumulator(forDataType DataType, phi float64) Accumulator {
	if forDataType == DataTypeInteger {
		return &percentileIntAccumulator{phi: phi, td: tdigest.New(100)}
	}
	return &percentileDecimalAccumulator{phi: phi, td: tdigest.New(100)}
}

type percentileIntAccumulator struct {
	phi float64
	td  *tdigest.TDigest
}

func (a *percentileIntAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.td.Add(float64(value.(int64)))
}

func (a *percentileIntAccumulator) Result() any {
	if a.td.Count() == 0 {
		return nil
	}
	return a.td.Quantile(a.phi)
}

func (a *percentileIntAccumulator) ResultTimestamp() *time.Time { return nil }

type percentileDecimalAccumulator struct {
	phi float64
	td  *tdigest.TDigest
}

func (a *percentileDecimalAccumulator) Add(value any, _ time.Time) {
	if value == nil {
		return
	}
	a.td.Add(value.(float64))
}

func (a *percentileDecimalAccumulator) Result() any {
	if a.td.Count() == 0 {
		return nil
	}
	return a.td.Quantile(a.phi)
}

func (a *percentileDecimalAccumulator) ResultTimestamp() *time.Time { return nil }
