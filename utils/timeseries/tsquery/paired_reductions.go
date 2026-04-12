package tsquery

import (
	"fmt"
	"math"
	"time"
)

// PairedAccumulator provides streaming aggregation for paired (two-field) reductions.
// It processes (actual, predicted) value pairs and produces a scalar error metric.
type PairedAccumulator interface {
	// AddPair feeds an (actual, predicted) value pair and its timestamp.
	// If either value is nil, the pair is skipped.
	AddPair(actual any, predicted any, ts time.Time)
	// Result returns the aggregated scalar value, or nil if no valid pairs were added.
	Result() any
	// ResultTimestamp returns nil — paired reductions produce aggregate statistics
	// not tied to a single timestamp.
	ResultTimestamp() *time.Time
}

// NewPairedAccumulator creates a streaming paired accumulator for this reduction type.
// Returns an error if the reduction type is not a paired reduction.
func (reductionType ReductionType) NewPairedAccumulator() (PairedAccumulator, error) {
	switch reductionType {
	case ReductionTypeMAE:
		return &maeAccumulator{}, nil
	case ReductionTypeRMSE:
		return &rmseAccumulator{}, nil
	case ReductionTypeMBE:
		return &mbeAccumulator{}, nil
	case ReductionTypeMAPE:
		return &mapeAccumulator{}, nil
	case ReductionTypePearson:
		return &pearsonAccumulator{}, nil
	case ReductionTypeR2:
		return &r2Accumulator{}, nil
	default:
		return nil, fmt.Errorf("reduction type %q is not a paired reduction", reductionType)
	}
}

// pairedToFloat64 converts an int64 or float64 value to float64.
// Returns (0, false) for nil or unsupported types.
func pairedToFloat64(val any) (float64, bool) {
	switch v := val.(type) {
	case float64:
		return v, true
	case int64:
		return float64(v), true
	default:
		return 0, false
	}
}

// --- MAE accumulator: mean(|actual − predicted|) ---

type maeAccumulator struct {
	sumAbsErr float64
	count     int64
}

func (a *maeAccumulator) AddPair(actual any, predicted any, _ time.Time) {
	act, okA := pairedToFloat64(actual)
	pred, okP := pairedToFloat64(predicted)
	if !okA || !okP {
		return
	}
	a.sumAbsErr += math.Abs(act - pred)
	a.count++
}

func (a *maeAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return a.sumAbsErr / float64(a.count)
}

func (a *maeAccumulator) ResultTimestamp() *time.Time { return nil }

// --- RMSE accumulator: √mean((actual − predicted)²) ---

type rmseAccumulator struct {
	sumSqErr float64
	count    int64
}

func (a *rmseAccumulator) AddPair(actual any, predicted any, _ time.Time) {
	act, okA := pairedToFloat64(actual)
	pred, okP := pairedToFloat64(predicted)
	if !okA || !okP {
		return
	}
	diff := act - pred
	a.sumSqErr += diff * diff
	a.count++
}

func (a *rmseAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return math.Sqrt(a.sumSqErr / float64(a.count))
}

func (a *rmseAccumulator) ResultTimestamp() *time.Time { return nil }

// --- MBE accumulator: mean(actual − predicted) ---

type mbeAccumulator struct {
	sumErr float64
	count  int64
}

func (a *mbeAccumulator) AddPair(actual any, predicted any, _ time.Time) {
	act, okA := pairedToFloat64(actual)
	pred, okP := pairedToFloat64(predicted)
	if !okA || !okP {
		return
	}
	a.sumErr += act - pred
	a.count++
}

func (a *mbeAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return a.sumErr / float64(a.count)
}

func (a *mbeAccumulator) ResultTimestamp() *time.Time { return nil }

// --- MAPE accumulator: mean(|actual − predicted| / |actual|) × 100 ---
// Rows where actual = 0 are skipped (MAPE is undefined).

type mapeAccumulator struct {
	sumPctErr float64
	count     int64
}

func (a *mapeAccumulator) AddPair(actual any, predicted any, _ time.Time) {
	act, okA := pairedToFloat64(actual)
	pred, okP := pairedToFloat64(predicted)
	if !okA || !okP {
		return
	}
	if act == 0 {
		return // skip: MAPE undefined when actual is zero
	}
	a.sumPctErr += math.Abs((act - pred) / act)
	a.count++
}

func (a *mapeAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return (a.sumPctErr / float64(a.count)) * 100.0
}

func (a *mapeAccumulator) ResultTimestamp() *time.Time { return nil }

// --- Pearson accumulator: Pearson correlation coefficient ---
// Uses Welford-like online algorithm for numerical stability (same pattern as varianceAccumulator).
// Maintains running means and co-moment instead of raw sums to avoid catastrophic cancellation.
// Returns nil when n < 2 or when either variable has zero variance.

type pearsonAccumulator struct {
	n            int64
	meanX, meanY float64
	m2X, m2Y     float64 // Welford's M2: Σ(xi − meanX)², Σ(yi − meanY)²
	cXY          float64 // co-moment: Σ(xi − meanX)(yi − meanY)
}

func (a *pearsonAccumulator) AddPair(actual any, predicted any, _ time.Time) {
	x, okA := pairedToFloat64(actual)
	y, okP := pairedToFloat64(predicted)
	if !okA || !okP {
		return
	}
	a.n++
	n := float64(a.n)
	dx := x - a.meanX
	a.meanX += dx / n
	dy := y - a.meanY
	a.meanY += dy / n
	// Welford update: use new meanX with old dx, new meanY with old dy
	a.m2X += dx * (x - a.meanX)
	a.m2Y += dy * (y - a.meanY)
	a.cXY += dx * (y - a.meanY)
}

func (a *pearsonAccumulator) Result() any {
	if a.n < 2 {
		return nil
	}
	denom := a.m2X * a.m2Y
	if denom <= 0 {
		return nil // zero variance in one or both variables
	}
	return a.cXY / math.Sqrt(denom)
}

func (a *pearsonAccumulator) ResultTimestamp() *time.Time { return nil }

// --- R² accumulator: coefficient of determination ---
// R² = 1 − SS_res / SS_tot where:
//   SS_res = Σ(actual − predicted)²
//   SS_tot = Σ(actual − mean(actual))²  (computed via Welford for numerical stability)
// Returns nil when n < 2 or SS_tot = 0 (constant actuals).

type r2Accumulator struct {
	n             int64
	meanActual    float64 // running mean of actuals (Welford)
	m2Actual      float64 // Welford's M2 for SS_tot: Σ(actual − meanActual)²
	sumResidualSq float64 // SS_res: Σ(actual − predicted)²
}

func (a *r2Accumulator) AddPair(actual any, predicted any, _ time.Time) {
	act, okA := pairedToFloat64(actual)
	pred, okP := pairedToFloat64(predicted)
	if !okA || !okP {
		return
	}
	a.n++
	dx := act - a.meanActual
	a.meanActual += dx / float64(a.n)
	a.m2Actual += dx * (act - a.meanActual) // Welford update for SS_tot
	diff := act - pred
	a.sumResidualSq += diff * diff
}

func (a *r2Accumulator) Result() any {
	if a.n < 2 {
		return nil
	}
	if a.m2Actual == 0 {
		return nil // constant actuals — R² is undefined
	}
	return 1.0 - a.sumResidualSq/a.m2Actual
}

func (a *r2Accumulator) ResultTimestamp() *time.Time { return nil }
