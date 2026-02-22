package datasource

// counterDeltaFunc returns the delta between two consecutive counter values,
// handling drops (negative current) and resets (current < previous).
// Returns (delta, shouldEmit). shouldEmit=false means drop the point.
type counterDeltaFunc func(currVal, prevVal float64) (delta float64, emit bool)

func newNonNegativeCounterDeltaFunc(maxCounterValue float64) counterDeltaFunc {
	if maxCounterValue > 0 {
		return func(currVal, prevVal float64) (float64, bool) {
			if currVal < 0 {
				return 0, false
			}
			if currVal < prevVal {
				return (maxCounterValue - prevVal) + currVal, true
			}
			return currVal - prevVal, true
		}
	}
	return func(currVal, prevVal float64) (float64, bool) {
		if currVal < 0 {
			return 0, false
		}
		if currVal < prevVal {
			return currVal, true
		}
		return currVal - prevVal, true
	}
}
