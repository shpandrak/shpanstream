package timeseries

import (
	"fmt"
	"time"
)

type Number interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr |
		~float32 | ~float64
}

type TsRecord[T any] struct {
	Timestamp time.Time `json:"timestamp"`
	Value     T         `json:"value,omitempty"`
}

// timeWeightedAverage computes the time-weighted average of two values (v1 and v2) at their respective times (v1Time and v2Time).
func timeWeightedAverage[N Number](targetTime, v1Time time.Time, v1 N, v2Time time.Time, v2 N) (N, error) {
	if v1Time.Equal(v2Time) {
		if v1Time == targetTime {
			return v1, nil
		}
		return 0, fmt.Errorf("v1Time and v2Time are the same: %s. targetTime:%s", v1Time, targetTime)
	}

	// Ensure targetTime is between v1Time and v2Time
	if targetTime.Before(v1Time) || targetTime.After(v2Time) {
		return 0, fmt.Errorf("targetTime %s is out of bounds (%s to %s)", targetTime, v1Time, v2Time)
	}

	// Calculate the interpolation factor
	totalDuration := v2Time.Sub(v1Time).Seconds()
	interpolatedDuration := targetTime.Sub(v1Time).Seconds()
	weight := interpolatedDuration / totalDuration

	// Perform the weighted average
	res := float64(v1) + (float64(v2)-float64(v1))*weight

	return N(res), nil
}
