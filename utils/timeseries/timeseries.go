package timeseries

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
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

func mapRecordValue[T any](r TsRecord[T]) T {
	return r.Value
}
func recordMapper[T any](t time.Time) func(T) TsRecord[T] {
	return func(v T) TsRecord[T] {
		return TsRecord[T]{
			Timestamp: t,
			Value:     v,
		}
	}
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

// timeWeightedAverageArr computes the time-weighted average of two values (v1Arr and v2Arr) erroring if the values are not numeric
func timeWeightedAverageArr(targetTime, v1Time time.Time, v1Arr []any, v2Time time.Time, v2Arr []any) ([]any, error) {
	if v1Time.Equal(v2Time) {
		if v1Time == targetTime {
			return v1Arr, nil
		}
		return nil, fmt.Errorf("v1Time and v2Time are the same: %s. targetTime:%s", v1Time, targetTime)
	}

	// Ensure targetTime is between v1Time and v2Time
	if targetTime.Before(v1Time) || targetTime.After(v2Time) {
		return nil, fmt.Errorf("targetTime %s is out of bounds (%s to %s)", targetTime, v1Time, v2Time)
	}

	// Calculate the interpolation factor
	totalDuration := v2Time.Sub(v1Time).Seconds()
	interpolatedDuration := targetTime.Sub(v1Time).Seconds()
	weight := interpolatedDuration / totalDuration

	res := make([]any, len(v1Arr))
	for i, _ := range v1Arr {
		v1, err := util.AnyToFloat64(v1Arr[i])
		if err != nil {
			return nil, err
		}
		v2, err := util.AnyToFloat64(v2Arr[i])
		if err != nil {
			return nil, err
		}
		// Perform the weighted average
		res[i] = v1 + (v2-v1)*weight
	}

	return res, nil
}
