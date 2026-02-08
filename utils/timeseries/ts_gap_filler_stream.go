package timeseries

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"io"
	"time"
)

// NewTsGapFillerStream wraps a sparse aligned stream and fills gaps between aligned points.
// It emits a point for every alignment period between the first and last data points.
// interpolateFn is called for FillModeLinear to compute the interpolated value between two points.
// copyFn is called for FillModeForwardFill to copy the previous value (avoiding aliasing for reference types).
func NewTsGapFillerStream[T any](
	sparseStream stream.Stream[TsRecord[T]],
	ap AlignmentPeriod,
	fillMode FillMode,
	interpolateFn func(targetTime, v1Time time.Time, v1 T, v2Time time.Time, v2 T) (T, error),
	copyFn func(v T) T,
) stream.Stream[TsRecord[T]] {
	var (
		prevPoint   *TsRecord[T]
		nextPoint   *TsRecord[T]
		expectedTs  time.Time
		initialized bool
		exhausted   bool
	)

	return stream.NewDownStreamSimple(
		sparseStream,
		func(ctx context.Context, srcProvider stream.ProviderFunc[TsRecord[T]]) (TsRecord[T], error) {
			// Initialize on first call: pull the first aligned point
			if !initialized {
				first, err := srcProvider(ctx)
				if err != nil {
					return util.DefaultValue[TsRecord[T]](), err
				}
				nextPoint = &TsRecord[T]{Value: first.Value, Timestamp: first.Timestamp}
				expectedTs = first.Timestamp
				initialized = true
			}

			for {
				if exhausted {
					return util.DefaultValue[TsRecord[T]](), io.EOF
				}

				// Advance: consume aligned points whose timestamp <= expectedTs
				for nextPoint != nil && !nextPoint.Timestamp.After(expectedTs) {
					prevPoint = nextPoint
					nxt, err := srcProvider(ctx)
					if err != nil {
						if err == io.EOF {
							nextPoint = nil
						} else {
							return util.DefaultValue[TsRecord[T]](), err
						}
					} else {
						nextPoint = &TsRecord[T]{Value: nxt.Value, Timestamp: nxt.Timestamp}
					}
				}

				// Exact match: prevPoint is at expectedTs
				if prevPoint != nil && prevPoint.Timestamp.Equal(expectedTs) {
					result := TsRecord[T]{Value: prevPoint.Value, Timestamp: expectedTs}
					expectedTs = ap.GetEndTime(expectedTs)
					// If source is exhausted and we've passed the last point, mark exhausted for next call
					if nextPoint == nil {
						exhausted = true
					}
					return result, nil
				}

				// Data exhausted & expectedTs > last aligned point -> EOF
				if nextPoint == nil {
					return util.DefaultValue[TsRecord[T]](), io.EOF
				}

				// Between two points: interpolate or forward-fill
				if prevPoint != nil && nextPoint != nil {
					var value T
					var err error
					switch fillMode {
					case FillModeLinear:
						value, err = interpolateFn(
							expectedTs,
							prevPoint.Timestamp,
							prevPoint.Value,
							nextPoint.Timestamp,
							nextPoint.Value,
						)
						if err != nil {
							return util.DefaultValue[TsRecord[T]](), fmt.Errorf("error interpolating gap-fill value: %w", err)
						}
					case FillModeForwardFill:
						value = copyFn(prevPoint.Value)
					default:
						return util.DefaultValue[TsRecord[T]](), fmt.Errorf("unsupported fill mode: %s", fillMode)
					}
					result := TsRecord[T]{Value: value, Timestamp: expectedTs}
					expectedTs = ap.GetEndTime(expectedTs)
					return result, nil
				}

				// Should not reach here, but safety
				return util.DefaultValue[TsRecord[T]](), io.EOF
			}
		},
		nil, // no open func
		nil, // no close func
	)
}
