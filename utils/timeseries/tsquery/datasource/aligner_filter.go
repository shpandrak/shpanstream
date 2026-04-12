package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

var _ Filter = AlignerFilter{}

type AlignerFilter struct {
	alignmentPeriod timeseries.AlignmentPeriod
	fillMode        *timeseries.FillMode
	bucketReduction *tsquery.ReductionType
}

func NewAlignerFilter(alignmentPeriod timeseries.AlignmentPeriod) AlignerFilter {
	return AlignerFilter{alignmentPeriod: alignmentPeriod}
}

func NewInterpolatingAlignerFilter(alignmentPeriod timeseries.AlignmentPeriod, fillMode timeseries.FillMode) AlignerFilter {
	return AlignerFilter{alignmentPeriod: alignmentPeriod, fillMode: &fillMode}
}

func (af AlignerFilter) AlignmentPeriod() timeseries.AlignmentPeriod {
	return af.alignmentPeriod
}

func (af AlignerFilter) FillMode() *timeseries.FillMode {
	return af.fillMode
}

func (af AlignerFilter) WithBucketReduction(rt tsquery.ReductionType) AlignerFilter {
	af.bucketReduction = &rt
	return af
}

func (af AlignerFilter) BucketReduction() *tsquery.ReductionType {
	return af.bucketReduction
}

func (af AlignerFilter) Filter(_ context.Context, result Result) (Result, error) {
	isNumeric := result.meta.DataType().IsNumeric()

	// Kind-aware default bucket reduction: when no explicit bucketReduction is set,
	// choose default based on MetricKind. Gauge/Rate use the existing interpolation path.
	if af.bucketReduction == nil {
		switch result.meta.MetricKind() {
		case tsquery.MetricKindDelta:
			sum := tsquery.ReductionTypeSum
			af.bucketReduction = &sum
		case tsquery.MetricKindCumulative:
			last := tsquery.ReductionTypeLast
			af.bucketReduction = &last
		}
	}

	// Validate bucket reduction compatibility (must come before fill mode check)
	if af.bucketReduction != nil && af.bucketReduction.RequiresNumeric() && !isNumeric {
		return util.DefaultValue[Result](), fmt.Errorf(
			"aligner filter with bucket reduction %q requires numeric data type, got: %s",
			*af.bucketReduction, result.meta.DataType(),
		)
	}

	// Determine effective output type — bucket reduction may change it
	effectiveDataType := result.meta.DataType()
	if af.bucketReduction != nil {
		effectiveDataType = af.bucketReduction.GetResultDataType(result.meta.DataType())
	}

	// Fill mode requires numeric OUTPUT type (after reduction)
	if af.fillMode != nil && !effectiveDataType.IsNumeric() {
		return util.DefaultValue[Result](), fmt.Errorf(
			"aligner filter with fill mode can only be applied to numeric data types, got: %s",
			effectiveDataType,
		)
	}

	// Determine output metadata — bucket reduction may change the data type
	resultMeta := result.meta
	if af.bucketReduction != nil {
		resultDataType := af.bucketReduction.GetResultDataType(result.meta.DataType())
		if resultDataType != result.meta.DataType() {
			newMeta, err := tsquery.NewFieldMetaFull(
				result.meta.Urn(),
				resultDataType,
				result.meta.MetricKind(),
				result.meta.Required(),
				result.meta.Unit(),
				result.meta.CustomMeta(),
			)
			if err != nil {
				return util.DefaultValue[Result](), fmt.Errorf("failed to create new field meta for bucket reduction: %w", err)
			}
			resultMeta = *newMeta
		}
	}

	var alignedStream stream.Stream[timeseries.TsRecord[any]]

	if af.bucketReduction != nil {
		// Bucket reduction mode: aggregate all values within each bucket using an Accumulator
		inputDataType := result.meta.DataType()
		reductionType := *af.bucketReduction

		alignedStream = stream.ClusterSortedStreamComparable[timeseries.TsRecord[any], timeseries.TsRecord[any], time.Time](
			func(
				ctx context.Context,
				clusterTimestampClassifier time.Time,
				clusterStream stream.Stream[timeseries.TsRecord[any]],
				_ *timeseries.TsRecord[any],
			) (timeseries.TsRecord[any], error) {
				acc, err := reductionType.NewAccumulator(inputDataType)
				if err != nil {
					return util.DefaultValue[timeseries.TsRecord[any]](), fmt.Errorf("failed to create accumulator for bucket reduction: %w", err)
				}

				err = clusterStream.ConsumeWithErr(ctx, func(item timeseries.TsRecord[any]) error {
					acc.Add(item.Value, item.Timestamp)
					return nil
				})
				if err != nil {
					return util.DefaultValue[timeseries.TsRecord[any]](), err
				}

				accResult := acc.Result()
				if accResult == nil {
					// Defensive: cluster stream always has at least one item, so this should not happen
					return util.DefaultValue[timeseries.TsRecord[any]](), fmt.Errorf("bucket reduction %q produced no result for bucket at %s", reductionType, clusterTimestampClassifier)
				}

				return timeseries.TsRecord[any]{
					Value:     accResult,
					Timestamp: clusterTimestampClassifier,
				}, nil
			},
			alignmentPeriodClassifierFunc[any](af.alignmentPeriod),
			result.data,
		)
	} else {
		// Default interpolation mode
		alignedStream = stream.ClusterSortedStreamComparable[timeseries.TsRecord[any], timeseries.TsRecord[any], time.Time](
			func(
				ctx context.Context,
				clusterTimestampClassifier time.Time,
				clusterStream stream.Stream[timeseries.TsRecord[any]],
				lastItemOnPreviousCluster *timeseries.TsRecord[any],
			) (timeseries.TsRecord[any], error) {

				firstItem, err := clusterStream.FindFirst().Get(ctx)
				if err != nil {
					return util.DefaultValue[timeseries.TsRecord[any]](), err
				}

				// If this is the first cluster, smudge the first item to the start of the cluster
				if lastItemOnPreviousCluster == nil {
					return timeseries.TsRecord[any]{
						Value:     firstItem.Value,
						Timestamp: clusterTimestampClassifier,
					}, nil
				} else {
					// If this is not the first cluster

					// Check if the first item is magically aligned to the slot, return it
					if firstItem.Timestamp == clusterTimestampClassifier {
						return timeseries.TsRecord[any]{
							Value:     firstItem.Value,
							Timestamp: clusterTimestampClassifier,
						}, nil
					} else if !isNumeric {
						// For non-numeric types, use the nearest value (step function)
						// instead of interpolation
						return timeseries.TsRecord[any]{
							Value:     firstItem.Value,
							Timestamp: clusterTimestampClassifier,
						}, nil
					} else {
						// If not, we need to calculate the time-weighted average
						avgItem, err := timeWeightedAverage(
							result.meta,
							clusterTimestampClassifier,
							lastItemOnPreviousCluster.Timestamp,
							lastItemOnPreviousCluster.Value,
							firstItem.Timestamp,
							firstItem.Value,
						)
						if err != nil {
							return util.DefaultValue[timeseries.TsRecord[any]](), fmt.Errorf("error calculating time weighted average while aliging streams: %w", err)
						}
						return timeseries.TsRecord[any]{
							Value:     avgItem,
							Timestamp: clusterTimestampClassifier,
						}, nil
					}
				}
			},
			alignmentPeriodClassifierFunc[any](af.alignmentPeriod),
			result.data,
		)
	}

	// Derive output samplePeriod from alignment period (the output cadence is now the alignment period)
	if fixedPeriod, ok := af.alignmentPeriod.(timeseries.FixedAlignmentPeriod); ok {
		resultMeta = resultMeta.WithSamplePeriod(fixedPeriod.Duration)
	}

	// If fillMode is set, wrap the sparse aligned stream with a gap-filler
	if af.fillMode != nil {
		fieldMeta := resultMeta
		alignedStream = timeseries.NewTsGapFillerStream[any](
			alignedStream,
			af.alignmentPeriod,
			*af.fillMode,
			func(targetTime, v1Time time.Time, v1 any, v2Time time.Time, v2 any) (any, error) {
				return timeWeightedAverage(fieldMeta, targetTime, v1Time, v1, v2Time, v2)
			},
			func(v any) any { return v },
		)
	}

	return Result{
		meta: resultMeta,
		data: alignedStream,
	}, nil

}

func alignmentPeriodClassifierFunc[T any](ap timeseries.AlignmentPeriod) func(a timeseries.TsRecord[T]) time.Time {
	return func(a timeseries.TsRecord[T]) time.Time { return ap.GetStartTime(a.Timestamp) }
}

// timeWeightedAverageArr computes the time-weighted average of two values (v1Arr and v2Arr) erroring if the values are not numeric
func timeWeightedAverage(fieldMeta tsquery.FieldMeta, targetTime, v1Time time.Time, v1 any, v2Time time.Time, v2 any) (any, error) {
	if v1Time.Equal(v2Time) {
		if v1Time == targetTime {
			return v1, nil
		}
		return nil, fmt.Errorf("v1Time and v2Time are the same: %s. targetTime:%s", v1Time, targetTime)
	}

	// Ensure the targetTime is between v1Time and v2Time
	if targetTime.Before(v1Time) || targetTime.After(v2Time) {
		return nil, fmt.Errorf("targetTime %s is out of bounds (%s to %s)", targetTime, v1Time, v2Time)
	}

	// Calculate the interpolation factor
	totalDuration := v2Time.Sub(v1Time).Seconds()
	interpolatedDuration := targetTime.Sub(v1Time).Seconds()
	weight := interpolatedDuration / totalDuration

	dt := fieldMeta.DataType()
	v1Float, err := dt.ToFloat64(v1)
	if err != nil {
		return nil, fmt.Errorf("error converting v1 to float64 for weighted average: %w", err)
	}
	v2Float, err := dt.ToFloat64(v2)
	if err != nil {
		return nil, fmt.Errorf("error converting v2 to float64 for weighted average: %w", err)
	}
	// Perform the weighted average
	retVal, err := dt.FromFloat64(v1Float + (v2Float-v1Float)*weight)
	if err != nil {
		return nil, fmt.Errorf("failed to convert cast weighted average value back to datatype values when calculating weighted average: %w", err)
	}
	return retVal, nil

}
