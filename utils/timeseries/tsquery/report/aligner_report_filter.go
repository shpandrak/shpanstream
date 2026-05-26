package report

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

// FieldAlignment holds per-field overrides for the report aligner, keyed externally by field URN.
// Both members are optional and independent:
//
//   - Reduction overrides the MetricKind-derived bucket reduction for the field. When nil, the
//     field's MetricKind decides the default: Delta → Sum, every other kind → interpolate to
//     bucket-start.
//   - FillMode overrides the aligner-level fill mode for the field during gap filling. It is only
//     meaningful when the aligner has fill enabled (NewInterpolatingAlignerFilter); otherwise
//     Filter returns an error. When nil, the field uses the aligner-level fill mode.
type FieldAlignment struct {
	Reduction *tsquery.ReductionType
	FillMode  *timeseries.FillMode
}

type AlignerFilter struct {
	alignmentPeriod timeseries.AlignmentPeriod
	fillMode        *timeseries.FillMode
	fieldAlignments map[string]FieldAlignment
}

func NewAlignerFilter(alignmentPeriod timeseries.AlignmentPeriod) AlignerFilter {
	return AlignerFilter{alignmentPeriod: alignmentPeriod}
}

func NewInterpolatingAlignerFilter(alignmentPeriod timeseries.AlignmentPeriod, fillMode timeseries.FillMode) AlignerFilter {
	return AlignerFilter{alignmentPeriod: alignmentPeriod, fillMode: &fillMode}
}

// WithFieldReduction returns a copy of the filter with an explicit bucket reduction for the field
// identified by urn, overriding the MetricKind-derived default.
func (af AlignerFilter) WithFieldReduction(urn string, rt tsquery.ReductionType) AlignerFilter {
	af.fieldAlignments = cloneFieldAlignments(af.fieldAlignments)
	fa := af.fieldAlignments[urn]
	fa.Reduction = &rt
	af.fieldAlignments[urn] = fa
	return af
}

// WithFieldFillMode returns a copy of the filter with an explicit gap-fill mode for the field
// identified by urn, overriding the aligner-level fill mode. Requires fill to be enabled on the
// aligner (NewInterpolatingAlignerFilter); Filter errors otherwise.
func (af AlignerFilter) WithFieldFillMode(urn string, fm timeseries.FillMode) AlignerFilter {
	af.fieldAlignments = cloneFieldAlignments(af.fieldAlignments)
	fa := af.fieldAlignments[urn]
	fa.FillMode = &fm
	af.fieldAlignments[urn] = fa
	return af
}

func cloneFieldAlignments(in map[string]FieldAlignment) map[string]FieldAlignment {
	out := make(map[string]FieldAlignment, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (af AlignerFilter) Filter(_ context.Context, result Result) (Result, error) {
	fieldsMeta := result.FieldsMeta()

	for _, fieldMeta := range fieldsMeta {
		if !fieldMeta.DataType().IsNumeric() {
			return util.DefaultValue[Result](), fmt.Errorf(
				"aligner filter can only be applied to numeric data types, got: %s",
				fieldMeta.DataType(),
			)
		}
	}

	if err := af.validateFieldOverrides(fieldsMeta); err != nil {
		return util.DefaultValue[Result](), err
	}

	reductions, err := af.resolveReductions(fieldsMeta)
	if err != nil {
		return util.DefaultValue[Result](), err
	}

	outFieldsMeta, err := outputFieldsMeta(fieldsMeta, reductions)
	if err != nil {
		return util.DefaultValue[Result](), err
	}

	var s stream.Stream[timeseries.TsRecord[[]any]]
	if anyReduction(reductions) {
		s = af.alignWithReductions(result.Stream(), fieldsMeta, reductions)
	} else {
		s = af.alignByInterpolation(result.Stream(), fieldsMeta)
	}

	if af.fillMode != nil {
		s = af.fillGaps(s, outFieldsMeta)
	}

	return NewResult(outFieldsMeta, s), nil
}

// validateFieldOverrides checks that every per-field override targets a known field URN, and that
// a per-field fill-mode override is only used when the aligner has fill enabled and names a valid
// fill mode.
func (af AlignerFilter) validateFieldOverrides(fieldsMeta []tsquery.FieldMeta) error {
	knownUrn := make(map[string]struct{}, len(fieldsMeta))
	for _, fm := range fieldsMeta {
		knownUrn[fm.Urn()] = struct{}{}
	}
	for urn, fa := range af.fieldAlignments {
		if _, ok := knownUrn[urn]; !ok {
			return fmt.Errorf("aligner field override references unknown field urn %q", urn)
		}
		if fa.FillMode != nil {
			if af.fillMode == nil {
				return fmt.Errorf(
					"aligner per-field fill mode override for %q requires fill to be enabled on the aligner", urn)
			}
			if err := fa.FillMode.Validate(); err != nil {
				return fmt.Errorf("aligner field %q: %w", urn, err)
			}
		}
	}
	return nil
}

// resolveReductions computes the effective bucket reduction for each field, by index:
// explicit override > MetricKind default (Delta → Sum) > nil (interpolate to bucket-start).
// A nil entry means the field is aligned by interpolation rather than reduced.
func (af AlignerFilter) resolveReductions(fieldsMeta []tsquery.FieldMeta) ([]*tsquery.ReductionType, error) {
	reductions := make([]*tsquery.ReductionType, len(fieldsMeta))
	for i, fm := range fieldsMeta {
		var rt *tsquery.ReductionType
		if fa, ok := af.fieldAlignments[fm.Urn()]; ok && fa.Reduction != nil {
			rt = fa.Reduction
		} else if fm.MetricKind() == tsquery.MetricKindDelta {
			sum := tsquery.ReductionTypeSum
			rt = &sum
		}
		if rt != nil {
			if err := rt.Validate(); err != nil {
				return nil, fmt.Errorf("aligner field %q: %w", fm.Urn(), err)
			}
			if rt.IsPaired() {
				return nil, fmt.Errorf(
					"aligner field %q: paired reduction %q is not supported (requires a compare field)", fm.Urn(), *rt)
			}
			if rt.RequiresNumeric() && !fm.DataType().IsNumeric() {
				return nil, fmt.Errorf(
					"aligner field %q: reduction %q requires numeric data type, got %s", fm.Urn(), *rt, fm.DataType())
			}
		}
		reductions[i] = rt
	}
	return reductions, nil
}

func anyReduction(reductions []*tsquery.ReductionType) bool {
	for _, rt := range reductions {
		if rt != nil {
			return true
		}
	}
	return false
}

// outputFieldsMeta derives the output metadata. A reduction may change a field's data type (e.g.
// Avg → Decimal). MetricKind is preserved (alignment changes granularity, not semantic meaning) and
// the raw, unnormalized kind is carried through to keep the unset-vs-explicit distinction.
func outputFieldsMeta(fieldsMeta []tsquery.FieldMeta, reductions []*tsquery.ReductionType) ([]tsquery.FieldMeta, error) {
	out := make([]tsquery.FieldMeta, len(fieldsMeta))
	for i, fm := range fieldsMeta {
		out[i] = fm
		if reductions[i] == nil {
			continue
		}
		outType := reductions[i].GetResultDataType(fm.DataType())
		if outType == fm.DataType() {
			continue
		}
		newMeta, err := tsquery.NewFieldMetaFull(
			fm.Urn(), outType, fm.RawMetricKind(), fm.Required(), fm.Unit(), fm.CustomMeta())
		if err != nil {
			return nil, fmt.Errorf("failed to build output meta for aligner field %q: %w", fm.Urn(), err)
		}
		out[i] = *newMeta
	}
	return out, nil
}

// alignWithReductions aligns the stream when at least one field resolves to a bucket reduction.
// In a single streaming pass per bucket it feeds reduction fields into per-field accumulators while
// capturing the first item for the interpolation fields, then assembles one output row per bucket.
func (af AlignerFilter) alignWithReductions(
	src stream.Stream[timeseries.TsRecord[[]any]],
	fieldsMeta []tsquery.FieldMeta,
	reductions []*tsquery.ReductionType,
) stream.Stream[timeseries.TsRecord[[]any]] {
	inputDataTypes := make([]tsquery.DataType, len(fieldsMeta))
	for i, fm := range fieldsMeta {
		inputDataTypes[i] = fm.DataType()
	}

	return stream.ClusterSortedStreamComparable[timeseries.TsRecord[[]any], timeseries.TsRecord[[]any], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream stream.Stream[timeseries.TsRecord[[]any]],
			lastItemOnPreviousCluster *timeseries.TsRecord[[]any],
		) (timeseries.TsRecord[[]any], error) {

			accs := make([]tsquery.Accumulator, len(fieldsMeta))
			for i := range fieldsMeta {
				if reductions[i] != nil {
					acc, err := reductions[i].NewAccumulator(inputDataTypes[i])
					if err != nil {
						return util.DefaultValue[timeseries.TsRecord[[]any]](), fmt.Errorf(
							"failed to create accumulator for field %q bucket reduction: %w", fieldsMeta[i].Urn(), err)
					}
					accs[i] = acc
				}
			}

			var firstItem *timeseries.TsRecord[[]any]
			err := clusterStream.ConsumeWithErr(ctx, func(item timeseries.TsRecord[[]any]) error {
				if firstItem == nil {
					it := item
					firstItem = &it
				}
				for i := range accs {
					if accs[i] != nil {
						accs[i].Add(item.Value[i], item.Timestamp)
					}
				}
				return nil
			})
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[[]any]](), err
			}
			if firstItem == nil {
				// Defensive: a cluster always has at least one item.
				return util.DefaultValue[timeseries.TsRecord[[]any]](), fmt.Errorf(
					"aligner produced no items for bucket at %s", clusterTimestampClassifier)
			}

			// Interpolation fields replicate the no-reduction behavior: the first cluster (or an item
			// already aligned to the slot) smudges the first value to the bucket start; otherwise the
			// value is interpolated across the cluster boundary.
			needInterp := lastItemOnPreviousCluster != nil && firstItem.Timestamp != clusterTimestampClassifier
			var interpArr []any
			if needInterp {
				interpArr, err = timeWeightedAverageArr(
					fieldsMeta,
					clusterTimestampClassifier,
					lastItemOnPreviousCluster.Timestamp,
					lastItemOnPreviousCluster.Value,
					firstItem.Timestamp,
					firstItem.Value,
				)
				if err != nil {
					return util.DefaultValue[timeseries.TsRecord[[]any]](), fmt.Errorf(
						"error calculating time weighted average while aligning streams: %w", err)
				}
			}

			out := make([]any, len(fieldsMeta))
			for i := range fieldsMeta {
				switch {
				case accs[i] != nil:
					out[i] = accs[i].Result()
				case needInterp:
					out[i] = interpArr[i]
				default:
					out[i] = firstItem.Value[i]
				}
			}

			return timeseries.TsRecord[[]any]{Value: out, Timestamp: clusterTimestampClassifier}, nil
		},
		recordAlignmentPeriodClassifierFunc(af.alignmentPeriod),
		src,
	)
}

// alignByInterpolation is the alignment path used when no field resolves to a bucket reduction. It
// either smudges the first item to the bucket start or computes a time-weighted average across
// cluster boundaries.
func (af AlignerFilter) alignByInterpolation(
	src stream.Stream[timeseries.TsRecord[[]any]],
	fieldsMeta []tsquery.FieldMeta,
) stream.Stream[timeseries.TsRecord[[]any]] {
	return stream.ClusterSortedStreamComparable[timeseries.TsRecord[[]any], timeseries.TsRecord[[]any], time.Time](
		func(
			ctx context.Context,
			clusterTimestampClassifier time.Time,
			clusterStream stream.Stream[timeseries.TsRecord[[]any]],
			lastItemOnPreviousCluster *timeseries.TsRecord[[]any],
		) (timeseries.TsRecord[[]any], error) {

			firstItem, err := clusterStream.FindFirst().Get(ctx)
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[[]any]](), err
			}

			// If this is the first cluster, smudge the first item to the start of the cluster.
			if lastItemOnPreviousCluster == nil {
				return timeseries.TsRecord[[]any]{
					Value:     firstItem.Value,
					Timestamp: clusterTimestampClassifier,
				}, nil
			}
			// Check if the first item is magically aligned to the slot, return it.
			if firstItem.Timestamp == clusterTimestampClassifier {
				return timeseries.TsRecord[[]any]{
					Value:     firstItem.Value,
					Timestamp: clusterTimestampClassifier,
				}, nil
			}
			// Otherwise, calculate the time-weighted average.
			avgItem, err := timeWeightedAverageArr(
				fieldsMeta,
				clusterTimestampClassifier,
				lastItemOnPreviousCluster.Timestamp,
				lastItemOnPreviousCluster.Value,
				firstItem.Timestamp,
				firstItem.Value,
			)
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[[]any]](), fmt.Errorf("error calculating time weighted average while aliging streams: %w", err)
			}
			return timeseries.TsRecord[[]any]{
				Value:     avgItem,
				Timestamp: clusterTimestampClassifier,
			}, nil
		},
		recordAlignmentPeriodClassifierFunc(af.alignmentPeriod),
		src,
	)
}

// fillGaps wraps the sparse aligned stream with a gap-filler. When no field carries a per-field fill
// override, the original single-mode gap-filler is used and behavior is unchanged. When at least one
// field overrides the fill mode, the gap-filler runs in linear mode (so the callback is always
// invoked for interior gaps, where both neighbors are available) and dispatches per field: a
// forward-fill field copies the previous value, a linear field interpolates between neighbors.
func (af AlignerFilter) fillGaps(
	s stream.Stream[timeseries.TsRecord[[]any]],
	outFieldsMeta []tsquery.FieldMeta,
) stream.Stream[timeseries.TsRecord[[]any]] {
	copyFn := func(v []any) []any { c := make([]any, len(v)); copy(c, v); return c }

	if !af.hasFieldFillOverride() {
		return timeseries.NewTsGapFillerStream[[]any](
			s,
			af.alignmentPeriod,
			*af.fillMode,
			func(targetTime, v1Time time.Time, v1 []any, v2Time time.Time, v2 []any) ([]any, error) {
				return timeWeightedAverageArr(outFieldsMeta, targetTime, v1Time, v1, v2Time, v2)
			},
			copyFn,
		)
	}

	fieldFill := make([]timeseries.FillMode, len(outFieldsMeta))
	for i, fm := range outFieldsMeta {
		fieldFill[i] = *af.fillMode
		if fa, ok := af.fieldAlignments[fm.Urn()]; ok && fa.FillMode != nil {
			fieldFill[i] = *fa.FillMode
		}
	}

	return timeseries.NewTsGapFillerStream[[]any](
		s,
		af.alignmentPeriod,
		timeseries.FillModeLinear,
		func(targetTime, v1Time time.Time, v1 []any, v2Time time.Time, v2 []any) ([]any, error) {
			out := make([]any, len(v1))
			for i := range v1 {
				switch fieldFill[i] {
				case timeseries.FillModeForwardFill:
					out[i] = v1[i]
				case timeseries.FillModeLinear:
					val, err := timeWeightedAverageValue(outFieldsMeta[i].DataType(), targetTime, v1Time, v1[i], v2Time, v2[i])
					if err != nil {
						return nil, err
					}
					out[i] = val
				default:
					return nil, fmt.Errorf("unsupported fill mode: %q", fieldFill[i])
				}
			}
			return out, nil
		},
		copyFn,
	)
}

func (af AlignerFilter) hasFieldFillOverride() bool {
	for _, fa := range af.fieldAlignments {
		if fa.FillMode != nil {
			return true
		}
	}
	return false
}

func recordAlignmentPeriodClassifierFunc(ap timeseries.AlignmentPeriod) func(a timeseries.TsRecord[[]any]) time.Time {
	return func(a timeseries.TsRecord[[]any]) time.Time { return ap.GetStartTime(a.Timestamp) }
}

// timeWeightedAverageArr computes the per-field time-weighted average of two value rows, erroring if
// any value is not numeric.
func timeWeightedAverageArr(fieldsMeta []tsquery.FieldMeta, targetTime, v1Time time.Time, v1Arr []any, v2Time time.Time, v2Arr []any) ([]any, error) {
	res := make([]any, len(v1Arr))
	for i := range v1Arr {
		v, err := timeWeightedAverageValue(fieldsMeta[i].DataType(), targetTime, v1Time, v1Arr[i], v2Time, v2Arr[i])
		if err != nil {
			return nil, err
		}
		res[i] = v
	}
	return res, nil
}

// timeWeightedAverageValue computes the time-weighted average of a single value between two
// bracketing samples, casting through float64 and back to the field's data type.
func timeWeightedAverageValue(dt tsquery.DataType, targetTime, v1Time time.Time, v1 any, v2Time time.Time, v2 any) (any, error) {
	if v1Time.Equal(v2Time) {
		if v1Time == targetTime {
			return v1, nil
		}
		return nil, fmt.Errorf("v1Time and v2Time are the same: %s. targetTime:%s", v1Time, targetTime)
	}

	// Ensure the targetTime is between v1Time and v2Time.
	if targetTime.Before(v1Time) || targetTime.After(v2Time) {
		return nil, fmt.Errorf("targetTime %s is out of bounds (%s to %s)", targetTime, v1Time, v2Time)
	}

	// Calculate the interpolation factor.
	totalDuration := v2Time.Sub(v1Time).Seconds()
	interpolatedDuration := targetTime.Sub(v1Time).Seconds()
	weight := interpolatedDuration / totalDuration

	v1Float, err := dt.ToFloat64(v1)
	if err != nil {
		return nil, fmt.Errorf("error converting v1 to float64 for weighted average: %w", err)
	}
	v2Float, err := dt.ToFloat64(v2)
	if err != nil {
		return nil, fmt.Errorf("error converting v2 to float64 for weighted average: %w", err)
	}
	retVal, err := dt.FromFloat64(v1Float + (v2Float-v1Float)*weight)
	if err != nil {
		return nil, fmt.Errorf("failed to convert cast weighted average value back to datatype values when calculating weighted average: %w", err)
	}
	return retVal, nil
}
