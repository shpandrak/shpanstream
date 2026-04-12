package aggregation

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"time"
)

// ReportAggregationFieldDef defines one aggregation to compute on a report field.
// For paired reductions (mae, rmse, mbe, mape, pearson, r2), CompareFieldUrn must
// be set to the "predicted" field URN; SourceFieldUrn is the "actual" field.
type ReportAggregationFieldDef struct {
	ReductionType  tsquery.ReductionType
	SourceFieldUrn string                 // which report field to aggregate ("actual" for paired reductions)
	CompareFieldUrn string                // "predicted" field for paired reductions; empty for single-field reductions
	AddFieldMeta   *tsquery.AddFieldMeta  // nil = auto-derive from source
	EmptyValue     datasource.StaticValue // nil = null when stream is empty
}

// FromReportAggregation aggregates a report datasource stream into scalar values.
type FromReportAggregation struct {
	reportDatasource report.DataSource
	fields           []ReportAggregationFieldDef
}

func NewFromReportAggregation(reportDs report.DataSource, fields []ReportAggregationFieldDef) *FromReportAggregation {
	return &FromReportAggregation{
		reportDatasource: reportDs,
		fields:           fields,
	}
}

func (a *FromReportAggregation) Execute(ctx context.Context, from time.Time, to time.Time) (Result, error) {
	// EAGER: execute report datasource, validate, create accumulators, build metadata
	reportResult, err := a.reportDatasource.Execute(ctx, from, to)
	if err != nil {
		return Result{}, fmt.Errorf("failed to execute report datasource for aggregation: %w", err)
	}

	fieldsMeta := reportResult.FieldsMeta()

	// Resolve field indices and create accumulators.
	// For paired reductions, we track both the source (actual) and compare (predicted) field indices.
	fieldIndices := make([]int, len(a.fields))
	compareFieldIndices := make([]int, len(a.fields))   // -1 for non-paired
	accumulators := make([]tsquery.Accumulator, len(a.fields))
	pairedAccumulators := make([]tsquery.PairedAccumulator, len(a.fields)) // nil for non-paired
	sourceFieldMetas := make([]tsquery.FieldMeta, len(a.fields))

	for i, field := range a.fields {
		compareFieldIndices[i] = -1

		// Find the source field index by URN
		idx := -1
		for j, fm := range fieldsMeta {
			if fm.Urn() == field.SourceFieldUrn {
				idx = j
				sourceFieldMetas[i] = fm
				break
			}
		}
		if idx < 0 {
			return Result{}, fmt.Errorf("source field URN %q not found in report datasource (available fields: %v)",
				field.SourceFieldUrn, urnList(fieldsMeta))
		}
		fieldIndices[i] = idx

		sourceDataType := sourceFieldMetas[i].DataType()

		// Validate numeric for reductions that require it
		if field.ReductionType.RequiresNumeric() {
			if !sourceDataType.IsNumeric() {
				return Result{}, fmt.Errorf("cannot aggregate non-numeric field %q (type %s) with reduction %s",
					field.SourceFieldUrn, sourceDataType, field.ReductionType)
			}
		}

		if field.ReductionType.IsPaired() {
			// Paired reduction: resolve compare field and create paired accumulator
			if field.CompareFieldUrn == "" {
				return Result{}, fmt.Errorf("paired reduction %s requires compareFieldUrn but none was provided (source field %q)",
					field.ReductionType, field.SourceFieldUrn)
			}

			cmpIdx := -1
			for j, fm := range fieldsMeta {
				if fm.Urn() == field.CompareFieldUrn {
					cmpIdx = j
					break
				}
			}
			if cmpIdx < 0 {
				return Result{}, fmt.Errorf("compare field URN %q not found in report datasource (available fields: %v)",
					field.CompareFieldUrn, urnList(fieldsMeta))
			}

			// Validate compare field is also numeric
			cmpDataType := fieldsMeta[cmpIdx].DataType()
			if !cmpDataType.IsNumeric() {
				return Result{}, fmt.Errorf("cannot use non-numeric compare field %q (type %s) with paired reduction %s",
					field.CompareFieldUrn, cmpDataType, field.ReductionType)
			}

			compareFieldIndices[i] = cmpIdx

			pairedAcc, err := field.ReductionType.NewPairedAccumulator()
			if err != nil {
				return Result{}, fmt.Errorf("failed to create paired accumulator for report field %d (reduction %s): %w",
					i, field.ReductionType, err)
			}
			pairedAccumulators[i] = pairedAcc
		} else {
			// Single-field reduction
			if field.CompareFieldUrn != "" {
				return Result{}, fmt.Errorf("compareFieldUrn must not be set for non-paired reduction %s (source field %q)",
					field.ReductionType, field.SourceFieldUrn)
			}

			acc, err := field.ReductionType.NewAccumulator(sourceDataType)
			if err != nil {
				return Result{}, fmt.Errorf("failed to create accumulator for report field %d (reduction %s on %q): %w",
					i, field.ReductionType, field.SourceFieldUrn, err)
			}
			accumulators[i] = acc
		}
	}

	// Validate URN uniqueness
	if err := a.validateReportFieldUrns(sourceFieldMetas); err != nil {
		return Result{}, err
	}

	// Build field metadata eagerly
	multipleFields := len(a.fields) > 1
	resultFieldsMeta := make([]tsquery.FieldMeta, len(a.fields))
	for i, field := range a.fields {
		sourceDataType := sourceFieldMetas[i].DataType()
		resultDataType := field.ReductionType.GetResultDataType(sourceDataType)
		meta, err := buildReportFieldMeta(field, sourceFieldMetas[i], resultDataType, multipleFields)
		if err != nil {
			return Result{}, fmt.Errorf("failed to build metadata for report aggregation field %d: %w", i, err)
		}
		resultFieldsMeta[i] = *meta
	}

	// LAZY: stream consumption + result building
	lazyFields := lazy.NewLazy(func(ctx context.Context) ([]AggregatedValue, error) {
		// Consume the report stream in a single pass
		err := reportResult.Stream().ConsumeWithErr(ctx, func(record timeseries.TsRecord[[]any]) error {
			for i := range a.fields {
				fieldIdx := fieldIndices[i]
				if fieldIdx >= len(record.Value) {
					return fmt.Errorf("report record has %d fields but aggregation requires field at index %d (URN %q)",
						len(record.Value), fieldIdx, a.fields[i].SourceFieldUrn)
				}

				if pairedAccumulators[i] != nil {
					// Paired accumulator: feed both actual and predicted values
					cmpIdx := compareFieldIndices[i]
					if cmpIdx >= len(record.Value) {
						return fmt.Errorf("report record has %d fields but aggregation requires compare field at index %d (URN %q)",
							len(record.Value), cmpIdx, a.fields[i].CompareFieldUrn)
					}
					pairedAccumulators[i].AddPair(record.Value[fieldIdx], record.Value[cmpIdx], record.Timestamp)
				} else {
					// Single-field accumulator
					accumulators[i].Add(record.Value[fieldIdx], record.Timestamp)
				}
			}
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to consume report stream for aggregation: %w", err)
		}

		// Build result fields
		resultFields := make([]AggregatedValue, len(a.fields))
		for i, field := range a.fields {
			var resultValue any
			var resultTs *time.Time
			if pairedAccumulators[i] != nil {
				resultValue = pairedAccumulators[i].Result()
				resultTs = pairedAccumulators[i].ResultTimestamp()
			} else {
				resultValue = accumulators[i].Result()
				resultTs = accumulators[i].ResultTimestamp()
			}

			// Handle empty stream with fallback
			if resultValue == nil && field.EmptyValue != nil {
				fallbackValue, err := executeStaticFallback(ctx, field.EmptyValue)
				if err != nil {
					return nil, fmt.Errorf("failed to execute emptyValue for report aggregation field %d: %w", i, err)
				}
				resultValue = fallbackValue
			}

			resultFields[i] = AggregatedValue{
				Value:     resultValue,
				Timestamp: resultTs,
			}
		}

		return resultFields, nil
	})

	return NewResult(resultFieldsMeta, lazyFields), nil
}

func buildReportFieldMeta(field ReportAggregationFieldDef, sourceMeta tsquery.FieldMeta, resultDataType tsquery.DataType, multipleFields bool) (*tsquery.FieldMeta, error) {
	// Determine URN
	urn := resolveReportFieldUrn(field, sourceMeta, multipleFields)

	// Determine unit
	unit := sourceMeta.Unit()
	if field.ReductionType == tsquery.ReductionTypeCount {
		unit = ""
	}
	if field.ReductionType.IsPaired() {
		unit = field.ReductionType.ResultUnit(sourceMeta.Unit())
	}
	if field.AddFieldMeta != nil && field.AddFieldMeta.OverrideUnit != "" {
		unit = field.AddFieldMeta.OverrideUnit
	}

	// Determine custom metadata
	customMeta := sourceMeta.CustomMeta()
	if field.AddFieldMeta != nil && field.AddFieldMeta.CustomMeta != nil {
		customMeta = field.AddFieldMeta.CustomMeta
	}

	// Determine metric kind: aggregation produces a scalar; default gauge, respect override
	metricKind := tsquery.MetricKindGauge
	if field.AddFieldMeta != nil && field.AddFieldMeta.OverrideMetricKind != "" {
		metricKind = field.AddFieldMeta.OverrideMetricKind
	}

	// samplePeriod not propagated — aggregation produces a terminal scalar, not a time series
	return tsquery.NewFieldMetaFull(urn, resultDataType, metricKind, sourceMeta.Required(), unit, customMeta)
}

func (a *FromReportAggregation) validateReportFieldUrns(sourceFieldMetas []tsquery.FieldMeta) error {
	multipleFields := len(a.fields) > 1
	urnSet := make(map[string]bool)
	for i, field := range a.fields {
		urn := resolveReportFieldUrn(field, sourceFieldMetas[i], multipleFields)
		if urnSet[urn] {
			return fmt.Errorf("duplicate URN %q for report aggregation field %d: provide explicit fieldMeta.uri to disambiguate", urn, i)
		}
		urnSet[urn] = true
	}
	return nil
}

func resolveReportFieldUrn(field ReportAggregationFieldDef, sourceMeta tsquery.FieldMeta, multipleFields bool) string {
	if field.AddFieldMeta != nil && field.AddFieldMeta.Urn != "" {
		return field.AddFieldMeta.Urn
	}
	if multipleFields {
		return sourceMeta.Urn() + "_" + string(field.ReductionType)
	}
	return sourceMeta.Urn()
}

func urnList(fieldsMeta []tsquery.FieldMeta) []string {
	urns := make([]string, len(fieldsMeta))
	for i, fm := range fieldsMeta {
		urns[i] = fm.Urn()
	}
	return urns
}
