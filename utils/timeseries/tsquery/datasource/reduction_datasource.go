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

var _ DataSource = ReductionDatasource{}

type ReductionDatasource struct {
	reductionType        tsquery.ReductionType
	alignmentPeriod      timeseries.AlignmentPeriod
	multiDataSource      MultiDataSource
	addFieldMeta         tsquery.AddFieldMeta
	emptyDatasourceValue Value // Optional fallback when multiDataSource yields zero datasources
}

func NewReductionDatasource(
	reductionType tsquery.ReductionType,
	alignmentPeriod timeseries.AlignmentPeriod,
	multiDataSource MultiDataSource,
	addFieldMeta tsquery.AddFieldMeta,
) *ReductionDatasource {
	return &ReductionDatasource{
		reductionType:   reductionType,
		alignmentPeriod: alignmentPeriod,
		multiDataSource: multiDataSource,
		addFieldMeta:    addFieldMeta,
	}
}

// NewReductionDatasourceWithEmptyFallback creates a ReductionDatasource with an optional
// fallback value to use when the multiDataSource yields zero datasources.
func NewReductionDatasourceWithEmptyFallback(
	reductionType tsquery.ReductionType,
	alignmentPeriod timeseries.AlignmentPeriod,
	multiDataSource MultiDataSource,
	addFieldMeta tsquery.AddFieldMeta,
	emptyDatasourceValue Value,
) *ReductionDatasource {
	return &ReductionDatasource{
		reductionType:        reductionType,
		alignmentPeriod:      alignmentPeriod,
		multiDataSource:      multiDataSource,
		addFieldMeta:         addFieldMeta,
		emptyDatasourceValue: emptyDatasourceValue,
	}
}

func (r ReductionDatasource) Execute(ctx context.Context, from time.Time, to time.Time) (Result, error) {

	// Validate alignment period is provided
	if r.alignmentPeriod == nil {
		return util.DefaultValue[Result](), fmt.Errorf("alignment period is required for reduction datasource")
	}

	// Execute datasources and apply alignment
	datasourceResultsToToReduce, err :=
		stream.MapWithErrAndCtx(
			r.multiDataSource.GetDatasources(ctx),
			func(ctx context.Context, ds DataSource) (Result, error) {
				return NewFilteredDataSource(ds, NewAlignerFilter(r.alignmentPeriod)).Execute(ctx, from, to)
			},
		).Collect(ctx)

	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("reduction datasource failed to downstream datasources: %w", err)
	}

	// Handle zero datasources case
	if len(datasourceResultsToToReduce) == 0 {
		if r.emptyDatasourceValue == nil {
			return util.DefaultValue[Result](), fmt.Errorf("no datasources to reduce")
		}
		// Use the fallback value for empty datasource case
		return r.executeEmptyDatasourceFallback(ctx)
	}

	// Extract metadata from all datasources and validate
	datasourceMetas := make([]tsquery.FieldMeta, len(datasourceResultsToToReduce))
	for i, result := range datasourceResultsToToReduce {
		datasourceMetas[i] = result.Meta()
	}

	// Verify all datasources have numeric, same type, and required fields
	dataType := datasourceMetas[0].DataType()
	if dataType != tsquery.DataTypeInteger && dataType != tsquery.DataTypeDecimal {
		return util.DefaultValue[Result](), fmt.Errorf("cannot reduce datasource %s: must be numeric (integer or decimal), got %s", datasourceMetas[0].Urn(), dataType)
	}
	if !datasourceMetas[0].Required() {
		return util.DefaultValue[Result](), fmt.Errorf("cannot reduce datasource %s: must be required", datasourceMetas[0].Urn())
	}

	// Track unit consistency
	firstUnit := datasourceMetas[0].Unit()
	allSameUnit := true

	for i := 1; i < len(datasourceMetas); i++ {
		if datasourceMetas[i].DataType() != dataType {
			return util.DefaultValue[Result](), fmt.Errorf("cannot reduce datasources: all datasources must have the same data type, got %s for %s and %s for %s",
				dataType, datasourceMetas[0].Urn(), datasourceMetas[i].DataType(), datasourceMetas[i].Urn())
		}
		if !datasourceMetas[i].Required() {
			return util.DefaultValue[Result](), fmt.Errorf("cannot reduce datasource %s: must be required", datasourceMetas[i].Urn())
		}
		if datasourceMetas[i].Unit() != firstUnit {
			allSameUnit = false
		}
	}

	// Create result field metadata
	resultDataType := r.reductionType.GetResultDataType(dataType)

	// Determine result unit: preserve if all datasources have the same unit
	var resultUnit string
	// For count, we can't preserve unit, so leave empty
	if allSameUnit && r.reductionType != tsquery.ReductionTypeCount {
		resultUnit = firstUnit
	}

	if r.addFieldMeta.Urn == "" {
		return util.DefaultValue[Result](), fmt.Errorf("URN in addFieldMeta is required for reduction datasource")
	}

	// Get URN from addFieldMeta (required)
	urn := r.addFieldMeta.Urn

	// Determine custom metadata: use from addFieldMeta if provided, otherwise from the first datasource
	var customMeta map[string]any
	if r.addFieldMeta.CustomMeta != nil {
		customMeta = r.addFieldMeta.CustomMeta
	} else {
		customMeta = datasourceMetas[0].CustomMeta()
	}

	// Determine the final unit: use OverrideUnit if not empty, otherwise use computed resultUnit
	finalUnit := resultUnit
	if r.addFieldMeta.OverrideUnit != "" {
		finalUnit = r.addFieldMeta.OverrideUnit
	}

	// Create result field metadata
	resultFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		urn,
		resultDataType,
		true, // a reduction result is always required
		finalUnit,
		customMeta,
	)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to create result field metadata: %w", err)
	}

	// Pre-compute the reduction function based on type and data type
	reducerFunc, err := r.reductionType.GetReducerFunc(dataType)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to get reduction function: %w", err)
	}

	// Extract streams from all results
	streams := make([]stream.Stream[timeseries.TsRecord[any]], len(datasourceResultsToToReduce))
	for i, result := range datasourceResultsToToReduce {
		streams[i] = result.Data()
	}

	var outputDataStream stream.Stream[timeseries.TsRecord[any]]
	// optimization for a single-datasource case: return the single stream directly
	if len(streams) == 1 && r.reductionType.UseIdentityWhenSingleValue() {
		outputDataStream = streams[0]
	} else {
		// Use InnerJoinStreams to reduce all values
		outputDataStream = timeseries.InnerJoinStreams(streams, reducerFunc)
	}
	return Result{
		meta: *resultFieldMeta,
		data: outputDataStream,
	}, nil
}

// executeEmptyDatasourceFallback handles the case when there are zero datasources
// by executing the emptyDatasourceValue to get metadata and returning an empty stream.
func (r ReductionDatasource) executeEmptyDatasourceFallback(ctx context.Context) (Result, error) {
	if r.addFieldMeta.Urn == "" {
		return util.DefaultValue[Result](), fmt.Errorf("URN in addFieldMeta is required for reduction datasource")
	}

	// Create a placeholder FieldMeta to pass to the field value execution.
	// Most field values (constant, nil) don't use the input FieldMeta,
	// but we need to provide one for the interface.
	placeholderMeta, err := tsquery.NewFieldMeta(
		r.addFieldMeta.Urn,
		tsquery.DataTypeDecimal, // placeholder type, not used
		true,
	)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to create placeholder metadata: %w", err)
	}

	// Execute the empty datasource value to get its metadata
	valueMeta, _, err := r.emptyDatasourceValue.Execute(ctx, *placeholderMeta)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to execute emptyDatasourceValue: %w", err)
	}

	// Determine the final unit: use OverrideUnit if not empty, otherwise use value's unit
	finalUnit := valueMeta.Unit
	if r.addFieldMeta.OverrideUnit != "" {
		finalUnit = r.addFieldMeta.OverrideUnit
	}

	// Create result field metadata from the emptyDatasourceValue's metadata
	resultFieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		r.addFieldMeta.Urn,
		valueMeta.DataType,
		valueMeta.Required,
		finalUnit,
		r.addFieldMeta.CustomMeta,
	)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to create result field metadata for empty datasource: %w", err)
	}

	// Return result with empty stream
	return Result{
		meta: *resultFieldMeta,
		data: stream.Empty[timeseries.TsRecord[any]](),
	}, nil
}
