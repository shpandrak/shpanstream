package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
)

func ParseDatasource(
	pCtx *ParsingContext,
	ds ApiQueryDatasource,
) (datasource.DataSource, error) {
	rawDs, err := ds.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedDs := rawDs.(type) {
	case ApiStaticQueryDatasource:
		return parseStaticDatasource(typedDs)
	case ApiFilteredQueryDatasource:
		return parseFilteredDatasource(pCtx, typedDs)
	case ApiReductionQueryDatasource:
		return parseReductionDatasource(pCtx, typedDs)
	case ApiFromReportQueryDatasource:
		return parseFromReportDatasource(pCtx, typedDs)
	default:
		return wrapAndReturn(pCtx.ParseDatasource(pCtx, ds))("failed parsing datasource with plugin parser")
	}
}

func parseStaticDatasource(ds ApiStaticQueryDatasource) (datasource.DataSource, error) {
	// Parse field metadata (single field for datasource package)
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		ds.FieldMeta.Uri,
		ds.FieldMeta.DataType,
		ds.FieldMeta.Required,
		ds.FieldMeta.Unit,
		ds.FieldMeta.CustomMetadata,
	)
	if err != nil {
		return nil, badInputErrorWrap(ds.FieldMeta, err, "failed to create field metadata for static datasource")
	}

	// Convert API data to timeseries records (single value per record)
	records := make([]timeseries.TsRecord[any], len(ds.Data))
	for i, measurement := range ds.Data {
		records[i] = timeseries.TsRecord[any]{
			Timestamp: measurement.Timestamp,
			Value:     measurement.Value,
		}
	}

	// Create a stream from records
	recordStream := stream.FromSlice(records)

	// Create and return the static datasource
	return datasource.NewStaticDatasource(*fieldMeta, recordStream)
}

func ParseMultiDatasource(pCtx *ParsingContext, multiDs ApiMultiDatasource) (datasource.MultiDataSource, error) {
	valueByDiscriminator, err := multiDs.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedMds := valueByDiscriminator.(type) {
	case ApiListMultiDatasource:
		return parseListMultiDatasource(pCtx, typedMds)
	default:
		return wrapAndReturn(pCtx.ParseMultiDatasource(pCtx, multiDs))("failed parsing multi datasource with plugin parser")
	}
}

func parseListMultiDatasource(pCtx *ParsingContext, typedMds ApiListMultiDatasource) (datasource.MultiDataSource, error) {
	dsList := make([]datasource.DataSource, len(typedMds.Datasources))
	for i, ds := range typedMds.Datasources {
		parsedDatasource, err := ParseDatasource(pCtx, ds)
		if err != nil {
			return nil, fmt.Errorf("failed to parse datasource %d in list multi datasource: %w", i, err)
		}
		dsList[i] = parsedDatasource
	}
	return datasource.NewListMultiDatasource(dsList), nil
}

func parseReductionDatasource(
	pCtx *ParsingContext,
	reductionDs ApiReductionQueryDatasource,
) (datasource.DataSource, error) {
	// Parse alignment period
	alignmentPeriod, err := parseAlignmentPeriod(reductionDs.AlignmentPeriod)
	if err != nil {
		return nil, fmt.Errorf("failed to parse alignment period for reduction datasource: %w", err)
	}

	// Parse multi datasource
	multiDatasource, err := ParseMultiDatasource(pCtx, reductionDs.MultiDatasource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse multi datasource for reduction: %w", err)
	}

	// Parse field metadata
	addFieldMeta := parseAddFieldMeta(reductionDs.FieldMeta)

	// Create and return the reduction datasource
	return datasource.NewReductionDatasource(
		reductionDs.ReductionType,
		alignmentPeriod,
		multiDatasource,
		addFieldMeta,
	), nil
}

func parseFilteredDatasource(
	pCtx *ParsingContext,
	filteredDs ApiFilteredQueryDatasource,
) (datasource.DataSource, error) {
	unfilteredDatasource, err := ParseDatasource(pCtx, filteredDs.Datasource)
	if err != nil {
		return nil, err
	}
	if len(filteredDs.Filters) == 0 {
		return unfilteredDatasource, nil
	}
	var parsedFilters []datasource.Filter
	for _, rawFilter := range filteredDs.Filters {
		parsedFilter, err := ParseFilter(pCtx, rawFilter)
		if err != nil {
			return nil, err
		}
		parsedFilters = append(parsedFilters, parsedFilter)
	}
	return datasource.NewFilteredDataSource(unfilteredDatasource, parsedFilters...), nil
}

func parseFromReportDatasource(
	pCtx *ParsingContext,
	fromReportDs ApiFromReportQueryDatasource,
) (datasource.DataSource, error) {
	// Parse the report datasource
	reportDs, err := ParseReportDatasource(pCtx, fromReportDs.ReportDatasource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse report datasource for fromReport: %w", err)
	}

	// Create and return the datasource that extracts a single field from the report
	return report.ToDatasource(reportDs, fromReportDs.FieldUrn), nil
}

func wrapAndReturn[T any](v T, err error) func(format string, a ...any) (T, error) {
	return func(format string, a ...any) (T, error) {
		if err != nil {
			return v, fmt.Errorf(fmt.Sprintf(format, a...)+": %w", err)
		}
		return v, nil
	}
}
