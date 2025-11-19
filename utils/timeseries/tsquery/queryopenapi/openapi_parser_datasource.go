package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
)

func ParseDatasource(
	pCtx *ParsingContext,
	ds ApiQueryDatasource,
) (report.DataSource, error) {
	rawDs, err := ds.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedDs := rawDs.(type) {
	case ApiJoinQueryDatasource:
		return parseJoinDatasource(pCtx, typedDs)
	case ApiStaticQueryDatasource:
		return parseStaticDatasource(typedDs)
	case ApiFilteredQueryDatasource:
		return parseFilteredDatasource(pCtx, typedDs)
	default:
		return util.WrapAndReturn(pCtx.ParseDatasource(pCtx, ds))("failed parsing datasource with plugin parser")
	}
}

func parseJoinDatasource(
	pCtx *ParsingContext,
	ds ApiJoinQueryDatasource,
) (report.DataSource, error) {
	multiDatasource, err := parseMultiDatasource(pCtx, ds.Datasources)
	if err != nil {
		return nil, fmt.Errorf("failed to parse multi datasource for join: %w", err)
	}
	var joinType report.JoinType
	switch ds.JoinType {
	case Inner:
		joinType = report.InnerJoin
	case Left:
		joinType = report.LeftJoin
	case Full:
		joinType = report.FullJoin
	default:
		return nil, fmt.Errorf("unsupported join type %v", ds.JoinType)
	}
	return report.NewJoinDatasource(multiDatasource, joinType), nil

}

func parseStaticDatasource(ds ApiStaticQueryDatasource) (report.DataSource, error) {
	// Parse field metadata
	var fieldsMeta []tsquery.FieldMeta
	for _, apiMeta := range ds.FieldsMeta {
		meta, err := tsquery.NewFieldMetaWithCustomData(
			apiMeta.Uri,
			apiMeta.DataType,
			apiMeta.Required,
			apiMeta.Unit,
			apiMeta.CustomMetadata,
		)
		if err != nil {
			return nil, badInputErrorWrap(apiMeta, err, "failed to create field metadata for static datasource")
		}
		fieldsMeta = append(fieldsMeta, *meta)
	}

	// Convert API data rows to timeseries records
	var records []timeseries.TsRecord[[]any]
	for _, row := range ds.Data {
		records = append(records, timeseries.TsRecord[[]any]{
			Timestamp: row.Timestamp,
			Value:     row.Values,
		})
	}

	// Create a stream from records
	recordStream := stream.FromSlice(records)

	// Create and return the static datasource
	return report.NewStaticDatasource(fieldsMeta, recordStream)
}

func parseMultiDatasource(pCtx *ParsingContext, multiDs ApiMultiDatasource) (report.MultiDataSource, error) {
	valueByDiscriminator, err := multiDs.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedMds := valueByDiscriminator.(type) {
	case ApiListMultiDatasource:
		return parseListMultiDatasource(pCtx, typedMds)
	default:
		return util.WrapAndReturn(pCtx.ParseMultiDatasource(pCtx, multiDs))("failed parsing multi datasource with plugin parser")

	}
}

func parseListMultiDatasource(pCtx *ParsingContext, typedMds ApiListMultiDatasource) (report.MultiDataSource, error) {
	dsList := make([]report.DataSource, len(typedMds.Datasources))
	for i, ds := range typedMds.Datasources {
		filteredDatasource, err := ParseDatasource(pCtx, ds)
		if err != nil {
			return nil, fmt.Errorf("failed to parse datasource %d in list multi datasource: %w", i, err)
		}
		dsList[i] = filteredDatasource

	}
	return report.NewListMultiDatasource(dsList), nil
}

func parseFilteredDatasource(
	pCtx *ParsingContext,
	filteredDs ApiFilteredQueryDatasource,
) (report.DataSource, error) {
	unfilteredDatasource, err := ParseDatasource(pCtx, filteredDs.Datasource)
	if err != nil {
		return nil, err
	}
	if len(filteredDs.Filters) == 0 {
		return unfilteredDatasource, nil
	}
	var parsedFilters []report.Filter
	for _, rawFilter := range filteredDs.Filters {
		parsedFilter, err := ParseFilter(rawFilter)
		if err != nil {
			return nil, err
		}
		parsedFilters = append(parsedFilters, parsedFilter)
	}
	return report.NewFilteredDataSource(unfilteredDatasource, parsedFilters...), nil

}
