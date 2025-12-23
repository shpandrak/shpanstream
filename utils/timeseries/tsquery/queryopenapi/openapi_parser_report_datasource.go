package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
)

// ParseReportDatasource parses an ApiReportDatasource into a report.DataSource
func ParseReportDatasource(
	pCtx *ParsingContext,
	ds ApiReportDatasource,
) (report.DataSource, error) {
	rawDs, err := ds.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedDs := rawDs.(type) {
	case ApiStaticReportDatasource:
		return parseStaticReportDatasource(typedDs)
	case ApiJoinReportDatasource:
		return parseJoinReportDatasource(pCtx, typedDs)
	case ApiFromDatasourceReportDatasource:
		return parseFromDatasourceReportDatasource(pCtx, typedDs)
	default:
		return wrapAndReturnReportDatasource(pCtx.ParseReportDatasource(pCtx, ds))("failed parsing report datasource with plugin parser")
	}
}

func parseStaticReportDatasource(ds ApiStaticReportDatasource) (report.DataSource, error) {
	// Parse fields metadata
	fieldsMeta := make([]tsquery.FieldMeta, len(ds.FieldsMeta))
	for i, fm := range ds.FieldsMeta {
		meta, err := tsquery.NewFieldMetaWithCustomData(
			fm.Uri,
			fm.DataType,
			fm.Required,
			fm.Unit,
			fm.CustomMetadata,
		)
		if err != nil {
			return nil, badInputErrorWrap(fm, err, "failed to create field metadata %d for static report datasource", i)
		}
		fieldsMeta[i] = *meta
	}

	// Convert API data to timeseries records (multiple values per record)
	records := make([]timeseries.TsRecord[[]any], len(ds.Data))
	for i, row := range ds.Data {
		records[i] = timeseries.TsRecord[[]any]{
			Timestamp: row.Timestamp,
			Value:     row.Values,
		}
	}

	// Create a stream from records
	recordStream := stream.FromSlice(records)

	// Create and return the static report datasource
	return report.NewStaticDatasource(fieldsMeta, recordStream)
}

func parseJoinReportDatasource(
	pCtx *ParsingContext,
	ds ApiJoinReportDatasource,
) (report.DataSource, error) {
	// Parse join type
	joinType, err := parseJoinType(ds.JoinType)
	if err != nil {
		return nil, fmt.Errorf("failed to parse join type for join report datasource: %w", err)
	}

	// Parse multi datasource
	multiDatasource, err := ParseReportMultiDatasource(pCtx, ds.MultiDatasource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse multi datasource for join report datasource: %w", err)
	}

	// Create and return the join datasource
	return report.NewJoinDatasource(multiDatasource, joinType), nil
}

func parseJoinType(jt ApiJoinType) (report.JoinType, error) {
	switch jt {
	case ApiJoinTypeInner:
		return report.InnerJoin, nil
	case ApiJoinTypeLeft:
		return report.LeftJoin, nil
	case ApiJoinTypeFull:
		return report.FullJoin, nil
	default:
		return 0, fmt.Errorf("unsupported join type: %s", jt)
	}
}

// ParseReportMultiDatasource parses an ApiReportMultiDatasource into a report.MultiDataSource
func ParseReportMultiDatasource(
	pCtx *ParsingContext,
	multiDs ApiReportMultiDatasource,
) (report.MultiDataSource, error) {
	valueByDiscriminator, err := multiDs.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}
	switch typedMds := valueByDiscriminator.(type) {
	case ApiListReportMultiDatasource:
		return parseListReportMultiDatasource(pCtx, typedMds)
	case ApiFromMultiDatasourceReportMultiDatasource:
		return parseFromMultiDatasourceReportMultiDatasource(pCtx, typedMds)
	default:
		return wrapAndReturnReportMultiDatasource(pCtx.ParseReportMultiDatasource(pCtx, multiDs))("failed parsing report multi datasource with plugin parser")
	}
}

func parseListReportMultiDatasource(
	pCtx *ParsingContext,
	typedMds ApiListReportMultiDatasource,
) (report.MultiDataSource, error) {
	dsList := make([]report.DataSource, len(typedMds.Datasources))
	for i, ds := range typedMds.Datasources {
		parsedDatasource, err := ParseReportDatasource(pCtx, ds)
		if err != nil {
			return nil, fmt.Errorf("failed to parse report datasource %d in list multi datasource: %w", i, err)
		}
		dsList[i] = parsedDatasource
	}
	return report.NewListMultiDatasource(dsList), nil
}

func parseFromDatasourceReportDatasource(
	pCtx *ParsingContext,
	ds ApiFromDatasourceReportDatasource,
) (report.DataSource, error) {
	// Parse the underlying query datasource
	parsedDs, err := ParseDatasource(pCtx, ds.Datasource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse datasource for fromDatasource report datasource: %w", err)
	}
	// Wrap it as a report datasource
	return report.FromDatasource(parsedDs), nil
}

func parseFromMultiDatasourceReportMultiDatasource(
	pCtx *ParsingContext,
	mds ApiFromMultiDatasourceReportMultiDatasource,
) (report.MultiDataSource, error) {
	// Parse the underlying multi datasource
	parsedMultiDs, err := ParseMultiDatasource(pCtx, mds.MultiDatasource)
	if err != nil {
		return nil, fmt.Errorf("failed to parse multi datasource for fromMultiDatasource report multi datasource: %w", err)
	}
	// Wrap it as a report multi datasource
	return report.FromMultiDatasource(parsedMultiDs), nil
}

// wrapAndReturnReportDatasource is a helper function to wrap errors for report datasources
func wrapAndReturnReportDatasource(result report.DataSource, err error) func(wrapMsg string) (report.DataSource, error) {
	return func(wrapMsg string) (report.DataSource, error) {
		if err != nil {
			return nil, fmt.Errorf("%s: %w", wrapMsg, err)
		}
		return result, nil
	}
}

// wrapAndReturnReportMultiDatasource is a helper function to wrap errors for report multi datasources
func wrapAndReturnReportMultiDatasource(result report.MultiDataSource, err error) func(wrapMsg string) (report.MultiDataSource, error) {
	return func(wrapMsg string) (report.MultiDataSource, error) {
		if err != nil {
			return nil, fmt.Errorf("%s: %w", wrapMsg, err)
		}
		return result, nil
	}
}
