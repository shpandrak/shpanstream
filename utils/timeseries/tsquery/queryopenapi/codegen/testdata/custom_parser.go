package testdata

import (
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
)

// CustomPluginParser implements PluginApiParser to handle custom datasource types
type CustomPluginParser struct{}

func (p CustomPluginParser) ParseDatasource(pCtx *ParsingContext, queryDatasource ApiQueryDatasource) (datasource.DataSource, error) {
	rawDs, err := queryDatasource.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedDs := rawDs.(type) {
	case ApiPostgresQueryDatasource:
		return parsePostgresDatasource(typedDs)
	default:
		return nil, fmt.Errorf("unsupported custom datasource type: %T", rawDs)
	}
}

func (p CustomPluginParser) ParseMultiDatasource(_ *ParsingContext, multiDatasource ApiMultiDatasource) (datasource.MultiDataSource, error) {
	return nil, fmt.Errorf("no custom multi-datasource types supported")
}

func (p CustomPluginParser) ParseFilter(_ *ParsingContext, filter ApiQueryFilter) (datasource.Filter, error) {
	return nil, fmt.Errorf("no custom filter types supported")
}

func (p CustomPluginParser) ParseFieldValue(_ *ParsingContext, field ApiQueryFieldValue) (datasource.Value, error) {
	return nil, fmt.Errorf("no custom field value types supported")
}

// parsePostgresDatasource creates a Postgres-backed datasource
// This is a mock implementation for testing purposes
func parsePostgresDatasource(ds ApiPostgresQueryDatasource) (datasource.DataSource, error) {
	// Parse field metadata
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		ds.FieldMeta.Uri,
		ds.FieldMeta.DataType,
		ds.FieldMeta.Required,
		ds.FieldMeta.Unit,
		ds.FieldMeta.CustomMetadata,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create field metadata for postgres datasource: %w", err)
	}

	// In a real implementation, this would:
	// 1. Connect to PostgreSQL using ds.ConnectionString
	// 2. Build a SQL query: SELECT {ds.FieldName}, {ds.TimestampFieldName} FROM {ds.TableName}
	// 3. Return a datasource that executes the query and streams results
	//
	// For this test/example, we return a static datasource with mock data
	emptyData := stream.FromSlice([]timeseries.TsRecord[any]{})
	staticDs, err := datasource.NewStaticDatasource(
		*fieldMeta,
		emptyData,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create static datasource: %w", err)
	}
	return staticDs, nil
}
