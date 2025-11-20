package testdata

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestCustomPluginParser_PostgresDatasource(t *testing.T) {
	// Create a Postgres datasource using the generated API types
	var fieldMeta ApiQueryFieldMeta
	fieldMeta.Uri = "test.metric.value"
	fieldMeta.DataType = tsquery.DataTypeDecimal
	fieldMeta.Required = true
	fieldMeta.Unit = "ms"

	var postgresDatasource ApiQueryDatasource
	err := postgresDatasource.FromApiPostgresQueryDatasource(ApiPostgresQueryDatasource{
		Type:                 "postgres",
		ConnectionString:     "postgresql://user:pass@localhost:5432/testdb",
		TableName:            "metrics",
		FieldName:            "value",
		TimestampFieldName:   "timestamp",
		FieldMeta:            fieldMeta,
	})
	require.NoError(t, err)

	// Create parsing context with custom plugin parser
	pCtx := NewParsingContext(context.Background(), CustomPluginParser{})

	// Parse the datasource
	ds, err := ParseDatasource(pCtx, postgresDatasource)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute the datasource to get metadata (datasource interface only exposes Execute)
	// We use zero times since this is a mock implementation
	result, err := ds.Execute(context.Background(), time.Time{}, time.Time{})
	require.NoError(t, err)

	// Verify the result has correct metadata
	meta := result.Meta()
	require.Equal(t, "test.metric.value", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	require.True(t, meta.Required())
	require.Equal(t, "ms", meta.Unit())
}

func TestCustomPluginParser_UnsupportedTypes(t *testing.T) {
	pCtx := NewParsingContext(context.Background(), CustomPluginParser{})

	// Test unsupported filter type
	var filter ApiQueryFilter
	_, err := pCtx.ParseFilter(pCtx, filter)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no custom filter types supported")

	// Test unsupported field value type
	var field ApiQueryFieldValue
	_, err = pCtx.ParseFieldValue(pCtx, field)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no custom field value types supported")

	// Test unsupported multi-datasource type
	var multiDs ApiMultiDatasource
	_, err = pCtx.ParseMultiDatasource(pCtx, multiDs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no custom multi-datasource types supported")
}

func TestCodegen_PackageTransformation(t *testing.T) {
	// This test validates that the codegen correctly transformed the package references
	// If this compiles, it means:
	// 1. Package declaration was changed from queryopenapi to testdata
	// 2. Api* struct references work correctly in the same package
	// 3. Imports were preserved correctly

	pCtx := NewParsingContext(context.Background(), nil)
	require.NotNil(t, pCtx)
	require.NotNil(t, pCtx.PluginApiParser)
}
