package queryopenapi

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestParseStaticDatasource_Valid(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{
				Timestamp: baseTime,
				Value:     20.5,
			},
			{
				Timestamp: baseTime.Add(1 * time.Hour),
				Value:     21.3,
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute the datasource to get results
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	// Verify field metadata
	fieldMeta := result.Meta()
	assert.Equal(t, "temperature", fieldMeta.Urn())
	assert.Equal(t, tsquery.DataTypeDecimal, fieldMeta.DataType())

	// Verify data
	records := result.Data().MustCollect()
	require.Len(t, records, 2)

	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.Equal(t, 20.5, records[0].Value)

	assert.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	assert.Equal(t, 21.3, records[1].Value)
}

func TestParseStaticDatasource_WithCustomMetadata(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	customMeta := map[string]interface{}{
		"source": "sensor1",
		"floor":  2,
	}

	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:            "temperature",
			DataType:       tsquery.DataTypeDecimal,
			Required:       true,
			Unit:           "celsius",
			CustomMetadata: customMeta,
		},
		Data: []ApiMeasurementValue{
			{
				Timestamp: baseTime,
				Value:     20.5,
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute the datasource to get results
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	// Verify field metadata with custom data
	fieldMeta := result.Meta()
	assert.Equal(t, "temperature", fieldMeta.Urn())
	assert.Equal(t, tsquery.DataTypeDecimal, fieldMeta.DataType())
	assert.True(t, fieldMeta.Required())
	assert.Equal(t, "celsius", fieldMeta.Unit())

	// Verify custom metadata
	meta := fieldMeta.CustomMeta()
	assert.Equal(t, "sensor1", meta["source"])
	assert.Equal(t, 2, meta["floor"])
}

func TestParseStaticDatasource_AllDataTypes(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		dataType tsquery.DataType
		value    any
	}{
		{"integer", tsquery.DataTypeInteger, int64(42)},
		{"decimal", tsquery.DataTypeDecimal, 3.14},
		{"string", tsquery.DataTypeString, "hello"},
		{"boolean", tsquery.DataTypeBoolean, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apiDs := ApiStaticQueryDatasource{
				Type: "static",
				FieldMeta: ApiQueryFieldMeta{
					Uri:      tt.name + "Field",
					DataType: tt.dataType,
					Required: false,
				},
				Data: []ApiMeasurementValue{
					{
						Timestamp: baseTime,
						Value:     tt.value,
					},
				},
			}

			ds, err := parseStaticDatasource(apiDs)
			require.NoError(t, err)
			require.NotNil(t, ds)

			// Execute and verify
			ctx := testContext()
			result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
			require.NoError(t, err)

			records := result.Data().MustCollect()
			require.Len(t, records, 1)

			// Verify the value
			assert.Equal(t, tt.value, records[0].Value)
		})
	}
}

func TestParseDatasource_Static(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{
				Timestamp: baseTime,
				Value:     100.0,
			},
		},
	}

	// Wrap in ApiQueryDatasource
	var apiDs ApiQueryDatasource
	err := apiDs.FromApiStaticQueryDatasource(staticDs)
	require.NoError(t, err)

	// Parse using the main parseDatasource function
	ds, err := ParseDatasource(NewParsingContext(context.Background(), nil), apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Verify it works
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, 100.0, records[0].Value)
}

func TestParseStaticDatasource_EmptyData(t *testing.T) {
	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{},
	}

	ds, err := parseStaticDatasource(apiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute and verify it returns empty results
	ctx := testContext()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 0)
}

func TestParseStaticDatasource_InvalidFieldMeta(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Empty URI should fail
	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "", // Invalid: empty URI
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{
				Timestamp: baseTime,
				Value:     20.5,
			},
		},
	}

	ds, err := parseStaticDatasource(apiDs)
	assert.Error(t, err)
	assert.Nil(t, ds)
	assert.Contains(t, err.Error(), "failed to create field metadata")
}

func TestParseReductionDatasource_WithPipeline(t *testing.T) {
	// This test validates: reduction -> multiply by itself -> square root -> cast to integer
	// If we reduce with sum and get value X, then X * X = X^2, then sqrt(X^2) = X
	// So the final cast integer should equal the original sum
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create two static datasources with values [1, 2, 3] and [4, 5, 6]
	// Sum reduction will give us 1+4=5, 2+5=7, 3+6=9
	staticDs1 := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value1",
			DataType: tsquery.DataTypeDecimal,
			Required: true, // Must be required for reduction
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 1.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 2.0},
			{Timestamp: baseTime.Add(2 * time.Hour), Value: 3.0},
		},
	}

	staticDs2 := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value2",
			DataType: tsquery.DataTypeDecimal,
			Required: true, // Must be required for reduction
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 4.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 5.0},
			{Timestamp: baseTime.Add(2 * time.Hour), Value: 6.0},
		},
	}

	// Wrap static datasources in ApiQueryDatasource
	var apiDs1, apiDs2 ApiQueryDatasource
	require.NoError(t, apiDs1.FromApiStaticQueryDatasource(staticDs1))
	require.NoError(t, apiDs2.FromApiStaticQueryDatasource(staticDs2))

	// Create alignment period
	var alignmentPeriod ApiAlignmentPeriod

	require.NoError(t, alignmentPeriod.FromApiCalendarAlignmentPeriod(ApiCalendarAlignmentPeriod{
		AlignmentPeriodType: Hour,
		ZoneId:              "UTC",
	}))

	// Create multi datasource
	var multiDs ApiMultiDatasource
	require.NoError(t, multiDs.FromApiListMultiDatasource(ApiListMultiDatasource{
		Datasources: []ApiQueryDatasource{apiDs1, apiDs2},
	}))

	// Create a reduction datasource (sum)
	reductionDs := ApiReductionQueryDatasource{
		ReductionType:   tsquery.ReductionTypeSum,
		AlignmentPeriod: alignmentPeriod,
		MultiDatasource: multiDs,
		FieldMeta: ApiAddFieldMeta{
			Uri: "summed",
		},
	}

	// Wrap reduction datasource in ApiQueryDatasource
	var reductionApiDs ApiQueryDatasource
	require.NoError(t, reductionApiDs.FromApiReductionQueryDatasource(reductionDs))

	// Create ref fields for multiplication: ref * ref (X * X = X^2)
	var refField1, refField2 ApiQueryFieldValue

	require.NoError(t, refField1.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	require.NoError(t, refField2.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	// Create a multiply field
	var multiplyFieldValue ApiQueryFieldValue

	require.NoError(t, multiplyFieldValue.FromApiNumericExpressionQueryFieldValue(ApiNumericExpressionQueryFieldValue{
		Op1: refField1,
		Op:  tsquery.BinaryNumericOperatorMul,
		Op2: refField2,
	}))

	// Apply sqrt to get back to X
	var sqrtFieldValue ApiQueryFieldValue
	require.NoError(t, sqrtFieldValue.FromApiUnaryNumericOperatorQueryFieldValue(ApiUnaryNumericOperatorQueryFieldValue{
		Operand: multiplyFieldValue,
		Op:      tsquery.UnaryNumericOperatorSqrt,
	}))

	// Cast to integer
	var castFieldValue ApiQueryFieldValue
	require.NoError(t, castFieldValue.FromApiCastQueryFieldValue(ApiCastQueryFieldValue{
		Source:     sqrtFieldValue,
		TargetType: tsquery.DataTypeInteger,
	}))

	// Create fieldValue filter
	var fieldValueFilter ApiQueryFilter
	require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
		FieldValue: castFieldValue,
		FieldMeta: ApiAddFieldMeta{
			Uri: "result",
		},
	}))

	// Create a filtered datasource with fieldValue filter
	filteredDs := ApiFilteredQueryDatasource{
		Datasource: reductionApiDs,
		Filters:    []ApiQueryFilter{fieldValueFilter},
	}

	// Wrap in ApiQueryDatasource
	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	// Parse the entire datasource
	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute and verify
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(3*time.Hour))
	require.NoError(t, err)

	// Verify field metadata
	fieldMeta := result.Meta()
	assert.Equal(t, "result", fieldMeta.Urn())
	assert.Equal(t, tsquery.DataTypeInteger, fieldMeta.DataType())

	// Verify data: sum gives us 5, 7, 9
	// 5*5=25, sqrt(25)=5, cast to int = 5
	// 7*7=49, sqrt(49)=7, cast to int = 7
	// 9*9=81, sqrt(81)=9, cast to int = 9
	records := result.Data().MustCollect()
	require.Len(t, records, 3)

	assert.Equal(t, baseTime, records[0].Timestamp)
	assert.Equal(t, int64(5), records[0].Value)

	assert.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	assert.Equal(t, int64(7), records[1].Value)

	assert.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
	assert.Equal(t, int64(9), records[2].Value)
}

// Helper function to create a test context
func testContext() context.Context {
	return context.Background()
}
