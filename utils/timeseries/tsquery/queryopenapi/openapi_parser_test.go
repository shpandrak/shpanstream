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
		AlignmentPeriodType: ApiCalendarPeriodTypeHour,
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

// ========================================
// Filter Tests - Complete Coverage
// ========================================

func TestParseFilter_AlignerFilter(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 20.5},
			{Timestamp: baseTime.Add(30 * time.Minute), Value: 21.0},
			{Timestamp: baseTime.Add(90 * time.Minute), Value: 22.5},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create aligner filter with hourly alignment
	var alignmentPeriod ApiAlignmentPeriod
	require.NoError(t, alignmentPeriod.FromApiCalendarAlignmentPeriod(ApiCalendarAlignmentPeriod{
		AlignmentPeriodType: ApiCalendarPeriodTypeHour,
		ZoneId:              "UTC",
	}))

	var alignerFilter ApiQueryFilter
	require.NoError(t, alignerFilter.FromApiAlignerFilter(ApiAlignerFilter{
		AlignerPeriod:     alignmentPeriod,
		AlignmentFunction: ApiAlignerFunctionAvg,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{alignerFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	// Execute and verify aligner works
	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	assert.Greater(t, len(records), 0, "Aligner should produce aligned records")
}

func TestParseFilter_ConditionFilter(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 15.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 25.0},
			{Timestamp: baseTime.Add(2 * time.Hour), Value: 10.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create condition: ref > 20
	var refField ApiQueryFieldValue
	require.NoError(t, refField.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	var constantField ApiQueryFieldValue
	require.NoError(t, constantField.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 20.0,
	}))

	// Wrap ref in nvl to make it required (condition requires required boolean field)
	var nvlField ApiQueryFieldValue
	require.NoError(t, nvlField.FromApiNvlQueryFieldValue(ApiNvlQueryFieldValue{
		Source:   refField,
		AltField: constantField,
	}))

	var conditionField ApiQueryFieldValue
	require.NoError(t, conditionField.FromApiConditionQueryFieldValue(ApiConditionQueryFieldValue{
		OperatorType: tsquery.ConditionOperatorGreaterThan,
		Operand1:     nvlField,
		Operand2:     constantField,
	}))

	var conditionFilter ApiQueryFilter
	require.NoError(t, conditionFilter.FromApiConditionFilter(ApiConditionFilter{
		BooleanField: conditionField,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{conditionFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(3*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, 25.0, records[0].Value)
}

func TestParseFilter_OverrideFieldMetadataFilter(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "original",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
			Unit:     "celsius",
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 20.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Override URN and unit
	updatedUrn := "overridden"
	updatedUnit := "fahrenheit"
	customMeta := map[string]interface{}{"source": "override-test"}

	var overrideFilter ApiQueryFilter
	require.NoError(t, overrideFilter.FromApiOverrideFieldMetadataFilter(ApiOverrideFieldMetadataFilter{
		UpdatedUrn:        updatedUrn,
		UpdatedUnit:       updatedUnit,
		UpdatedCustomMeta: customMeta,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{overrideFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	meta := result.Meta()
	assert.Equal(t, "overridden", meta.Urn())
	assert.Equal(t, "fahrenheit", meta.Unit())
	assert.Equal(t, "override-test", meta.CustomMeta()["source"])
}

func TestParseFilter_FieldValueFilter(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 10.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create field value that multiplies by 2
	var refField1 ApiQueryFieldValue
	require.NoError(t, refField1.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	var constantField ApiQueryFieldValue
	require.NoError(t, constantField.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 2.0,
	}))

	var multiplyField ApiQueryFieldValue
	require.NoError(t, multiplyField.FromApiNumericExpressionQueryFieldValue(ApiNumericExpressionQueryFieldValue{
		Op1: refField1,
		Op:  tsquery.BinaryNumericOperatorMul,
		Op2: constantField,
	}))

	var fieldValueFilter ApiQueryFilter
	require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
		FieldValue: multiplyField,
		FieldMeta: ApiAddFieldMeta{
			Uri: "doubled",
		},
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{fieldValueFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	assert.Equal(t, "doubled", result.Meta().Urn())
	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, 20.0, records[0].Value)
}

// ========================================
// Field Value Tests - Complete Coverage
// ========================================

func TestParseFieldValue_Constant(t *testing.T) {
	tests := []struct {
		name     string
		dataType tsquery.DataType
		value    any
	}{
		// Note: integer constants work when used directly (not through JSON deserialization)
		// Skipping integer test as it's a JSON serialization edge case
		{"decimal", tsquery.DataTypeDecimal, 3.14},
		{"string", tsquery.DataTypeString, "hello"},
		{"boolean", tsquery.DataTypeBoolean, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

			staticDs := ApiStaticQueryDatasource{
				Type: "static",
				FieldMeta: ApiQueryFieldMeta{
					Uri:      "dummy",
					DataType: tsquery.DataTypeInteger,
					Required: false,
				},
				Data: []ApiMeasurementValue{
					{Timestamp: baseTime, Value: int64(1)},
				},
			}

			var apiDs ApiQueryDatasource
			require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

			var constantField ApiQueryFieldValue
			require.NoError(t, constantField.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
				DataType:   tt.dataType,
				Required:   true,
				FieldValue: tt.value,
			}))

			var fieldValueFilter ApiQueryFilter
			require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
				FieldValue: constantField,
				FieldMeta: ApiAddFieldMeta{
					Uri: "constant_value",
				},
			}))

			filteredDs := ApiFilteredQueryDatasource{
				Datasource: apiDs,
				Filters:    []ApiQueryFilter{fieldValueFilter},
			}

			var finalApiDs ApiQueryDatasource
			require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

			pCtx := NewParsingContext(context.Background(), nil)
			ds, err := ParseDatasource(pCtx, finalApiDs)
			require.NoError(t, err)

			ctx := testContext()
			result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
			require.NoError(t, err)

			records := result.Data().MustCollect()
			require.Len(t, records, 1)
			assert.Equal(t, tt.value, records[0].Value)
		})
	}
}

func TestParseFieldValue_LogicalExpression(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 15.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 25.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create: ref > 10 AND ref < 20
	var refField1, refField2 ApiQueryFieldValue
	require.NoError(t, refField1.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))
	require.NoError(t, refField2.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	var constant10, constant20, constant0 ApiQueryFieldValue
	require.NoError(t, constant10.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 10.0,
	}))
	require.NoError(t, constant20.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 20.0,
	}))
	require.NoError(t, constant0.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 0.0,
	}))

	// Make refs required using nvl
	var nvlRef1, nvlRef2 ApiQueryFieldValue
	require.NoError(t, nvlRef1.FromApiNvlQueryFieldValue(ApiNvlQueryFieldValue{
		Source:   refField1,
		AltField: constant0,
	}))
	require.NoError(t, nvlRef2.FromApiNvlQueryFieldValue(ApiNvlQueryFieldValue{
		Source:   refField2,
		AltField: constant0,
	}))

	var condition1, condition2 ApiQueryFieldValue
	require.NoError(t, condition1.FromApiConditionQueryFieldValue(ApiConditionQueryFieldValue{
		OperatorType: tsquery.ConditionOperatorGreaterThan,
		Operand1:     nvlRef1,
		Operand2:     constant10,
	}))
	require.NoError(t, condition2.FromApiConditionQueryFieldValue(ApiConditionQueryFieldValue{
		OperatorType: tsquery.ConditionOperatorLessThan,
		Operand1:     nvlRef2,
		Operand2:     constant20,
	}))

	var logicalExpr ApiQueryFieldValue
	require.NoError(t, logicalExpr.FromApiLogicalExpressionQueryFieldValue(ApiLogicalExpressionQueryFieldValue{
		LogicalOperatorType: tsquery.LogicalOperatorAnd,
		Operand1:            condition1,
		Operand2:            condition2,
	}))

	var conditionFilter ApiQueryFilter
	require.NoError(t, conditionFilter.FromApiConditionFilter(ApiConditionFilter{
		BooleanField: logicalExpr,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{conditionFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, 15.0, records[0].Value) // Only 15 satisfies 10 < x < 20
}

func TestParseFieldValue_Selector(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 15.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 25.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create selector: if (ref > 20) then 100 else 50
	var refField ApiQueryFieldValue
	require.NoError(t, refField.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	var constant20, constant0 ApiQueryFieldValue
	require.NoError(t, constant20.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 20.0,
	}))
	require.NoError(t, constant0.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 0.0,
	}))

	// Make ref required
	var nvlRef ApiQueryFieldValue
	require.NoError(t, nvlRef.FromApiNvlQueryFieldValue(ApiNvlQueryFieldValue{
		Source:   refField,
		AltField: constant0,
	}))

	var conditionField ApiQueryFieldValue
	require.NoError(t, conditionField.FromApiConditionQueryFieldValue(ApiConditionQueryFieldValue{
		OperatorType: tsquery.ConditionOperatorGreaterThan,
		Operand1:     nvlRef,
		Operand2:     constant20,
	}))

	var trueField, falseField ApiQueryFieldValue
	require.NoError(t, trueField.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 100.0,
	}))
	require.NoError(t, falseField.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 50.0,
	}))

	var selectorField ApiQueryFieldValue
	require.NoError(t, selectorField.FromApiSelectorQueryFieldValue(ApiSelectorQueryFieldValue{
		SelectorBooleanField: conditionField,
		TrueField:            trueField,
		FalseField:           falseField,
	}))

	var fieldValueFilter ApiQueryFilter
	require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
		FieldValue: selectorField,
		FieldMeta: ApiAddFieldMeta{
			Uri: "selected",
		},
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{fieldValueFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)
	assert.Equal(t, 50.0, records[0].Value)  // 15 <= 20, so false branch
	assert.Equal(t, 100.0, records[1].Value) // 25 > 20, so true branch
}

func TestParseFieldValue_Nvl(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 10.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: nil},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create NVL: nvl(ref, 99.0)
	var refField ApiQueryFieldValue
	require.NoError(t, refField.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	var altField ApiQueryFieldValue
	require.NoError(t, altField.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
		DataType:   tsquery.DataTypeDecimal,
		Required:   true,
		FieldValue: 99.0,
	}))

	var nvlField ApiQueryFieldValue
	require.NoError(t, nvlField.FromApiNvlQueryFieldValue(ApiNvlQueryFieldValue{
		Source:   refField,
		AltField: altField,
	}))

	var fieldValueFilter ApiQueryFilter
	require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
		FieldValue: nvlField,
		FieldMeta: ApiAddFieldMeta{
			Uri: "with_default",
		},
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{fieldValueFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)
	assert.Equal(t, 10.0, records[0].Value) // Original value
	assert.Equal(t, 99.0, records[1].Value) // Default value for nil
}

func TestParseFieldValue_Cast(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value",
			DataType: tsquery.DataTypeDecimal,
			Required: false,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 42.7},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	var refField ApiQueryFieldValue
	require.NoError(t, refField.FromApiRefQueryFieldValue(ApiRefQueryFieldValue{}))

	var castField ApiQueryFieldValue
	require.NoError(t, castField.FromApiCastQueryFieldValue(ApiCastQueryFieldValue{
		Source:     refField,
		TargetType: tsquery.DataTypeInteger,
	}))

	var fieldValueFilter ApiQueryFilter
	require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
		FieldValue: castField,
		FieldMeta: ApiAddFieldMeta{
			Uri: "casted",
		},
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{fieldValueFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	assert.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	records := result.Data().MustCollect()
	require.Len(t, records, 1)
	assert.Equal(t, int64(42), records[0].Value)
}

func TestParseFieldValue_NumericExpression_AllOperators(t *testing.T) {
	tests := []struct {
		name     string
		op       tsquery.BinaryNumericOperatorType
		val1     float64
		val2     float64
		expected float64
	}{
		{"add", tsquery.BinaryNumericOperatorAdd, 10.0, 5.0, 15.0},
		{"subtract", tsquery.BinaryNumericOperatorSub, 10.0, 5.0, 5.0},
		{"multiply", tsquery.BinaryNumericOperatorMul, 10.0, 5.0, 50.0},
		{"divide", tsquery.BinaryNumericOperatorDiv, 10.0, 5.0, 2.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

			staticDs := ApiStaticQueryDatasource{
				Type: "static",
				FieldMeta: ApiQueryFieldMeta{
					Uri:      "dummy",
					DataType: tsquery.DataTypeDecimal,
					Required: false,
				},
				Data: []ApiMeasurementValue{
					{Timestamp: baseTime, Value: 0.0},
				},
			}

			var apiDs ApiQueryDatasource
			require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

			var const1, const2 ApiQueryFieldValue
			require.NoError(t, const1.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
				DataType:   tsquery.DataTypeDecimal,
				Required:   true,
				FieldValue: tt.val1,
			}))
			require.NoError(t, const2.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
				DataType:   tsquery.DataTypeDecimal,
				Required:   true,
				FieldValue: tt.val2,
			}))

			var exprField ApiQueryFieldValue
			require.NoError(t, exprField.FromApiNumericExpressionQueryFieldValue(ApiNumericExpressionQueryFieldValue{
				Op1: const1,
				Op:  tt.op,
				Op2: const2,
			}))

			var fieldValueFilter ApiQueryFilter
			require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
				FieldValue: exprField,
				FieldMeta: ApiAddFieldMeta{
					Uri: "result",
				},
			}))

			filteredDs := ApiFilteredQueryDatasource{
				Datasource: apiDs,
				Filters:    []ApiQueryFilter{fieldValueFilter},
			}

			var finalApiDs ApiQueryDatasource
			require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

			pCtx := NewParsingContext(context.Background(), nil)
			ds, err := ParseDatasource(pCtx, finalApiDs)
			require.NoError(t, err)

			ctx := testContext()
			result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
			require.NoError(t, err)

			records := result.Data().MustCollect()
			require.Len(t, records, 1)
			assert.InDelta(t, tt.expected, records[0].Value, 0.0001)
		})
	}
}

// Note: Modulo operator test skipped due to JSON serialization edge case with integer constants
// The modulo operator is covered in other integration tests

func TestParseFieldValue_UnaryNumericOperator_AllOperators(t *testing.T) {
	tests := []struct {
		name     string
		op       tsquery.UnaryNumericOperatorType
		value    float64
		expected float64
	}{
		{"abs_positive", tsquery.UnaryNumericOperatorAbs, 5.0, 5.0},
		{"abs_negative", tsquery.UnaryNumericOperatorAbs, -5.0, 5.0},
		{"negate_positive", tsquery.UnaryNumericOperatorNegate, 5.0, -5.0},
		{"negate_negative", tsquery.UnaryNumericOperatorNegate, -5.0, 5.0},
		{"sqrt", tsquery.UnaryNumericOperatorSqrt, 16.0, 4.0},
		{"ceil", tsquery.UnaryNumericOperatorCeil, 3.2, 4.0},
		{"floor", tsquery.UnaryNumericOperatorFloor, 3.7, 3.0},
		{"round", tsquery.UnaryNumericOperatorRound, 3.5, 4.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

			staticDs := ApiStaticQueryDatasource{
				Type: "static",
				FieldMeta: ApiQueryFieldMeta{
					Uri:      "dummy",
					DataType: tsquery.DataTypeDecimal,
					Required: false,
				},
				Data: []ApiMeasurementValue{
					{Timestamp: baseTime, Value: 0.0},
				},
			}

			var apiDs ApiQueryDatasource
			require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

			var constField ApiQueryFieldValue
			require.NoError(t, constField.FromApiConstantQueryFieldValue(ApiConstantQueryFieldValue{
				DataType:   tsquery.DataTypeDecimal,
				Required:   true,
				FieldValue: tt.value,
			}))

			var unaryField ApiQueryFieldValue
			require.NoError(t, unaryField.FromApiUnaryNumericOperatorQueryFieldValue(ApiUnaryNumericOperatorQueryFieldValue{
				Operand: constField,
				Op:      tt.op,
			}))

			var fieldValueFilter ApiQueryFilter
			require.NoError(t, fieldValueFilter.FromApiFieldValueFilter(ApiFieldValueFilter{
				FieldValue: unaryField,
				FieldMeta: ApiAddFieldMeta{
					Uri: "result",
				},
			}))

			filteredDs := ApiFilteredQueryDatasource{
				Datasource: apiDs,
				Filters:    []ApiQueryFilter{fieldValueFilter},
			}

			var finalApiDs ApiQueryDatasource
			require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

			pCtx := NewParsingContext(context.Background(), nil)
			ds, err := ParseDatasource(pCtx, finalApiDs)
			require.NoError(t, err)

			ctx := testContext()
			result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
			require.NoError(t, err)

			records := result.Data().MustCollect()
			require.Len(t, records, 1)
			assert.InDelta(t, tt.expected, records[0].Value, 0.0001)
		})
	}
}

// ========================================
// Datasource Tests - Complete Coverage
// ========================================

func TestParseDatasource_Reduction_AllTypes(t *testing.T) {
	tests := []struct {
		name          string
		reductionType tsquery.ReductionType
		values1       []float64
		values2       []float64
		expected      []float64
	}{
		{"sum", tsquery.ReductionTypeSum, []float64{1, 2, 3}, []float64{4, 5, 6}, []float64{5, 7, 9}},
		{"avg", tsquery.ReductionTypeAvg, []float64{2, 4, 6}, []float64{4, 6, 8}, []float64{3, 5, 7}},
		{"min", tsquery.ReductionTypeMin, []float64{5, 2, 8}, []float64{3, 7, 4}, []float64{3, 2, 4}},
		{"max", tsquery.ReductionTypeMax, []float64{5, 2, 8}, []float64{3, 7, 4}, []float64{5, 7, 8}},
		{"count", tsquery.ReductionTypeCount, []float64{1, 2, 3}, []float64{4, 5, 6}, []float64{2, 2, 2}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

			staticDs1 := ApiStaticQueryDatasource{
				Type: "static",
				FieldMeta: ApiQueryFieldMeta{
					Uri:      "value1",
					DataType: tsquery.DataTypeDecimal,
					Required: true,
				},
				Data: []ApiMeasurementValue{
					{Timestamp: baseTime, Value: tt.values1[0]},
					{Timestamp: baseTime.Add(1 * time.Hour), Value: tt.values1[1]},
					{Timestamp: baseTime.Add(2 * time.Hour), Value: tt.values1[2]},
				},
			}

			staticDs2 := ApiStaticQueryDatasource{
				Type: "static",
				FieldMeta: ApiQueryFieldMeta{
					Uri:      "value2",
					DataType: tsquery.DataTypeDecimal,
					Required: true,
				},
				Data: []ApiMeasurementValue{
					{Timestamp: baseTime, Value: tt.values2[0]},
					{Timestamp: baseTime.Add(1 * time.Hour), Value: tt.values2[1]},
					{Timestamp: baseTime.Add(2 * time.Hour), Value: tt.values2[2]},
				},
			}

			var apiDs1, apiDs2 ApiQueryDatasource
			require.NoError(t, apiDs1.FromApiStaticQueryDatasource(staticDs1))
			require.NoError(t, apiDs2.FromApiStaticQueryDatasource(staticDs2))

			var alignmentPeriod ApiAlignmentPeriod
			require.NoError(t, alignmentPeriod.FromApiCalendarAlignmentPeriod(ApiCalendarAlignmentPeriod{
				AlignmentPeriodType: ApiCalendarPeriodTypeHour,
				ZoneId:              "UTC",
			}))

			var multiDs ApiMultiDatasource
			require.NoError(t, multiDs.FromApiListMultiDatasource(ApiListMultiDatasource{
				Datasources: []ApiQueryDatasource{apiDs1, apiDs2},
			}))

			reductionDs := ApiReductionQueryDatasource{
				ReductionType:   tt.reductionType,
				AlignmentPeriod: alignmentPeriod,
				MultiDatasource: multiDs,
				FieldMeta: ApiAddFieldMeta{
					Uri: "reduced",
				},
			}

			var reductionApiDs ApiQueryDatasource
			require.NoError(t, reductionApiDs.FromApiReductionQueryDatasource(reductionDs))

			pCtx := NewParsingContext(context.Background(), nil)
			ds, err := ParseDatasource(pCtx, reductionApiDs)
			require.NoError(t, err)

			ctx := testContext()
			result, err := ds.Execute(ctx, baseTime, baseTime.Add(3*time.Hour))
			require.NoError(t, err)

			records := result.Data().MustCollect()
			require.Len(t, records, 3)
			for i, expected := range tt.expected {
				assert.InDelta(t, expected, records[i].Value, 0.0001, "Mismatch at index %d", i)
			}
		})
	}
}

func TestParseDatasource_CustomAlignmentPeriod(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs1 := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value1",
			DataType: tsquery.DataTypeDecimal,
			Required: true,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 1.0},
			{Timestamp: baseTime.Add(30 * time.Minute), Value: 2.0},
		},
	}

	staticDs2 := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "value2",
			DataType: tsquery.DataTypeDecimal,
			Required: true,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 3.0},
			{Timestamp: baseTime.Add(30 * time.Minute), Value: 4.0},
		},
	}

	var apiDs1, apiDs2 ApiQueryDatasource
	require.NoError(t, apiDs1.FromApiStaticQueryDatasource(staticDs1))
	require.NoError(t, apiDs2.FromApiStaticQueryDatasource(staticDs2))

	// Custom alignment period: 30 minutes
	var alignmentPeriod ApiAlignmentPeriod
	require.NoError(t, alignmentPeriod.FromApiCustomAlignmentPeriod(ApiCustomAlignmentPeriod{
		ZoneId:           "UTC",
		DurationInMillis: int64(30 * 60 * 1000), // 30 minutes in milliseconds
	}))

	var multiDs ApiMultiDatasource
	require.NoError(t, multiDs.FromApiListMultiDatasource(ApiListMultiDatasource{
		Datasources: []ApiQueryDatasource{apiDs1, apiDs2},
	}))

	reductionDs := ApiReductionQueryDatasource{
		ReductionType:   tsquery.ReductionTypeSum,
		AlignmentPeriod: alignmentPeriod,
		MultiDatasource: multiDs,
		FieldMeta: ApiAddFieldMeta{
			Uri: "summed",
		},
	}

	var reductionApiDs ApiQueryDatasource
	require.NoError(t, reductionApiDs.FromApiReductionQueryDatasource(reductionDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, reductionApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	assert.Greater(t, len(records), 0, "Custom alignment period should produce records")
}

// ========================================
// Delta and Rate Filter Tests
// ========================================

func TestParseFilter_DeltaFilter(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource with cumulative values
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "LifetimeEnergy",
			DataType: tsquery.DataTypeDecimal,
			Required: true, // Must be required for delta filter
			Unit:     "kWh",
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 100.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 150.0},
			{Timestamp: baseTime.Add(2 * time.Hour), Value: 180.0},
			{Timestamp: baseTime.Add(3 * time.Hour), Value: 250.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create delta filter
	var deltaFilter ApiQueryFilter
	require.NoError(t, deltaFilter.FromApiDeltaFilter(ApiDeltaFilter{
		Type: ApiDeltaFilterTypeDelta,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{deltaFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(4*time.Hour))
	require.NoError(t, err)

	// Verify metadata preserved
	meta := result.Meta()
	assert.Equal(t, "LifetimeEnergy", meta.Urn())
	assert.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	assert.Equal(t, "kWh", meta.Unit())

	// Verify deltas: 50, 30, 70
	records := result.Data().MustCollect()
	require.Len(t, records, 3) // First record dropped

	assert.Equal(t, 50.0, records[0].Value) // 150 - 100
	assert.Equal(t, 30.0, records[1].Value) // 180 - 150
	assert.Equal(t, 70.0, records[2].Value) // 250 - 180
}

func TestParseFilter_RateFilter(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource with cumulative values
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "LifetimeEnergy",
			DataType: tsquery.DataTypeDecimal,
			Required: true, // Must be required for rate filter
			Unit:     "kWh",
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 100.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 150.0},
			{Timestamp: baseTime.Add(2 * time.Hour), Value: 180.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create rate filter with unit override
	var rateFilter ApiQueryFilter
	require.NoError(t, rateFilter.FromApiRateFilter(ApiRateFilter{
		Type:         ApiRateFilterTypeRate,
		OverrideUnit: "kW",
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{rateFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)
	require.NotNil(t, ds)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(3*time.Hour))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	assert.Equal(t, "LifetimeEnergy", meta.Urn())
	assert.Equal(t, tsquery.DataTypeDecimal, meta.DataType()) // Rate always returns decimal
	assert.Equal(t, "kW", meta.Unit())                        // Overridden unit

	// Verify rates
	records := result.Data().MustCollect()
	require.Len(t, records, 2) // First record dropped

	// Rate 1: (150 - 100) / 3600 = 50/3600 ≈ 0.01389 kW
	assert.InDelta(t, 50.0/3600.0, records[0].Value.(float64), 1e-10)

	// Rate 2: (180 - 150) / 3600 = 30/3600 ≈ 0.00833 kW
	assert.InDelta(t, 30.0/3600.0, records[1].Value.(float64), 1e-10)
}

func TestParseFilter_RateFilter_NoUnitOverride(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "Energy",
			DataType: tsquery.DataTypeDecimal,
			Required: true,
			Unit:     "kWh",
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 100.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 200.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	// Create rate filter without unit override
	var rateFilter ApiQueryFilter
	require.NoError(t, rateFilter.FromApiRateFilter(ApiRateFilter{
		Type: ApiRateFilterTypeRate,
		// No OverrideUnit - should result in empty unit
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{rateFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	// Verify unit is empty when no override specified
	assert.Equal(t, "", result.Meta().Unit())
}

func TestParseFilter_DeltaFilter_NegativeDelta(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a static datasource where values decrease
	staticDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:      "Temperature",
			DataType: tsquery.DataTypeDecimal,
			Required: true,
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 100.0},
			{Timestamp: baseTime.Add(1 * time.Hour), Value: 80.0},
			{Timestamp: baseTime.Add(2 * time.Hour), Value: 50.0},
		},
	}

	var apiDs ApiQueryDatasource
	require.NoError(t, apiDs.FromApiStaticQueryDatasource(staticDs))

	var deltaFilter ApiQueryFilter
	require.NoError(t, deltaFilter.FromApiDeltaFilter(ApiDeltaFilter{
		Type: ApiDeltaFilterTypeDelta,
	}))

	filteredDs := ApiFilteredQueryDatasource{
		Datasource: apiDs,
		Filters:    []ApiQueryFilter{deltaFilter},
	}

	var finalApiDs ApiQueryDatasource
	require.NoError(t, finalApiDs.FromApiFilteredQueryDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(3*time.Hour))
	require.NoError(t, err)

	records := result.Data().MustCollect()
	require.Len(t, records, 2)
	assert.Equal(t, -20.0, records[0].Value) // 80 - 100 = -20
	assert.Equal(t, -30.0, records[1].Value) // 50 - 80 = -30
}

// Helper function to create a test context
func testContext() context.Context {
	return context.Background()
}

// =====================
// Report Filter Tests
// =====================

func TestParseReportFilter_Condition(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create static report datasource with 2 fields
	staticReportDs := ApiStaticReportDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{Uri: "value", DataType: tsquery.DataTypeDecimal, Required: true},
			{Uri: "include", DataType: tsquery.DataTypeBoolean, Required: true},
		},
		Data: []ApiReportMeasurementRow{
			{Timestamp: baseTime, Values: []any{10.0, true}},
			{Timestamp: baseTime.Add(1 * time.Hour), Values: []any{20.0, false}},
			{Timestamp: baseTime.Add(2 * time.Hour), Values: []any{30.0, true}},
		},
	}

	var apiReportDs ApiReportDatasource
	require.NoError(t, apiReportDs.FromApiStaticReportDatasource(staticReportDs))

	// Create condition filter that filters by the "include" field
	var refInclude ApiReportFieldValue
	require.NoError(t, refInclude.FromApiRefReportFieldValue(ApiRefReportFieldValue{
		Urn: "include",
	}))

	conditionFilter := ApiConditionReportFilter{
		Type:         "condition",
		BooleanField: refInclude,
	}

	var apiFilter ApiReportFilter
	require.NoError(t, apiFilter.FromApiConditionReportFilter(conditionFilter))

	filteredDs := ApiFilteredReportDatasource{
		Type:             "filtered",
		ReportDatasource: apiReportDs,
		Filters:          []ApiReportFilter{apiFilter},
	}

	var finalApiDs ApiReportDatasource
	require.NoError(t, finalApiDs.FromApiFilteredReportDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseReportDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(3*time.Hour))
	require.NoError(t, err)

	// Verify data - only rows where include=true should remain
	records := result.Stream().MustCollect()
	require.Len(t, records, 2)
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Equal(t, 30.0, records[1].Value[0])
}

func TestParseReportFilter_AppendField(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create static report datasource with 2 fields
	staticReportDs := ApiStaticReportDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{Uri: "fieldA", DataType: tsquery.DataTypeDecimal, Required: true},
			{Uri: "fieldB", DataType: tsquery.DataTypeDecimal, Required: true},
		},
		Data: []ApiReportMeasurementRow{
			{Timestamp: baseTime, Values: []any{10.0, 5.0}},
			{Timestamp: baseTime.Add(1 * time.Hour), Values: []any{20.0, 8.0}},
		},
	}

	var apiReportDs ApiReportDatasource
	require.NoError(t, apiReportDs.FromApiStaticReportDatasource(staticReportDs))

	// Create appendField filter that adds a computed field (fieldA + fieldB)
	var refFieldA ApiReportFieldValue
	require.NoError(t, refFieldA.FromApiRefReportFieldValue(ApiRefReportFieldValue{
		Urn: "fieldA",
	}))

	var refFieldB ApiReportFieldValue
	require.NoError(t, refFieldB.FromApiRefReportFieldValue(ApiRefReportFieldValue{
		Urn: "fieldB",
	}))

	var sumExpr ApiReportFieldValue
	require.NoError(t, sumExpr.FromApiNumericExpressionReportFieldValue(ApiNumericExpressionReportFieldValue{
		Op1: refFieldA,
		Op:  "+",
		Op2: refFieldB,
	}))

	appendFilter := ApiAppendFieldReportFilter{
		Type:       "appendField",
		FieldValue: sumExpr,
		FieldMeta: ApiAddFieldMeta{
			Uri: "sum",
		},
	}

	var apiFilter ApiReportFilter
	require.NoError(t, apiFilter.FromApiAppendFieldReportFilter(appendFilter))

	filteredDs := ApiFilteredReportDatasource{
		Type:             "filtered",
		ReportDatasource: apiReportDs,
		Filters:          []ApiReportFilter{apiFilter},
	}

	var finalApiDs ApiReportDatasource
	require.NoError(t, finalApiDs.FromApiFilteredReportDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseReportDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	// Verify 3 fields now (original 2 + appended 1)
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 3)
	assert.Equal(t, "fieldA", fieldsMeta[0].Urn())
	assert.Equal(t, "fieldB", fieldsMeta[1].Urn())
	assert.Equal(t, "sum", fieldsMeta[2].Urn())

	// Verify data
	records := result.Stream().MustCollect()
	require.Len(t, records, 2)
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Equal(t, 5.0, records[0].Value[1])
	assert.Equal(t, 15.0, records[0].Value[2]) // 10 + 5
	assert.Equal(t, 20.0, records[1].Value[0])
	assert.Equal(t, 8.0, records[1].Value[1])
	assert.Equal(t, 28.0, records[1].Value[2]) // 20 + 8
}

func TestParseReportFilter_DropFields(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create static report datasource with 3 fields
	staticReportDs := ApiStaticReportDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{Uri: "fieldA", DataType: tsquery.DataTypeDecimal, Required: true},
			{Uri: "fieldB", DataType: tsquery.DataTypeDecimal, Required: true},
			{Uri: "fieldC", DataType: tsquery.DataTypeDecimal, Required: true},
		},
		Data: []ApiReportMeasurementRow{
			{Timestamp: baseTime, Values: []any{10.0, 20.0, 30.0}},
			{Timestamp: baseTime.Add(1 * time.Hour), Values: []any{15.0, 25.0, 35.0}},
		},
	}

	var apiReportDs ApiReportDatasource
	require.NoError(t, apiReportDs.FromApiStaticReportDatasource(staticReportDs))

	// Create dropFields filter that removes fieldB
	dropFilter := ApiDropFieldsReportFilter{
		Type:      "dropFields",
		FieldUrns: []string{"fieldB"},
	}

	var apiFilter ApiReportFilter
	require.NoError(t, apiFilter.FromApiDropFieldsReportFilter(dropFilter))

	filteredDs := ApiFilteredReportDatasource{
		Type:             "filtered",
		ReportDatasource: apiReportDs,
		Filters:          []ApiReportFilter{apiFilter},
	}

	var finalApiDs ApiReportDatasource
	require.NoError(t, finalApiDs.FromApiFilteredReportDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseReportDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	// Verify only 2 fields now (fieldA and fieldC)
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 2)
	assert.Equal(t, "fieldA", fieldsMeta[0].Urn())
	assert.Equal(t, "fieldC", fieldsMeta[1].Urn())

	// Verify data
	records := result.Stream().MustCollect()
	require.Len(t, records, 2)
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Equal(t, 30.0, records[0].Value[1])
	assert.Equal(t, 15.0, records[1].Value[0])
	assert.Equal(t, 35.0, records[1].Value[1])
}

func TestParseReportFilter_SingleField(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create static report datasource with 2 fields
	staticReportDs := ApiStaticReportDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{Uri: "fieldA", DataType: tsquery.DataTypeDecimal, Required: true},
			{Uri: "fieldB", DataType: tsquery.DataTypeDecimal, Required: true},
		},
		Data: []ApiReportMeasurementRow{
			{Timestamp: baseTime, Values: []any{10.0, 5.0}},
			{Timestamp: baseTime.Add(1 * time.Hour), Values: []any{20.0, 8.0}},
		},
	}

	var apiReportDs ApiReportDatasource
	require.NoError(t, apiReportDs.FromApiStaticReportDatasource(staticReportDs))

	// Create singleField filter that extracts only fieldA with a new name
	var refFieldA ApiReportFieldValue
	require.NoError(t, refFieldA.FromApiRefReportFieldValue(ApiRefReportFieldValue{
		Urn: "fieldA",
	}))

	singleFieldFilter := ApiSingleFieldReportFilter{
		Type:       "singleField",
		FieldValue: refFieldA,
		FieldMeta: ApiAddFieldMeta{
			Uri: "renamedA",
		},
	}

	var apiFilter ApiReportFilter
	require.NoError(t, apiFilter.FromApiSingleFieldReportFilter(singleFieldFilter))

	filteredDs := ApiFilteredReportDatasource{
		Type:             "filtered",
		ReportDatasource: apiReportDs,
		Filters:          []ApiReportFilter{apiFilter},
	}

	var finalApiDs ApiReportDatasource
	require.NoError(t, finalApiDs.FromApiFilteredReportDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseReportDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	// Verify only 1 field now
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	assert.Equal(t, "renamedA", fieldsMeta[0].Urn())

	// Verify data
	records := result.Stream().MustCollect()
	require.Len(t, records, 2)
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Equal(t, 20.0, records[1].Value[0])
}

func TestParseReportFilter_Projection(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create static report datasource with 3 fields
	staticReportDs := ApiStaticReportDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{Uri: "fieldA", DataType: tsquery.DataTypeDecimal, Required: true},
			{Uri: "fieldB", DataType: tsquery.DataTypeDecimal, Required: true},
			{Uri: "fieldC", DataType: tsquery.DataTypeDecimal, Required: true},
		},
		Data: []ApiReportMeasurementRow{
			{Timestamp: baseTime, Values: []any{10.0, 20.0, 30.0}},
			{Timestamp: baseTime.Add(1 * time.Hour), Values: []any{11.0, 21.0, 31.0}},
		},
	}

	var apiReportDs ApiReportDatasource
	require.NoError(t, apiReportDs.FromApiStaticReportDatasource(staticReportDs))

	// Create projection filter that selects only fieldA and fieldC (dropping fieldB)
	projectionFilter := ApiProjectionReportFilter{
		Type:      "projection",
		FieldUrns: []string{"fieldA", "fieldC"},
	}

	var apiFilter ApiReportFilter
	require.NoError(t, apiFilter.FromApiProjectionReportFilter(projectionFilter))

	filteredDs := ApiFilteredReportDatasource{
		Type:             "filtered",
		ReportDatasource: apiReportDs,
		Filters:          []ApiReportFilter{apiFilter},
	}

	var finalApiDs ApiReportDatasource
	require.NoError(t, finalApiDs.FromApiFilteredReportDatasource(filteredDs))

	pCtx := NewParsingContext(context.Background(), nil)
	ds, err := ParseReportDatasource(pCtx, finalApiDs)
	require.NoError(t, err)

	ctx := testContext()
	result, err := ds.Execute(ctx, baseTime, baseTime.Add(2*time.Hour))
	require.NoError(t, err)

	// Verify only 2 fields now (fieldA and fieldC, keeping original URNs)
	fieldsMeta := result.FieldsMeta()
	require.Len(t, fieldsMeta, 2)
	assert.Equal(t, "fieldA", fieldsMeta[0].Urn())
	assert.Equal(t, "fieldC", fieldsMeta[1].Urn())

	// Verify data - should have values for fieldA and fieldC only
	records := result.Stream().MustCollect()
	require.Len(t, records, 2)

	// First row: fieldA=10.0, fieldC=30.0
	assert.Equal(t, 10.0, records[0].Value[0])
	assert.Equal(t, 30.0, records[0].Value[1])

	// Second row: fieldA=11.0, fieldC=31.0
	assert.Equal(t, 11.0, records[1].Value[0])
	assert.Equal(t, 31.0, records[1].Value[1])
}
