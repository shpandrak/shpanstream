package tsquery_test

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Helper to create a datasource with a single boolean field
func createBooleanDatasource(t *testing.T, urn string, timestamps []time.Time, values []bool) datasource.DataSource {
	require.Equal(t, len(timestamps), len(values))

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(urn, tsquery.DataTypeBoolean, true, "", nil)
	require.NoError(t, err)

	records := make([]timeseries.TsRecord[any], len(timestamps))
	for i := range timestamps {
		records[i] = timeseries.TsRecord[any]{
			Timestamp: timestamps[i],
			Value:     values[i],
		}
	}

	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	return ds
}

// Helper to create a datasource with a single integer field
func createIntegerDatasource(t *testing.T, urn string, timestamps []time.Time, values []int64) datasource.DataSource {
	require.Equal(t, len(timestamps), len(values))

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(urn, tsquery.DataTypeInteger, true, "", nil)
	require.NoError(t, err)

	records := make([]timeseries.TsRecord[any], len(timestamps))
	for i := range timestamps {
		records[i] = timeseries.TsRecord[any]{
			Timestamp: timestamps[i],
			Value:     values[i],
		}
	}

	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	return ds
}

// Helper to create constant field value for datasource
func createConstantDatasourceField(dataType tsquery.DataType, value any) datasource.Value {
	valueMeta := tsquery.ValueMeta{DataType: dataType, Required: true}
	return datasource.NewConstantFieldValue(valueMeta, value)
}

// --- Boolean to String Selector Test ---

func TestSelectorDatasource_BooleanToString_Production(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Create boolean datasource: IsProduction
	values := []bool{true, false, true, false}
	ds := createBooleanDatasource(t, "IsProduction", timestamps, values)

	// Create selector: if IsProduction then "production" else "development"
	selectorField := datasource.NewRefFieldValue() // The boolean field itself
	trueField := createConstantDatasourceField(tsquery.DataTypeString, "production")
	falseField := createConstantDatasourceField(tsquery.DataTypeString, "development")

	selector := datasource.NewSelectorFieldValue(selectorField, trueField, falseField)

	// Apply selector using FieldValueFilter
	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{Urn: "environment"})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "environment", meta.Urn())
	require.Equal(t, tsquery.DataTypeString, meta.DataType())
	require.True(t, meta.Required())

	// Verify values
	records := result.Data().MustCollect()
	require.Len(t, records, 4)
	require.Equal(t, "production", records[0].Value)
	require.Equal(t, "development", records[1].Value)
	require.Equal(t, "production", records[2].Value)
	require.Equal(t, "development", records[3].Value)

	// Verify timestamps preserved
	require.Equal(t, timestamps[0], records[0].Timestamp)
	require.Equal(t, timestamps[1], records[1].Timestamp)
	require.Equal(t, timestamps[2], records[2].Timestamp)
	require.Equal(t, timestamps[3], records[3].Timestamp)
}

// --- Integer Threshold to String Selector Test ---

func TestSelectorDatasource_IntegerThreshold_HighLow(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
		baseTime.Add(4 * time.Hour),
	}

	// Create integer datasource: RequestCount
	values := []int64{50, 150, 75, 200, 90}
	ds := createIntegerDatasource(t, "RequestCount", timestamps, values)

	// Create condition: RequestCount > 100
	refField := datasource.NewRefFieldValue()
	threshold := createConstantDatasourceField(tsquery.DataTypeInteger, int64(100))
	conditionField := datasource.NewConditionFieldValue(
		tsquery.ConditionOperatorGreaterThan,
		refField,
		threshold,
	)

	// Create selector: if (RequestCount > 100) then "high" else "low"
	highValue := createConstantDatasourceField(tsquery.DataTypeString, "high")
	lowValue := createConstantDatasourceField(tsquery.DataTypeString, "low")
	selector := datasource.NewSelectorFieldValue(conditionField, highValue, lowValue)

	// Apply selector
	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{Urn: "load_level"})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "load_level", meta.Urn())
	require.Equal(t, tsquery.DataTypeString, meta.DataType())
	require.True(t, meta.Required())

	// Verify values: 50 (low), 150 (high), 75 (low), 200 (high), 90 (low)
	records := result.Data().MustCollect()
	require.Len(t, records, 5)
	require.Equal(t, "low", records[0].Value)  // 50 <= 100
	require.Equal(t, "high", records[1].Value) // 150 > 100
	require.Equal(t, "low", records[2].Value)  // 75 <= 100
	require.Equal(t, "high", records[3].Value) // 200 > 100
	require.Equal(t, "low", records[4].Value)  // 90 <= 100
}

// --- Fun Math Test: Square Root Threshold ---

func TestSelectorDatasource_SquareRootThreshold_Fun(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
		baseTime.Add(3 * time.Hour),
	}

	// Create integer datasource with values: 64, 144, 81, 225
	// Square roots: 8, 12, 9, 15
	values := []int64{64, 144, 81, 225}
	ds := createIntegerDatasource(t, "Area", timestamps, values)

	// Create sqrt field: sqrt(Area)
	refField := datasource.NewRefFieldValue()
	sqrtField := datasource.NewUnaryNumericOperatorFieldValue(
		refField,
		tsquery.UnaryNumericOperatorSqrt,
	)

	// Create condition: sqrt(Area) > 10
	// Note: sqrt of integer returns integer (truncated)
	threshold := createConstantDatasourceField(tsquery.DataTypeInteger, int64(10))
	conditionField := datasource.NewConditionFieldValue(
		tsquery.ConditionOperatorGreaterThan,
		sqrtField,
		threshold,
	)

	// Create selector: if (sqrt(Area) > 10) then "large" else "small"
	largeValue := createConstantDatasourceField(tsquery.DataTypeString, "large")
	smallValue := createConstantDatasourceField(tsquery.DataTypeString, "small")
	selector := datasource.NewSelectorFieldValue(conditionField, largeValue, smallValue)

	// Apply selector
	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{Urn: "size_category"})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "size_category", meta.Urn())
	require.Equal(t, tsquery.DataTypeString, meta.DataType())
	require.True(t, meta.Required())

	// Verify values
	// sqrt(64) = int(8) <= 10 → "small"
	// sqrt(144) = int(12) > 10 → "large"
	// sqrt(81) = int(9) <= 10 → "small"
	// sqrt(225) = int(15) > 10 → "large"
	records := result.Data().MustCollect()
	require.Len(t, records, 4)
	require.Equal(t, "small", records[0].Value) // sqrt(64) = int(8)
	require.Equal(t, "large", records[1].Value) // sqrt(144) = int(12)
	require.Equal(t, "small", records[2].Value) // sqrt(81) = int(9)
	require.Equal(t, "large", records[3].Value) // sqrt(225) = int(15)
}

// --- Selector with Numeric Values (not just strings) ---

func TestSelectorDatasource_NumericSelector_Decimal(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{
		baseTime,
		baseTime.Add(1 * time.Hour),
		baseTime.Add(2 * time.Hour),
	}

	// Create boolean datasource: IsPremium
	values := []bool{true, false, true}
	ds := createBooleanDatasource(t, "IsPremium", timestamps, values)

	// Create selector: if IsPremium then 99.99 else 149.99
	selectorField := datasource.NewRefFieldValue()
	premiumPrice := createConstantDatasourceField(tsquery.DataTypeDecimal, 99.99)
	standardPrice := createConstantDatasourceField(tsquery.DataTypeDecimal, 149.99)
	selector := datasource.NewSelectorFieldValue(selectorField, premiumPrice, standardPrice)

	// Apply selector
	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{
		Urn:          "price",
		OverrideUnit: "usd",
	})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata
	meta := result.Meta()
	require.Equal(t, "price", meta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, meta.DataType())
	require.True(t, meta.Required())
	require.Equal(t, "usd", meta.Unit())

	// Verify values
	records := result.Data().MustCollect()
	require.Len(t, records, 3)
	require.Equal(t, 99.99, records[0].Value)  // premium
	require.Equal(t, 149.99, records[1].Value) // standard
	require.Equal(t, 99.99, records[2].Value)  // premium
}

// --- Error Tests ---

func TestSelectorDatasource_ErrorOnMismatchedTypes(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	ds := createBooleanDatasource(t, "IsActive", timestamps, []bool{true})

	// Try to create selector with mismatched types (decimal vs string)
	selectorField := datasource.NewRefFieldValue()
	trueField := createConstantDatasourceField(tsquery.DataTypeDecimal, 10.5)  // decimal
	falseField := createConstantDatasourceField(tsquery.DataTypeString, "off") // string
	selector := datasource.NewSelectorFieldValue(selectorField, trueField, falseField)

	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{Urn: "value"})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute - should fail
	_, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible datatypes")
}

func TestSelectorDatasource_ErrorOnMismatchedUnits(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	ds := createBooleanDatasource(t, "UseMetric", timestamps, []bool{true})

	// Create constant fields with different units
	metricMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true, Unit: "meters"}
	imperialMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeDecimal, Required: true, Unit: "feet"}

	selectorField := datasource.NewRefFieldValue()
	trueField := datasource.NewConstantFieldValue(metricMeta, 10.0)
	falseField := datasource.NewConstantFieldValue(imperialMeta, 30.0)
	selector := datasource.NewSelectorFieldValue(selectorField, trueField, falseField)

	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{Urn: "distance"})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute - should fail
	_, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible units")
}

func TestSelectorDatasource_ErrorOnNonBooleanSelector(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	timestamps := []time.Time{baseTime}

	// Create integer datasource (not boolean)
	ds := createIntegerDatasource(t, "Count", timestamps, []int64{42})

	// Try to use integer as selector
	selectorField := datasource.NewRefFieldValue() // Will be integer, not boolean
	trueField := createConstantDatasourceField(tsquery.DataTypeString, "yes")
	falseField := createConstantDatasourceField(tsquery.DataTypeString, "no")
	selector := datasource.NewSelectorFieldValue(selectorField, trueField, falseField)

	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{Urn: "result"})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute - should fail
	_, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "selector field must be boolean")
}

func TestSelectorDatasource_ErrorOnOptionalSelector(t *testing.T) {
	ctx := context.Background()
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	// Create a boolean datasource but make it optional
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("IsActive", tsquery.DataTypeBoolean, false, "", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{{Timestamp: baseTime, Value: true}}
	ds, err := datasource.NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Try to use optional boolean as selector
	selectorField := datasource.NewRefFieldValue()
	trueField := createConstantDatasourceField(tsquery.DataTypeString, "on")
	falseField := createConstantDatasourceField(tsquery.DataTypeString, "off")
	selector := datasource.NewSelectorFieldValue(selectorField, trueField, falseField)

	filter := datasource.NewFieldValueFilter(selector, tsquery.AddFieldMeta{Urn: "status"})
	filteredDS := datasource.NewFilteredDataSource(ds, filter)

	// Execute - should fail
	_, err = filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "selector field must be required")
}
