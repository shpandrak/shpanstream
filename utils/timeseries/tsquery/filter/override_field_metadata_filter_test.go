package filter

import (
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOverrideFieldMetadataFilter_OverrideUrnOnly(t *testing.T) {
	// Create original field metadata
	originalMeta, err := tsquery.NewFieldMetaWithCustomData(
		"original_field",
		tsquery.DataTypeDecimal,
		true,
		"celsius",
		map[string]any{"source": "sensor1"},
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{25.0}, Timestamp: time.Unix(0, 0)},
		{Value: []any{30.0}, Timestamp: time.Unix(60, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override URN only
	newUrn := "renamed_field"
	filter := NewOverrideFieldMetadataFilter("original_field", &newUrn, nil, nil)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify URN is updated
	require.Len(t, filteredResult.FieldsMeta(), 1)
	require.Equal(t, "renamed_field", filteredResult.FieldsMeta()[0].Urn())

	// Verify other metadata is preserved
	require.Equal(t, tsquery.DataTypeDecimal, filteredResult.FieldsMeta()[0].DataType())
	require.Equal(t, true, filteredResult.FieldsMeta()[0].Required())
	require.Equal(t, "celsius", filteredResult.FieldsMeta()[0].Unit())
	// Custom metadata is not preserved when not explicitly provided
	require.Equal(t, "", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("source"))

	// Verify stream data is unchanged
	collectedRecords := filteredResult.Stream().MustCollect()
	require.Len(t, collectedRecords, 2)
	require.Equal(t, []any{25.0}, collectedRecords[0].Value)
	require.Equal(t, []any{30.0}, collectedRecords[1].Value)
}

func TestOverrideFieldMetadataFilter_OverrideUnitOnly(t *testing.T) {
	// Create original field metadata
	originalMeta, err := tsquery.NewFieldMetaWithCustomData(
		"temperature",
		tsquery.DataTypeDecimal,
		true,
		"celsius",
		nil,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{25.0}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override unit only
	newUnit := "fahrenheit"
	filter := NewOverrideFieldMetadataFilter("temperature", nil, &newUnit, nil)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify unit is updated
	require.Equal(t, "fahrenheit", filteredResult.FieldsMeta()[0].Unit())

	// Verify other metadata is preserved
	require.Equal(t, "temperature", filteredResult.FieldsMeta()[0].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, filteredResult.FieldsMeta()[0].DataType())
	require.Equal(t, true, filteredResult.FieldsMeta()[0].Required())
}

func TestOverrideFieldMetadataFilter_OverrideCustomMetaOnly(t *testing.T) {
	// Create original field metadata with custom meta
	originalMeta, err := tsquery.NewFieldMetaWithCustomData(
		"sensor_reading",
		tsquery.DataTypeDecimal,
		true,
		"units",
		map[string]any{
			"source":   "sensor1",
			"location": "room1",
		},
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{42.0}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override and add custom metadata
	newCustomMeta := map[string]any{
		"location": "room2",      // Override existing
		"device":   "thermometer", // Add new
	}
	filter := NewOverrideFieldMetadataFilter("sensor_reading", nil, nil, newCustomMeta)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify custom metadata is replaced (not merged)
	require.Equal(t, "", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("source"))             // Not preserved
	require.Equal(t, "room2", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("location"))      // From new custom meta
	require.Equal(t, "thermometer", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("device")) // From new custom meta

	// Verify other metadata is preserved
	require.Equal(t, "sensor_reading", filteredResult.FieldsMeta()[0].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, filteredResult.FieldsMeta()[0].DataType())
	require.Equal(t, "units", filteredResult.FieldsMeta()[0].Unit())
}

func TestOverrideFieldMetadataFilter_OverrideMultipleProperties(t *testing.T) {
	// Create original field metadata
	originalMeta, err := tsquery.NewFieldMetaWithCustomData(
		"old_name",
		tsquery.DataTypeInteger,
		false,
		"old_unit",
		map[string]any{"key1": "value1"},
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{int64(100)}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override multiple properties
	newUrn := "new_name"
	newUnit := "new_unit"
	newCustomMeta := map[string]any{"key2": "value2"}

	filter := NewOverrideFieldMetadataFilter("old_name", &newUrn, &newUnit, newCustomMeta)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify all overrides applied
	require.Equal(t, "new_name", filteredResult.FieldsMeta()[0].Urn())
	require.Equal(t, "new_unit", filteredResult.FieldsMeta()[0].Unit())
	require.Equal(t, "", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("key1"))       // Not preserved
	require.Equal(t, "value2", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("key2")) // From new custom meta

	// Verify immutable properties unchanged
	require.Equal(t, tsquery.DataTypeInteger, filteredResult.FieldsMeta()[0].DataType())
	require.Equal(t, false, filteredResult.FieldsMeta()[0].Required())
}

func TestOverrideFieldMetadataFilter_NoOverrides(t *testing.T) {
	// Create original field metadata
	originalMeta, err := tsquery.NewFieldMetaWithCustomData(
		"field_name",
		tsquery.DataTypeString,
		true,
		"meters",
		map[string]any{"key": "value"},
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{"test"}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// No overrides (all nil)
	filter := NewOverrideFieldMetadataFilter("field_name", nil, nil, nil)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify immutable metadata is unchanged
	require.Equal(t, "field_name", filteredResult.FieldsMeta()[0].Urn())
	require.Equal(t, "meters", filteredResult.FieldsMeta()[0].Unit())
	require.Equal(t, tsquery.DataTypeString, filteredResult.FieldsMeta()[0].DataType())
	require.Equal(t, true, filteredResult.FieldsMeta()[0].Required())
	// Custom metadata is cleared when not provided
	require.Equal(t, "", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("key"))
}

func TestOverrideFieldMetadataFilter_ErrorOnFieldNotFound(t *testing.T) {
	// Create field metadata
	originalMeta, err := tsquery.NewFieldMeta("existing_field", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Try to override non-existent field
	newUrn := "new_name"
	filter := NewOverrideFieldMetadataFilter("non_existent_field", &newUrn, nil, nil)
	_, err = filter.Filter(result)

	// Verify error
	require.Error(t, err)
	require.Contains(t, err.Error(), "not found")
	require.Contains(t, err.Error(), "non_existent_field")
}

func TestOverrideFieldMetadataFilter_WithMultipleFields(t *testing.T) {
	// Create multiple fields
	field1, err := tsquery.NewFieldMeta("field1", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)
	field2, err := tsquery.NewFieldMeta("field2", tsquery.DataTypeInteger, false)
	require.NoError(t, err)
	field3, err := tsquery.NewFieldMeta("field3", tsquery.DataTypeString, true)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0, int64(2), "three"}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*field1, *field2, *field3}, stream.Just(records...))

	// Override only the middle field
	newUrn := "field2_renamed"
	filter := NewOverrideFieldMetadataFilter("field2", &newUrn, nil, nil)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify correct field was updated
	require.Len(t, filteredResult.FieldsMeta(), 3)
	require.Equal(t, "field1", filteredResult.FieldsMeta()[0].Urn())
	require.Equal(t, "field2_renamed", filteredResult.FieldsMeta()[1].Urn())
	require.Equal(t, "field3", filteredResult.FieldsMeta()[2].Urn())

	// Verify data types are preserved
	require.Equal(t, tsquery.DataTypeDecimal, filteredResult.FieldsMeta()[0].DataType())
	require.Equal(t, tsquery.DataTypeInteger, filteredResult.FieldsMeta()[1].DataType())
	require.Equal(t, tsquery.DataTypeString, filteredResult.FieldsMeta()[2].DataType())

	// Verify required flags are preserved
	require.Equal(t, true, filteredResult.FieldsMeta()[0].Required())
	require.Equal(t, false, filteredResult.FieldsMeta()[1].Required())
	require.Equal(t, true, filteredResult.FieldsMeta()[2].Required())
}

func TestOverrideFieldMetadataFilter_DataTypeAndRequiredUnchanged(t *testing.T) {
	// Test all data types to ensure they remain unchanged
	dataTypes := []tsquery.DataType{
		tsquery.DataTypeDecimal,
		tsquery.DataTypeInteger,
		tsquery.DataTypeString,
		tsquery.DataTypeBoolean,
	}

	for _, dataType := range dataTypes {
		for _, required := range []bool{true, false} {
			requiredStr := "false"
			if required {
				requiredStr = "true"
			}
			t.Run(string(dataType)+"_required_"+requiredStr, func(t *testing.T) {
				originalMeta, err := tsquery.NewFieldMeta("field", dataType, required)
				require.NoError(t, err)

				records := []timeseries.TsRecord[[]any]{
					{Value: []any{getValueForDataType(dataType)}, Timestamp: time.Unix(0, 0)},
				}

				result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

				// Try to override other fields
				newUrn := "new_field"
				newUnit := "new_unit"
				filter := NewOverrideFieldMetadataFilter("field", &newUrn, &newUnit, nil)
				filteredResult, err := filter.Filter(result)
				require.NoError(t, err)

				// Verify dataType and required are unchanged
				require.Equal(t, dataType, filteredResult.FieldsMeta()[0].DataType())
				require.Equal(t, required, filteredResult.FieldsMeta()[0].Required())

				// Verify other fields were updated
				require.Equal(t, "new_field", filteredResult.FieldsMeta()[0].Urn())
				require.Equal(t, "new_unit", filteredResult.FieldsMeta()[0].Unit())
			})
		}
	}
}

func TestOverrideFieldMetadataFilter_EmptyCustomMeta(t *testing.T) {
	// Create field with no custom metadata
	originalMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Add custom metadata
	newCustomMeta := map[string]any{"key": "value"}
	filter := NewOverrideFieldMetadataFilter("field", nil, nil, newCustomMeta)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify custom metadata was added
	require.Equal(t, "value", filteredResult.FieldsMeta()[0].GetCustomMetaStringValue("key"))
}

func TestOverrideFieldMetadataFilter_PreservesStreamData(t *testing.T) {
	// Create field metadata
	originalMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, true)
	require.NoError(t, err)

	// Create multiple records with different timestamps
	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.5}, Timestamp: time.Unix(100, 0)},
		{Value: []any{2.5}, Timestamp: time.Unix(200, 0)},
		{Value: []any{3.5}, Timestamp: time.Unix(300, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override metadata
	newUrn := "renamed"
	filter := NewOverrideFieldMetadataFilter("field", &newUrn, nil, nil)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify stream data is completely unchanged
	collectedRecords := filteredResult.Stream().MustCollect()
	require.Len(t, collectedRecords, 3)
	require.Equal(t, []any{1.5}, collectedRecords[0].Value)
	require.Equal(t, []any{2.5}, collectedRecords[1].Value)
	require.Equal(t, []any{3.5}, collectedRecords[2].Value)
	require.Equal(t, time.Unix(100, 0), collectedRecords[0].Timestamp)
	require.Equal(t, time.Unix(200, 0), collectedRecords[1].Timestamp)
	require.Equal(t, time.Unix(300, 0), collectedRecords[2].Timestamp)
}

func TestOverrideFieldMetadataFilter_OverrideUnitToEmpty(t *testing.T) {
	// Create field with unit
	originalMeta, err := tsquery.NewFieldMetaWithCustomData(
		"field",
		tsquery.DataTypeDecimal,
		true,
		"celsius",
		nil,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{25.0}, Timestamp: time.Unix(0, 0)},
	}

	result := tsquery.NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override unit to empty string
	emptyUnit := ""
	filter := NewOverrideFieldMetadataFilter("field", nil, &emptyUnit, nil)
	filteredResult, err := filter.Filter(result)
	require.NoError(t, err)

	// Verify unit is now empty
	require.Equal(t, "", filteredResult.FieldsMeta()[0].Unit())
}

// Helper function to get a valid value for each data type
func getValueForDataType(dataType tsquery.DataType) any {
	switch dataType {
	case tsquery.DataTypeDecimal:
		return 1.5
	case tsquery.DataTypeInteger:
		return int64(42)
	case tsquery.DataTypeString:
		return "test"
	case tsquery.DataTypeBoolean:
		return true
	default:
		return nil
	}
}
