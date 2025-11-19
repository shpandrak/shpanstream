package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestOverrideFieldMetadataFilter_OverrideUrn(t *testing.T) {
	// Create a static datasource with original metadata
	originalUrn := "original_metric"
	newUrn := "renamed_metric"

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(originalUrn, tsquery.DataTypeInteger, true, "count", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(100)},
		{Timestamp: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), Value: int64(200)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter to override URN
	filter := NewOverrideFieldMetadataFilter(&newUrn, nil, nil)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify URN was overridden
	require.Equal(t, newUrn, result.Meta().Urn())

	// Verify other metadata unchanged
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	require.Equal(t, true, result.Meta().Required())
	require.Equal(t, "count", result.Meta().Unit())

	// Verify data stream unchanged
	resultRecords := result.Data().MustCollect()
	require.Equal(t, records, resultRecords)
}

func TestOverrideFieldMetadataFilter_OverrideUnit(t *testing.T) {
	// Create datasource with original unit
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("temperature", tsquery.DataTypeDecimal, true, "celsius", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: 25.5},
		{Timestamp: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), Value: 26.0},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter to override unit
	newUnit := "fahrenheit"
	filter := NewOverrideFieldMetadataFilter(nil, &newUnit, nil)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify unit was overridden
	require.Equal(t, "fahrenheit", result.Meta().Unit())

	// Verify other metadata unchanged
	require.Equal(t, "temperature", result.Meta().Urn())
	require.Equal(t, tsquery.DataTypeDecimal, result.Meta().DataType())
	require.Equal(t, true, result.Meta().Required())

	// Verify data stream unchanged
	resultRecords := result.Data().MustCollect()
	require.Equal(t, records, resultRecords)
}

func TestOverrideFieldMetadataFilter_OverrideCustomMeta(t *testing.T) {
	// Create datasource with original custom metadata
	originalCustomMeta := map[string]any{
		"source":  "sensor_1",
		"version": "v1",
	}

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		"metric",
		tsquery.DataTypeInteger,
		true,
		"units",
		originalCustomMeta,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(10)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter to override custom metadata
	newCustomMeta := map[string]any{
		"source":      "sensor_2",
		"version":     "v2",
		"environment": "production",
	}
	filter := NewOverrideFieldMetadataFilter(nil, nil, newCustomMeta)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify custom metadata was replaced entirely (not merged)
	require.Equal(t, newCustomMeta, result.Meta().CustomMeta())

	// Verify other metadata unchanged
	require.Equal(t, "metric", result.Meta().Urn())
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	require.Equal(t, true, result.Meta().Required())
	require.Equal(t, "units", result.Meta().Unit())

	// Verify data stream unchanged
	resultRecords := result.Data().MustCollect()
	require.Equal(t, records, resultRecords)
}

func TestOverrideFieldMetadataFilter_OverrideMultipleFields(t *testing.T) {
	// Create datasource with original metadata
	originalCustomMeta := map[string]any{"key": "original"}

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		"old_urn",
		tsquery.DataTypeInteger,
		true,
		"old_unit",
		originalCustomMeta,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(42)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter to override all metadata at once
	newUrn := "new_urn"
	newUnit := "new_unit"
	newCustomMeta := map[string]any{"key": "new"}

	filter := NewOverrideFieldMetadataFilter(&newUrn, &newUnit, newCustomMeta)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify all overrides applied
	require.Equal(t, "new_urn", result.Meta().Urn())
	require.Equal(t, "new_unit", result.Meta().Unit())
	require.Equal(t, newCustomMeta, result.Meta().CustomMeta())

	// Verify immutable fields unchanged
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	require.Equal(t, true, result.Meta().Required())

	// Verify data stream unchanged
	resultRecords := result.Data().MustCollect()
	require.Equal(t, records, resultRecords)
}

func TestOverrideFieldMetadataFilter_NoOverrides(t *testing.T) {
	// Create datasource
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("metric", tsquery.DataTypeInteger, true, "count", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(100)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter with no overrides (all nil)
	filter := NewOverrideFieldMetadataFilter(nil, nil, nil)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify all metadata unchanged
	require.Equal(t, "metric", result.Meta().Urn())
	require.Equal(t, "count", result.Meta().Unit())
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	require.Equal(t, true, result.Meta().Required())

	// Verify data stream unchanged
	resultRecords := result.Data().MustCollect()
	require.Equal(t, records, resultRecords)
}

func TestOverrideFieldMetadataFilter_DataTypeImmutable(t *testing.T) {
	// Create datasource with integer type
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("metric", tsquery.DataTypeInteger, true, "count", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(100)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter (DataType cannot be changed via this filter)
	newUrn := "renamed"
	filter := NewOverrideFieldMetadataFilter(&newUrn, nil, nil)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify DataType remains immutable
	require.Equal(t, tsquery.DataTypeInteger, result.Meta().DataType())
	require.Equal(t, "renamed", result.Meta().Urn())
}

func TestOverrideFieldMetadataFilter_RequiredImmutable(t *testing.T) {
	// Create datasource with required=true
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("metric", tsquery.DataTypeInteger, true, "count", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(100)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter (Required cannot be changed via this filter)
	newUrn := "renamed"
	filter := NewOverrideFieldMetadataFilter(&newUrn, nil, nil)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify Required remains immutable
	require.Equal(t, true, result.Meta().Required())
	require.Equal(t, "renamed", result.Meta().Urn())
}

func TestOverrideFieldMetadataFilter_EmptyUrn(t *testing.T) {
	// Create datasource
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("original", tsquery.DataTypeInteger, true, "count", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(100)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Try to override with empty URN (should fail)
	emptyUrn := ""
	filter := NewOverrideFieldMetadataFilter(&emptyUrn, nil, nil)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute and expect error
	ctx := context.Background()
	_, err = filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create updated field metadata")
}

func TestOverrideFieldMetadataFilter_CustomMetaReplacement(t *testing.T) {
	// Create datasource with rich custom metadata
	originalCustomMeta := map[string]any{
		"source":      "sensor_1",
		"version":     "v1",
		"location":    "building_a",
		"description": "original description",
	}

	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		"metric",
		tsquery.DataTypeInteger,
		true,
		"units",
		originalCustomMeta,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(10)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter with minimal custom metadata (should REPLACE, not merge)
	newCustomMeta := map[string]any{
		"version": "v2",
	}
	filter := NewOverrideFieldMetadataFilter(nil, nil, newCustomMeta)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify custom metadata was replaced entirely (old keys are gone)
	require.Equal(t, newCustomMeta, result.Meta().CustomMeta())
	require.Equal(t, 1, len(result.Meta().CustomMeta()))
	require.Equal(t, "v2", result.Meta().CustomMeta()["version"])

	// Verify old keys are not present
	_, hasSource := result.Meta().CustomMeta()["source"]
	require.False(t, hasSource)
	_, hasLocation := result.Meta().CustomMeta()["location"]
	require.False(t, hasLocation)
}

func TestOverrideFieldMetadataFilter_PreservesStreamData(t *testing.T) {
	// Create datasource with multiple records
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData("metric", tsquery.DataTypeInteger, true, "count", nil)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), Value: int64(100)},
		{Timestamp: time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC), Value: int64(200)},
		{Timestamp: time.Date(2024, 1, 1, 2, 0, 0, 0, time.UTC), Value: int64(300)},
	}

	ds, err := NewStaticDatasource(*fieldMeta, stream.FromSlice(records))
	require.NoError(t, err)

	// Apply filter with all overrides
	newUrn := "renamed"
	newUnit := "items"
	newCustomMeta := map[string]any{"version": "v2"}
	filter := NewOverrideFieldMetadataFilter(&newUrn, &newUnit, newCustomMeta)
	filteredDS := NewFilteredDataSource(ds, filter)

	// Execute
	ctx := context.Background()
	result, err := filteredDS.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	require.NoError(t, err)

	// Verify metadata was overridden
	require.Equal(t, "renamed", result.Meta().Urn())
	require.Equal(t, "items", result.Meta().Unit())
	require.Equal(t, newCustomMeta, result.Meta().CustomMeta())

	// Verify data stream is completely unchanged
	resultRecords := result.Data().MustCollect()
	require.Equal(t, records, resultRecords)
}
