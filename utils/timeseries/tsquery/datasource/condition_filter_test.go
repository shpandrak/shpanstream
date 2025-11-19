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

func TestConditionFilter_FilterTrue(t *testing.T) {
	ctx := context.Background()

	// Create test data
	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: int64(10), Timestamp: time.Unix(0, 0)},
		{Value: int64(20), Timestamp: time.Unix(1, 0)},
		{Value: int64(30), Timestamp: time.Unix(2, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	// Create a condition filter that always returns true
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	trueField := NewConstantFieldValue(boolMeta, true)

	conditionFilter := NewConditionFilter(trueField)

	// Apply filter
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// All records should pass through
	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 3)
}

func TestConditionFilter_FilterFalse(t *testing.T) {
	ctx := context.Background()

	// Create test data
	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: int64(10), Timestamp: time.Unix(0, 0)},
		{Value: int64(20), Timestamp: time.Unix(1, 0)},
		{Value: int64(30), Timestamp: time.Unix(2, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	// Create a condition filter that always returns false
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	falseField := NewConstantFieldValue(boolMeta, false)

	conditionFilter := NewConditionFilter(falseField)

	// Apply filter
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// No records should pass through
	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 0)
}

func TestConditionFilter_NonBooleanField(t *testing.T) {
	ctx := context.Background()

	// Create test data
	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: int64(10), Timestamp: time.Unix(0, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	// Create a condition filter with a non-boolean field
	intMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeInteger, Required: true}
	intField := NewConstantFieldValue(intMeta, int64(42))

	conditionFilter := NewConditionFilter(intField)

	// Apply filter - should fail
	_, err = conditionFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "boolean field")
}

func TestConditionFilter_OptionalBooleanField(t *testing.T) {
	ctx := context.Background()

	// Create test data
	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: int64(10), Timestamp: time.Unix(0, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	// Create a condition filter with an optional boolean field
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: false}
	optionalBoolField := NewConstantFieldValue(boolMeta, true)

	conditionFilter := NewConditionFilter(optionalBoolField)

	// Apply filter - should fail because field is optional
	_, err = conditionFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "required")
	require.Contains(t, err.Error(), "non-optional")
}

func TestConditionFilter_MetadataPreserved(t *testing.T) {
	ctx := context.Background()

	// Create test data with specific metadata
	fieldMeta, err := tsquery.NewFieldMetaWithCustomData(
		"test_field",
		tsquery.DataTypeDecimal,
		false,
		"meters",
		map[string]any{"custom": "value"},
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: 10.5, Timestamp: time.Unix(0, 0)},
		{Value: 20.5, Timestamp: time.Unix(1, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	// Apply a condition filter
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	trueField := NewConstantFieldValue(boolMeta, true)

	conditionFilter := NewConditionFilter(trueField)

	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata is preserved
	resultMeta := filteredResult.Meta()
	require.Equal(t, "test_field", resultMeta.Urn())
	require.Equal(t, tsquery.DataTypeDecimal, resultMeta.DataType())
	require.Equal(t, "meters", resultMeta.Unit())
	require.Equal(t, "value", resultMeta.CustomMeta()["custom"])
}

// mockConditionalField is a mock field that evaluates a condition based on the record value
type mockConditionalField struct {
	threshold int64
}

func (m mockConditionalField) Execute(fieldsMeta tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	valueMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}

	valueSupplier := func(ctx context.Context, record timeseries.TsRecord[any]) (any, error) {
		// Check if the value in the record is greater than threshold
		if intVal, ok := record.Value.(int64); ok {
			return intVal > m.threshold, nil
		}
		return false, nil
	}

	return valueMeta, valueSupplier, nil
}

func TestConditionFilter_WithDynamicCondition(t *testing.T) {
	ctx := context.Background()

	// Create test data
	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{
		{Value: int64(10), Timestamp: time.Unix(0, 0)},
		{Value: int64(20), Timestamp: time.Unix(1, 0)},
		{Value: int64(30), Timestamp: time.Unix(2, 0)},
		{Value: int64(40), Timestamp: time.Unix(3, 0)},
		{Value: int64(50), Timestamp: time.Unix(4, 0)},
	}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	// Create a condition filter that filters values > 25
	conditionalField := mockConditionalField{threshold: 25}

	conditionFilter := NewConditionFilter(conditionalField)

	// Apply filter
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Only records with value > 25 should pass (30, 40, 50)
	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 3)
	require.Equal(t, int64(30), resultData[0].Value)
	require.Equal(t, int64(40), resultData[1].Value)
	require.Equal(t, int64(50), resultData[2].Value)
}

func TestConditionFilter_EmptyStream(t *testing.T) {
	ctx := context.Background()

	// Create test data with empty stream
	fieldMeta, err := tsquery.NewFieldMeta("value", tsquery.DataTypeInteger, false)
	require.NoError(t, err)

	records := []timeseries.TsRecord[any]{}

	inputStream := stream.Just(records...)
	staticDS, err := NewStaticDatasource(*fieldMeta, inputStream)
	require.NoError(t, err)

	result, err := staticDS.Execute(ctx, time.Unix(0, 0), time.Unix(10, 0))
	require.NoError(t, err)

	// Apply condition filter
	boolMeta := tsquery.ValueMeta{DataType: tsquery.DataTypeBoolean, Required: true}
	trueField := NewConstantFieldValue(boolMeta, true)

	conditionFilter := NewConditionFilter(trueField)

	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Should still be empty
	resultData, err := filteredResult.Data().Collect(ctx)
	require.NoError(t, err)
	require.Len(t, resultData, 0)
}
