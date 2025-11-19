package tsquery_test

import (
	"context"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Helper function to create a result from a struct stream with required fields
func createResultFromStructs[T any](structStream stream.Stream[T], fieldNames []string, fieldTypes []tsquery.DataType, fieldUnits []string) (report.Result, error) {
	// First, create with datasource to get the structure
	ds, err := report.NewStaticStructDatasource(structStream)
	if err != nil {
		return report.Result{}, err
	}

	ctx := context.Background()
	result, err := ds.Execute(ctx, time.Time{}, time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC))
	if err != nil {
		return report.Result{}, err
	}

	// Rebuild field metadata with required=true and proper units
	var newFieldsMeta []tsquery.FieldMeta
	for i, name := range fieldNames {
		unit := ""
		if fieldUnits != nil && i < len(fieldUnits) {
			unit = fieldUnits[i]
		}
		meta, err := tsquery.NewFieldMetaWithCustomData(name, fieldTypes[i], true, unit, nil)
		if err != nil {
			return report.Result{}, err
		}
		newFieldsMeta = append(newFieldsMeta, *meta)
	}

	return report.NewResult(newFieldsMeta, result.Stream()), nil
}

// Test structs for reduce field tests

type DecimalMetrics struct {
	Timestamp   time.Time
	Temperature float64
	Humidity    float64
	Pressure    float64
}

type IntegerMetrics struct {
	Timestamp time.Time
	Count1    int64
	Count2    int64
	Count3    int64
}

type MixedTypeMetrics struct {
	Timestamp   time.Time
	Temperature float64
	Count       int64
}

type OptionalFieldMetrics struct {
	Timestamp time.Time
	Required1 float64
	Optional1 float64
	Required2 float64
}

// --- Sum Tests ---

func TestReduceField_SumAllFields_Decimal(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []DecimalMetrics{
		{Timestamp: baseTime, Temperature: 20.0, Humidity: 60.0, Pressure: 1013.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 22.0, Humidity: 65.0, Pressure: 1015.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 18.0, Humidity: 70.0, Pressure: 1012.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"DecimalMetrics:Temperature", "DecimalMetrics:Humidity", "DecimalMetrics:Pressure"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"celsius", "percent", "hPa"},
	)
	require.NoError(t, err)

	// Apply reduce field: sum all fields
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "total"})

	reducedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata
	fieldsMeta := reducedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	require.Equal(t, "total", fieldsMeta[0].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, fieldsMeta[0].DataType())
	require.True(t, fieldsMeta[0].Required())
	require.Equal(t, "", fieldsMeta[0].Unit()) // Different units, so no unit preserved

	// Verify values
	records := reducedResult.Stream().MustCollect()
	require.Len(t, records, 3)

	// First record: 20.0 + 60.0 + 1013.0 = 1093.0
	require.Equal(t, 1093.0, records[0].Value[0])

	// Second record: 22.0 + 65.0 + 1015.0 = 1102.0
	require.Equal(t, 1102.0, records[1].Value[0])

	// Third record: 18.0 + 70.0 + 1012.0 = 1100.0
	require.Equal(t, 1100.0, records[2].Value[0])
}

func TestReduceField_SumSpecificFields_Integer(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []IntegerMetrics{
		{Timestamp: baseTime, Count1: 10, Count2: 20, Count3: 30},
		{Timestamp: baseTime.Add(1 * time.Hour), Count1: 15, Count2: 25, Count3: 35},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2", "IntegerMetrics:Count3"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		[]string{"items", "items", "items"},
	)
	require.NoError(t, err)

	// Apply reduce field: sum only Count1 and Count2
	reduceField := report.NewReduceFieldValues([]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2"}, tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "sum_counts"})

	reducedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata
	fieldsMeta := reducedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	require.Equal(t, "sum_counts", fieldsMeta[0].Urn())
	require.Equal(t, tsquery.DataTypeInteger, fieldsMeta[0].DataType())
	require.True(t, fieldsMeta[0].Required())
	require.Equal(t, "items", fieldsMeta[0].Unit()) // Same unit preserved

	// Verify values
	records := reducedResult.Stream().MustCollect()
	require.Len(t, records, 2)

	// First record: 10 + 20 = 30
	require.Equal(t, int64(30), records[0].Value[0])

	// Second record: 15 + 25 = 40
	require.Equal(t, int64(40), records[1].Value[0])
}

// --- Average Tests ---

func TestReduceField_AvgAllFields_Decimal(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []DecimalMetrics{
		{Timestamp: baseTime, Temperature: 20.0, Humidity: 60.0, Pressure: 90.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"DecimalMetrics:Temperature", "DecimalMetrics:Humidity", "DecimalMetrics:Pressure"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Apply reduce field: average all fields
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeAvg)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "average"})

	reducedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata
	fieldsMeta := reducedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	require.Equal(t, "average", fieldsMeta[0].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, fieldsMeta[0].DataType()) // Average always returns decimal
	require.True(t, fieldsMeta[0].Required())

	// Verify values
	records := reducedResult.Stream().MustCollect()
	require.Len(t, records, 1)

	// (20.0 + 60.0 + 90.0) / 3 = 56.666...
	require.InDelta(t, 56.666666666, records[0].Value[0].(float64), 0.00001)
}

func TestReduceField_MinMax_Integer(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []IntegerMetrics{
		{Timestamp: baseTime, Count1: 100, Count2: 50, Count3: 75},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2", "IntegerMetrics:Count3"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		[]string{"items", "items", "items"},
	)
	require.NoError(t, err)

	// Test MIN
	minField := report.NewReduceFieldValues([]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2"}, tsquery.ReductionTypeMin)
	minFilter := report.NewSingleFieldFilter(minField, tsquery.AddFieldMeta{Urn: "result"})
	minResult, err := minFilter.Filter(result)
	require.NoError(t, err)

	minRecords := minResult.Stream().MustCollect()
	require.Equal(t, int64(50), minRecords[0].Value[0]) // min(100, 50) = 50

	// Test MAX
	maxField := report.NewReduceFieldValues([]string{"IntegerMetrics:Count2", "IntegerMetrics:Count3"}, tsquery.ReductionTypeMax)
	maxFilter := report.NewSingleFieldFilter(maxField, tsquery.AddFieldMeta{Urn: "result"})

	// Need to recreate result since stream was consumed
	result2, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2", "IntegerMetrics:Count3"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		[]string{"items", "items", "items"},
	)
	require.NoError(t, err)

	maxResult, err := maxFilter.Filter(result2)
	require.NoError(t, err)

	maxRecords := maxResult.Stream().MustCollect()
	require.Equal(t, int64(75), maxRecords[0].Value[0]) // max(50, 75) = 75
}

// --- Count Test ---

func TestReduceField_Count(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []DecimalMetrics{
		{Timestamp: baseTime, Temperature: 20.0, Humidity: 60.0, Pressure: 1013.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"DecimalMetrics:Temperature", "DecimalMetrics:Humidity", "DecimalMetrics:Pressure"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Apply reduce field: count all fields
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeCount)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "field_count"})

	reducedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata
	fieldsMeta := reducedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	require.Equal(t, "field_count", fieldsMeta[0].Urn())
	require.Equal(t, tsquery.DataTypeInteger, fieldsMeta[0].DataType()) // Count always returns integer
	require.True(t, fieldsMeta[0].Required())

	// Verify values
	records := reducedResult.Stream().MustCollect()
	require.Len(t, records, 1)

	// Count of 3 fields
	require.Equal(t, int64(3), records[0].Value[0])
}

// --- Error Cases ---

func TestReduceField_ErrorOnMixedDataTypes(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []MixedTypeMetrics{
		{Timestamp: baseTime, Temperature: 20.0, Count: 10},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"MixedTypeMetrics:Temperature", "MixedTypeMetrics:Count"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Try to reduce fields with different data types (decimal and integer)
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "result"})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "all fields must have the same data type")
}

func TestReduceField_ErrorOnNonExistentField(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []IntegerMetrics{
		{Timestamp: baseTime, Count1: 10, Count2: 20, Count3: 30},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2", "IntegerMetrics:Count3"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Try to reduce a field that doesn't exist
	reduceField := report.NewReduceFieldValues([]string{"IntegerMetrics:NonExistent"}, tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "result"})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "some fields not found")
}

func TestReduceField_ErrorOnOptionalField(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []OptionalFieldMetrics{
		{Timestamp: baseTime, Required1: 10.0, Optional1: 20.0, Required2: 30.0},
	}

	// Create result with one optional field
	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"OptionalFieldMetrics:Required1", "OptionalFieldMetrics:Optional1", "OptionalFieldMetrics:Required2"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Manually set one field to optional
	fieldsMeta := result.FieldsMeta()
	optionalMeta, err := tsquery.NewFieldMetaWithCustomData(
		fieldsMeta[1].Urn(),
		fieldsMeta[1].DataType(),
		false, // optional
		"",
		nil,
	)
	require.NoError(t, err)

	newMeta := []tsquery.FieldMeta{fieldsMeta[0], *optionalMeta, fieldsMeta[2]}
	result = report.NewResult(newMeta, result.Stream())

	// Try to reduce all fields (includes optional field)
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "result"})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be required")
}

// --- Unit Preservation Tests ---

func TestReduceField_PreservesUnitWhenAllSame(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []IntegerMetrics{
		{Timestamp: baseTime, Count1: 10, Count2: 20, Count3: 30},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2", "IntegerMetrics:Count3"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		[]string{"items", "items", "items"},
	)
	require.NoError(t, err)

	// All IntegerMetrics fields have unit "items"
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "result"})

	reducedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify unit is preserved
	fieldsMeta := reducedResult.FieldsMeta()
	require.Equal(t, "items", fieldsMeta[0].Unit())
}

func TestReduceField_NoUnitWhenDifferent(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []DecimalMetrics{
		{Timestamp: baseTime, Temperature: 20.0, Humidity: 60.0, Pressure: 1013.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"DecimalMetrics:Temperature", "DecimalMetrics:Humidity", "DecimalMetrics:Pressure"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"celsius", "percent", "hPa"},
	)
	require.NoError(t, err)

	// DecimalMetrics fields have different units (celsius, percent, hPa)
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "result"})

	reducedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify no unit when they differ
	fieldsMeta := reducedResult.FieldsMeta()
	require.Equal(t, "", fieldsMeta[0].Unit())
}

// --- Multiple Records Test ---

func TestReduceField_MultipleRecordsProcessedCorrectly(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []IntegerMetrics{
		{Timestamp: baseTime, Count1: 1, Count2: 2, Count3: 3},
		{Timestamp: baseTime.Add(1 * time.Hour), Count1: 4, Count2: 5, Count3: 6},
		{Timestamp: baseTime.Add(2 * time.Hour), Count1: 7, Count2: 8, Count3: 9},
		{Timestamp: baseTime.Add(3 * time.Hour), Count1: 10, Count2: 11, Count3: 12},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"IntegerMetrics:Count1", "IntegerMetrics:Count2", "IntegerMetrics:Count3"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Apply reduce field: sum all fields
	reduceField := report.NewReduceAllFieldValues(tsquery.ReductionTypeSum)
	singleFieldFilter := report.NewSingleFieldFilter(reduceField, tsquery.AddFieldMeta{Urn: "result"})

	reducedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify all records processed
	records := reducedResult.Stream().MustCollect()
	require.Len(t, records, 4)

	// Verify each record's sum
	require.Equal(t, int64(6), records[0].Value[0])  // 1 + 2 + 3
	require.Equal(t, int64(15), records[1].Value[0]) // 4 + 5 + 6
	require.Equal(t, int64(24), records[2].Value[0]) // 7 + 8 + 9
	require.Equal(t, int64(33), records[3].Value[0]) // 10 + 11 + 12

	// Verify timestamps preserved
	require.Equal(t, baseTime, records[0].Timestamp)
	require.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	require.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
	require.Equal(t, baseTime.Add(3*time.Hour), records[3].Timestamp)
}
