package tsquery_test

import (
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/filter"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Helper function to create a constant field with given datatype and value
func createConstantField(t *testing.T, urn string, dataType tsquery.DataType, value any) field.Value {
	valueMeta := field.ValueMeta{DataType: dataType, Required: true}
	return field.NewConstantFieldValue(valueMeta, value)
}

// Test structs for condition filter tests

type SensorReading struct {
	Timestamp   time.Time
	Temperature float64
	Humidity    float64
}

type CounterMetrics struct {
	Timestamp time.Time
	Requests  int64
	Errors    int64
}

type ApplicationStatus struct {
	Timestamp time.Time
	Status    string
	Active    bool
}

// --- Greater Than Tests ---

func TestConditionFilter_GreaterThan_Decimal(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 15.0, Humidity: 60.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 25.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 18.0, Humidity: 70.0},
		{Timestamp: baseTime.Add(3 * time.Hour), Temperature: 30.0, Humidity: 55.0},
		{Timestamp: baseTime.Add(4 * time.Hour), Temperature: 12.0, Humidity: 75.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"celsius", "percent"},
	)
	require.NoError(t, err)

	// Create condition: Temperature > 20.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		threshold,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Only records with temp > 20

	// Should have records at index 1 (25.0) and 3 (30.0)
	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 65.0, records[0].Value[1])
	require.Equal(t, baseTime.Add(1*time.Hour), records[0].Timestamp)

	require.Equal(t, 30.0, records[1].Value[0])
	require.Equal(t, 55.0, records[1].Value[1])
	require.Equal(t, baseTime.Add(3*time.Hour), records[1].Timestamp)
}

func TestConditionFilter_GreaterThan_Integer(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []CounterMetrics{
		{Timestamp: baseTime, Requests: 100, Errors: 5},
		{Timestamp: baseTime.Add(1 * time.Hour), Requests: 500, Errors: 10},
		{Timestamp: baseTime.Add(2 * time.Hour), Requests: 250, Errors: 3},
		{Timestamp: baseTime.Add(3 * time.Hour), Requests: 1000, Errors: 15},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"CounterMetrics:Requests", "CounterMetrics:Errors"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		[]string{"count", "count"},
	)
	require.NoError(t, err)

	// Create condition: Requests > 300
	requestsField := field.NewRefFieldValue("CounterMetrics:Requests")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeInteger, int64(300))
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		requestsField,
		threshold,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with requests > 300

	require.Equal(t, int64(500), records[0].Value[0])
	require.Equal(t, int64(10), records[0].Value[1])

	require.Equal(t, int64(1000), records[1].Value[0])
	require.Equal(t, int64(15), records[1].Value[1])
}

// --- Less Than Tests ---

func TestConditionFilter_LessThan_Decimal(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 15.0, Humidity: 60.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 25.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 10.0, Humidity: 70.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature < 20.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorLessThan,
		tempField,
		threshold,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with temp < 20

	require.Equal(t, 15.0, records[0].Value[0])
	require.Equal(t, 10.0, records[1].Value[0])
}

// --- Greater Equal Tests ---

func TestConditionFilter_GreaterEqual_Integer(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []CounterMetrics{
		{Timestamp: baseTime, Requests: 100, Errors: 5},
		{Timestamp: baseTime.Add(1 * time.Hour), Requests: 200, Errors: 10},
		{Timestamp: baseTime.Add(2 * time.Hour), Requests: 200, Errors: 8},
		{Timestamp: baseTime.Add(3 * time.Hour), Requests: 150, Errors: 3},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"CounterMetrics:Requests", "CounterMetrics:Errors"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Requests >= 200
	requestsField := field.NewRefFieldValue("CounterMetrics:Requests")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeInteger, int64(200))
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterEqual,
		requestsField,
		threshold,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with requests >= 200

	require.Equal(t, int64(200), records[0].Value[0])
	require.Equal(t, int64(200), records[1].Value[0])
}

// --- Less Equal Tests ---

func TestConditionFilter_LessEqual_Decimal(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 15.0, Humidity: 60.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 20.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 25.0, Humidity: 70.0},
		{Timestamp: baseTime.Add(3 * time.Hour), Temperature: 20.0, Humidity: 55.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature <= 20.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorLessEqual,
		tempField,
		threshold,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 3) // Records with temp <= 20

	require.Equal(t, 15.0, records[0].Value[0])
	require.Equal(t, 20.0, records[1].Value[0])
	require.Equal(t, 20.0, records[2].Value[0])
}

// --- Equals Tests ---

func TestConditionFilter_Equals_Integer(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []CounterMetrics{
		{Timestamp: baseTime, Requests: 100, Errors: 5},
		{Timestamp: baseTime.Add(1 * time.Hour), Requests: 100, Errors: 10},
		{Timestamp: baseTime.Add(2 * time.Hour), Requests: 200, Errors: 5},
		{Timestamp: baseTime.Add(3 * time.Hour), Requests: 100, Errors: 3},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"CounterMetrics:Requests", "CounterMetrics:Errors"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Requests == 100
	requestsField := field.NewRefFieldValue("CounterMetrics:Requests")
	target := createConstantField(t, "target", tsquery.DataTypeInteger, int64(100))
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorEquals,
		requestsField,
		target,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 3) // Records with requests == 100

	require.Equal(t, int64(100), records[0].Value[0])
	require.Equal(t, int64(100), records[1].Value[0])
	require.Equal(t, int64(100), records[2].Value[0])
}

// --- Not Equals Tests ---

func TestConditionFilter_NotEquals_Decimal(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 20.0, Humidity: 60.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 25.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 20.0, Humidity: 70.0},
		{Timestamp: baseTime.Add(3 * time.Hour), Temperature: 18.0, Humidity: 55.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature != 20.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	target := createConstantField(t, "target", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorNotEquals,
		tempField,
		target,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with temp != 20

	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 18.0, records[1].Value[0])
}

// --- Field Comparison Tests ---

func TestConditionFilter_CompareTwoFields(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []CounterMetrics{
		{Timestamp: baseTime, Requests: 100, Errors: 5},
		{Timestamp: baseTime.Add(1 * time.Hour), Requests: 50, Errors: 10},
		{Timestamp: baseTime.Add(2 * time.Hour), Requests: 200, Errors: 8},
		{Timestamp: baseTime.Add(3 * time.Hour), Requests: 30, Errors: 40},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"CounterMetrics:Requests", "CounterMetrics:Errors"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Requests > Errors (filter for healthy state)
	requestsField := field.NewRefFieldValue("CounterMetrics:Requests")
	errorsField := field.NewRefFieldValue("CounterMetrics:Errors")
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		requestsField,
		errorsField,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 3) // Records where requests > errors (100>5, 50>10, 200>8)

	// Record 0: 100 > 5
	require.Equal(t, int64(100), records[0].Value[0])
	require.Equal(t, int64(5), records[0].Value[1])

	// Record 1: 50 > 10
	require.Equal(t, int64(50), records[1].Value[0])
	require.Equal(t, int64(10), records[1].Value[1])

	// Record 2: 200 > 8
	require.Equal(t, int64(200), records[2].Value[0])
	require.Equal(t, int64(8), records[2].Value[1])
}

// --- Empty Result Tests ---

func TestConditionFilter_NoMatchingRows(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 15.0, Humidity: 60.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 18.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 12.0, Humidity: 70.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature > 100.0 (no records match)
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 100.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		threshold,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify empty result
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 0)
}

func TestConditionFilter_AllRowsMatch(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 25.0, Humidity: 60.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 30.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 22.0, Humidity: 70.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature > 20.0 (all records match)
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		threshold,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify all records returned
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 30.0, records[1].Value[0])
	require.Equal(t, 22.0, records[2].Value[0])
}

// --- Error Cases ---

func TestConditionFilter_ErrorOnNonBooleanField(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 20.0, Humidity: 60.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Try to use a non-boolean field (temperature is decimal)
	nonBooleanField := field.NewRefFieldValue("SensorReading:Temperature")
	conditionFilter := filter.NewConditionFilter(nonBooleanField)

	_, err = conditionFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires a boolean field")
}

// --- Complex Filtering Tests ---

func TestConditionFilter_ChainedWithOtherFilters(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 15.0, Humidity: 60.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 25.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 30.0, Humidity: 70.0},
		{Timestamp: baseTime.Add(3 * time.Hour), Temperature: 18.0, Humidity: 55.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// First filter: Temperature > 20.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		threshold,
	)
	conditionFilter := filter.NewConditionFilter(conditionField)

	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Second filter: Keep only temperature field
	singleFieldFilter := filter.NewSingleFieldFilter(field.NewRefFieldValue("SensorReading:Temperature"), filter.AddFieldMeta{Urn: "SensorReading:Temperature"})
	finalResult, err := singleFieldFilter.Filter(filteredResult)
	require.NoError(t, err)

	// Verify final result
	records := finalResult.Stream().MustCollect()
	require.Len(t, records, 2)
	require.Len(t, records[0].Value, 1) // Only one field

	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 30.0, records[1].Value[0])
}

func TestConditionFilter_PreservesFieldMetadata(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 25.0, Humidity: 60.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"celsius", "percent"},
	)
	require.NoError(t, err)

	// Apply condition filter
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		threshold,
	)
	conditionFilter := filter.NewConditionFilter(conditionField)

	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata is preserved
	fieldsMeta := filteredResult.FieldsMeta()
	require.Len(t, fieldsMeta, 2)
	require.Equal(t, "SensorReading:Temperature", fieldsMeta[0].Urn())
	require.Equal(t, "celsius", fieldsMeta[0].Unit())
	require.Equal(t, "SensorReading:Humidity", fieldsMeta[1].Urn())
	require.Equal(t, "percent", fieldsMeta[1].Unit())
}

// --- String Comparison Tests ---

func TestConditionFilter_Equals_String(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ApplicationStatus{
		{Timestamp: baseTime, Status: "running", Active: true},
		{Timestamp: baseTime.Add(1 * time.Hour), Status: "stopped", Active: false},
		{Timestamp: baseTime.Add(2 * time.Hour), Status: "running", Active: true},
		{Timestamp: baseTime.Add(3 * time.Hour), Status: "error", Active: false},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ApplicationStatus:Status", "ApplicationStatus:Active"},
		[]tsquery.DataType{tsquery.DataTypeString, tsquery.DataTypeBoolean},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Status == "running"
	statusField := field.NewRefFieldValue("ApplicationStatus:Status")
	target := createConstantField(t, "target", tsquery.DataTypeString, "running")
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorEquals,
		statusField,
		target,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with status == "running"

	require.Equal(t, "running", records[0].Value[0])
	require.Equal(t, true, records[0].Value[1])

	require.Equal(t, "running", records[1].Value[0])
	require.Equal(t, true, records[1].Value[1])
}

func TestConditionFilter_NotEquals_String(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ApplicationStatus{
		{Timestamp: baseTime, Status: "running", Active: true},
		{Timestamp: baseTime.Add(1 * time.Hour), Status: "stopped", Active: false},
		{Timestamp: baseTime.Add(2 * time.Hour), Status: "running", Active: true},
		{Timestamp: baseTime.Add(3 * time.Hour), Status: "error", Active: false},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ApplicationStatus:Status", "ApplicationStatus:Active"},
		[]tsquery.DataType{tsquery.DataTypeString, tsquery.DataTypeBoolean},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Status != "running"
	statusField := field.NewRefFieldValue("ApplicationStatus:Status")
	target := createConstantField(t, "target", tsquery.DataTypeString, "running")
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorNotEquals,
		statusField,
		target,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with status != "running"

	require.Equal(t, "stopped", records[0].Value[0])
	require.Equal(t, "error", records[1].Value[0])
}

// --- Boolean Comparison Tests ---

func TestConditionFilter_Equals_Boolean(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ApplicationStatus{
		{Timestamp: baseTime, Status: "running", Active: true},
		{Timestamp: baseTime.Add(1 * time.Hour), Status: "stopped", Active: false},
		{Timestamp: baseTime.Add(2 * time.Hour), Status: "running", Active: true},
		{Timestamp: baseTime.Add(3 * time.Hour), Status: "error", Active: false},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ApplicationStatus:Status", "ApplicationStatus:Active"},
		[]tsquery.DataType{tsquery.DataTypeString, tsquery.DataTypeBoolean},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Active == true
	activeField := field.NewRefFieldValue("ApplicationStatus:Active")
	target := createConstantField(t, "target", tsquery.DataTypeBoolean, true)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorEquals,
		activeField,
		target,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with active == true

	require.Equal(t, "running", records[0].Value[0])
	require.Equal(t, true, records[0].Value[1])

	require.Equal(t, "running", records[1].Value[0])
	require.Equal(t, true, records[1].Value[1])
}

func TestConditionFilter_NotEquals_Boolean(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ApplicationStatus{
		{Timestamp: baseTime, Status: "running", Active: true},
		{Timestamp: baseTime.Add(1 * time.Hour), Status: "stopped", Active: false},
		{Timestamp: baseTime.Add(2 * time.Hour), Status: "running", Active: true},
		{Timestamp: baseTime.Add(3 * time.Hour), Status: "error", Active: false},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ApplicationStatus:Status", "ApplicationStatus:Active"},
		[]tsquery.DataType{tsquery.DataTypeString, tsquery.DataTypeBoolean},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Active != true (i.e., Active == false)
	activeField := field.NewRefFieldValue("ApplicationStatus:Active")
	target := createConstantField(t, "target", tsquery.DataTypeBoolean, true)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorNotEquals,
		activeField,
		target,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(conditionField)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2) // Records with active != true

	require.Equal(t, "stopped", records[0].Value[0])
	require.Equal(t, false, records[0].Value[1])

	require.Equal(t, "error", records[1].Value[0])
	require.Equal(t, false, records[1].Value[1])
}

// --- Error Cases for Non-Numeric Types ---

func TestConditionFilter_ErrorOnStringWithGreaterThan(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ApplicationStatus{
		{Timestamp: baseTime, Status: "running", Active: true},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ApplicationStatus:Status", "ApplicationStatus:Active"},
		[]tsquery.DataType{tsquery.DataTypeString, tsquery.DataTypeBoolean},
		nil,
	)
	require.NoError(t, err)

	// Try to use > operator with string type
	statusField := field.NewRefFieldValue("ApplicationStatus:Status")
	target := createConstantField(t, "target", tsquery.DataTypeString, "running")
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		statusField,
		target,
	)

	conditionFilter := filter.NewConditionFilter(conditionField)
	_, err = conditionFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported for non-numeric type")
	require.Contains(t, err.Error(), "Only equals and not_equals are supported")
}

func TestConditionFilter_ErrorOnBooleanWithLessThan(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ApplicationStatus{
		{Timestamp: baseTime, Status: "running", Active: true},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ApplicationStatus:Status", "ApplicationStatus:Active"},
		[]tsquery.DataType{tsquery.DataTypeString, tsquery.DataTypeBoolean},
		nil,
	)
	require.NoError(t, err)

	// Try to use < operator with boolean type
	activeField := field.NewRefFieldValue("ApplicationStatus:Active")
	target := createConstantField(t, "target", tsquery.DataTypeBoolean, true)
	conditionField := field.NewConditionFieldValue(field.ConditionOperatorLessThan,
		activeField,
		target,
	)

	conditionFilter := filter.NewConditionFilter(conditionField)
	_, err = conditionFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported for non-numeric type")
	require.Contains(t, err.Error(), "Only equals and not_equals are supported")
}

// --- Logical Expression Tests (AND/OR) ---

func TestLogicalExpressionFilter_And_BothTrue(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 25.0, Humidity: 65.0},                    // temp>20 AND humidity>60 = true
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 15.0, Humidity: 65.0}, // temp>20=false, humidity>60=true = false
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 30.0, Humidity: 55.0}, // temp>20=true, humidity>60=false = false
		{Timestamp: baseTime.Add(3 * time.Hour), Temperature: 22.0, Humidity: 70.0}, // temp>20 AND humidity>60 = true
		{Timestamp: baseTime.Add(4 * time.Hour), Temperature: 18.0, Humidity: 58.0}, // both false
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature > 20.0 AND Humidity > 60.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	humidityField := field.NewRefFieldValue("SensorReading:Humidity")

	tempThreshold := createConstantField(t, "temp_threshold", tsquery.DataTypeDecimal, 20.0)
	humidityThreshold := createConstantField(t, "humidity_threshold", tsquery.DataTypeDecimal, 60.0)

	tempCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		tempThreshold,
	)

	humidityCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		humidityField,
		humidityThreshold,
	)

	andCondition := field.NewLogicalExpressionFieldValue(field.LogicalOperatorAnd,
		tempCondition,
		humidityCondition,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(andCondition)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records - should only have records where BOTH conditions are true
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 65.0, records[0].Value[1])
	require.Equal(t, baseTime, records[0].Timestamp)

	require.Equal(t, 22.0, records[1].Value[0])
	require.Equal(t, 70.0, records[1].Value[1])
	require.Equal(t, baseTime.Add(3*time.Hour), records[1].Timestamp)
}

func TestLogicalExpressionFilter_Or_AtLeastOneTrue(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 25.0, Humidity: 65.0},                    // temp>20 OR humidity>60 = true
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 15.0, Humidity: 65.0}, // temp>20=false, humidity>60=true = true
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 30.0, Humidity: 55.0}, // temp>20=true, humidity>60=false = true
		{Timestamp: baseTime.Add(3 * time.Hour), Temperature: 18.0, Humidity: 58.0}, // both false = false
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature > 20.0 OR Humidity > 60.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	humidityField := field.NewRefFieldValue("SensorReading:Humidity")

	tempThreshold := createConstantField(t, "temp_threshold", tsquery.DataTypeDecimal, 20.0)
	humidityThreshold := createConstantField(t, "humidity_threshold", tsquery.DataTypeDecimal, 60.0)

	tempCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		tempThreshold,
	)

	humidityCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		humidityField,
		humidityThreshold,
	)

	orCondition := field.NewLogicalExpressionFieldValue(field.LogicalOperatorOr,
		tempCondition,
		humidityCondition,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(orCondition)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records - should have records where AT LEAST ONE condition is true
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 65.0, records[0].Value[1])

	require.Equal(t, 15.0, records[1].Value[0])
	require.Equal(t, 65.0, records[1].Value[1])

	require.Equal(t, 30.0, records[2].Value[0])
	require.Equal(t, 55.0, records[2].Value[1])
}

func TestLogicalExpressionFilter_And_WithIntegerFields(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []CounterMetrics{
		{Timestamp: baseTime, Requests: 1000, Errors: 5},                    // requests>500 AND errors<10 = true
		{Timestamp: baseTime.Add(1 * time.Hour), Requests: 300, Errors: 3},  // requests>500=false = false
		{Timestamp: baseTime.Add(2 * time.Hour), Requests: 800, Errors: 15}, // errors<10=false = false
		{Timestamp: baseTime.Add(3 * time.Hour), Requests: 600, Errors: 8},  // both true = true
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"CounterMetrics:Requests", "CounterMetrics:Errors"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Requests > 500 AND Errors < 10
	requestsField := field.NewRefFieldValue("CounterMetrics:Requests")
	errorsField := field.NewRefFieldValue("CounterMetrics:Errors")

	requestsThreshold := createConstantField(t, "requests_threshold", tsquery.DataTypeInteger, int64(500))
	errorsThreshold := createConstantField(t, "errors_threshold", tsquery.DataTypeInteger, int64(10))

	requestsCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		requestsField,
		requestsThreshold,
	)

	errorsCondition := field.NewConditionFieldValue(field.ConditionOperatorLessThan,
		errorsField,
		errorsThreshold,
	)

	andCondition := field.NewLogicalExpressionFieldValue(field.LogicalOperatorAnd,
		requestsCondition,
		errorsCondition,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(andCondition)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, int64(1000), records[0].Value[0])
	require.Equal(t, int64(5), records[0].Value[1])

	require.Equal(t, int64(600), records[1].Value[0])
	require.Equal(t, int64(8), records[1].Value[1])
}

func TestLogicalExpressionFilter_NestedLogicalExpressions(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []CounterMetrics{
		{Timestamp: baseTime, Requests: 1000, Errors: 5},                    // (requests>500 AND errors<10) OR errors==0 = true OR false = true
		{Timestamp: baseTime.Add(1 * time.Hour), Requests: 300, Errors: 0},  // (false AND true) OR true = false OR true = true
		{Timestamp: baseTime.Add(2 * time.Hour), Requests: 800, Errors: 15}, // (true AND false) OR false = false OR false = false
		{Timestamp: baseTime.Add(3 * time.Hour), Requests: 200, Errors: 20}, // (false AND false) OR false = false OR false = false
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"CounterMetrics:Requests", "CounterMetrics:Errors"},
		[]tsquery.DataType{tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Create condition: (Requests > 500 AND Errors < 10) OR Errors == 0
	requestsField := field.NewRefFieldValue("CounterMetrics:Requests")
	errorsField := field.NewRefFieldValue("CounterMetrics:Errors")

	requestsThreshold := createConstantField(t, "requests_threshold", tsquery.DataTypeInteger, int64(500))
	errorsThreshold := createConstantField(t, "errors_threshold", tsquery.DataTypeInteger, int64(10))
	zeroErrors := createConstantField(t, "zero", tsquery.DataTypeInteger, int64(0))

	requestsCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		requestsField,
		requestsThreshold,
	)

	lowErrorsCondition := field.NewConditionFieldValue(field.ConditionOperatorLessThan,
		errorsField,
		errorsThreshold,
	)

	zeroErrorsCondition := field.NewConditionFieldValue(field.ConditionOperatorEquals,
		errorsField,
		zeroErrors,
	)

	// First AND condition
	andCondition := field.NewLogicalExpressionFieldValue(field.LogicalOperatorAnd,
		requestsCondition,
		lowErrorsCondition,
	)

	// Then OR with zero errors
	orCondition := field.NewLogicalExpressionFieldValue(field.LogicalOperatorOr,
		andCondition,
		zeroErrorsCondition,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(orCondition)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify filtered records
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 2)

	require.Equal(t, int64(1000), records[0].Value[0])
	require.Equal(t, int64(5), records[0].Value[1])

	require.Equal(t, int64(300), records[1].Value[0])
	require.Equal(t, int64(0), records[1].Value[1])
}

func TestLogicalExpressionFilter_Or_AllFalse(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 15.0, Humidity: 55.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 18.0, Humidity: 58.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature > 20.0 OR Humidity > 60.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	humidityField := field.NewRefFieldValue("SensorReading:Humidity")

	tempThreshold := createConstantField(t, "temp_threshold", tsquery.DataTypeDecimal, 20.0)
	humidityThreshold := createConstantField(t, "humidity_threshold", tsquery.DataTypeDecimal, 60.0)

	tempCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		tempField,
		tempThreshold,
	)

	humidityCondition := field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
		humidityField,
		humidityThreshold,
	)

	orCondition := field.NewLogicalExpressionFieldValue(field.LogicalOperatorOr,
		tempCondition,
		humidityCondition,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(orCondition)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify empty result - no records match
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 0)
}

func TestLogicalExpressionFilter_And_AllTrue(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SensorReading{
		{Timestamp: baseTime, Temperature: 25.0, Humidity: 65.0},
		{Timestamp: baseTime.Add(1 * time.Hour), Temperature: 30.0, Humidity: 70.0},
		{Timestamp: baseTime.Add(2 * time.Hour), Temperature: 22.0, Humidity: 68.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SensorReading:Temperature", "SensorReading:Humidity"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Temperature > 20.0 AND Humidity > 60.0
	tempField := field.NewRefFieldValue("SensorReading:Temperature")
	humidityField := field.NewRefFieldValue("SensorReading:Humidity")

	// Apply condition filter
	filteredResult, err := filter.NewConditionFilter(field.NewLogicalExpressionFieldValue(field.LogicalOperatorAnd,
		field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
			tempField,
			createConstantField(t, "temp_threshold", tsquery.DataTypeDecimal, 20.0),
		),
		field.NewConditionFieldValue(field.ConditionOperatorGreaterThan,
			humidityField,
			createConstantField(t, "humidity_threshold", tsquery.DataTypeDecimal, 60.0),
		),
	)).Filter(result)
	require.NoError(t, err)

	// Verify all records match
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 3)

	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 30.0, records[1].Value[0])
	require.Equal(t, 22.0, records[2].Value[0])
}

func TestLogicalExpressionFilter_WithStringAndBooleanComparisons(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ApplicationStatus{
		{Timestamp: baseTime, Status: "running", Active: true},                     // status=="running" AND active==true = true
		{Timestamp: baseTime.Add(1 * time.Hour), Status: "stopped", Active: false}, // false AND false = false
		{Timestamp: baseTime.Add(2 * time.Hour), Status: "running", Active: false}, // true AND false = false
		{Timestamp: baseTime.Add(3 * time.Hour), Status: "error", Active: true},    // false AND true = false
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ApplicationStatus:Status", "ApplicationStatus:Active"},
		[]tsquery.DataType{tsquery.DataTypeString, tsquery.DataTypeBoolean},
		nil,
	)
	require.NoError(t, err)

	// Create condition: Status == "running" AND Active == true
	statusField := field.NewRefFieldValue("ApplicationStatus:Status")
	activeField := field.NewRefFieldValue("ApplicationStatus:Active")

	runningValue := createConstantField(t, "running", tsquery.DataTypeString, "running")
	trueValue := createConstantField(t, "true", tsquery.DataTypeBoolean, true)

	statusCondition := field.NewConditionFieldValue(field.ConditionOperatorEquals,
		statusField,
		runningValue,
	)

	activeCondition := field.NewConditionFieldValue(field.ConditionOperatorEquals,
		activeField,
		trueValue,
	)

	andCondition := field.NewLogicalExpressionFieldValue(field.LogicalOperatorAnd,
		statusCondition,
		activeCondition,
	)

	// Apply condition filter
	conditionFilter := filter.NewConditionFilter(andCondition)
	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Verify only first record matches
	records := filteredResult.Stream().MustCollect()
	require.Len(t, records, 1)

	require.Equal(t, "running", records[0].Value[0])
	require.Equal(t, true, records[0].Value[1])
}
