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
func createConstantField(t *testing.T, urn string, dataType tsquery.DataType, value any) field.Field {
	meta, err := tsquery.NewFieldMeta(urn, dataType, true)
	require.NoError(t, err)
	return field.NewConstantField(*meta, value)
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
	tempField := field.NewRefField("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionField(
		"temp_above_threshold",
		field.ConditionOperatorGreaterThan,
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
	requestsField := field.NewRefField("CounterMetrics:Requests")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeInteger, int64(300))
	conditionField := field.NewConditionField(
		"high_traffic",
		field.ConditionOperatorGreaterThan,
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
	tempField := field.NewRefField("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionField(
		"temp_below_threshold",
		field.ConditionOperatorLessThan,
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
	requestsField := field.NewRefField("CounterMetrics:Requests")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeInteger, int64(200))
	conditionField := field.NewConditionField(
		"high_or_equal_traffic",
		field.ConditionOperatorGreaterEqual,
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
	tempField := field.NewRefField("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionField(
		"temp_at_or_below",
		field.ConditionOperatorLessEqual,
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
	requestsField := field.NewRefField("CounterMetrics:Requests")
	target := createConstantField(t, "target", tsquery.DataTypeInteger, int64(100))
	conditionField := field.NewConditionField(
		"exactly_100",
		field.ConditionOperatorEquals,
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
	tempField := field.NewRefField("SensorReading:Temperature")
	target := createConstantField(t, "target", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionField(
		"not_20",
		field.ConditionOperatorNotEquals,
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
	requestsField := field.NewRefField("CounterMetrics:Requests")
	errorsField := field.NewRefField("CounterMetrics:Errors")
	conditionField := field.NewConditionField(
		"healthy_ratio",
		field.ConditionOperatorGreaterThan,
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
	tempField := field.NewRefField("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 100.0)
	conditionField := field.NewConditionField(
		"extreme_temp",
		field.ConditionOperatorGreaterThan,
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
	tempField := field.NewRefField("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionField(
		"above_20",
		field.ConditionOperatorGreaterThan,
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
	nonBooleanField := field.NewRefField("SensorReading:Temperature")
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
	tempField := field.NewRefField("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionField(
		"above_20",
		field.ConditionOperatorGreaterThan,
		tempField,
		threshold,
	)
	conditionFilter := filter.NewConditionFilter(conditionField)

	filteredResult, err := conditionFilter.Filter(result)
	require.NoError(t, err)

	// Second filter: Keep only temperature field
	singleFieldFilter := filter.NewSingleFieldFilter(field.NewRefField("SensorReading:Temperature"))
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
	tempField := field.NewRefField("SensorReading:Temperature")
	threshold := createConstantField(t, "threshold", tsquery.DataTypeDecimal, 20.0)
	conditionField := field.NewConditionField(
		"above_20",
		field.ConditionOperatorGreaterThan,
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
	statusField := field.NewRefField("ApplicationStatus:Status")
	target := createConstantField(t, "target", tsquery.DataTypeString, "running")
	conditionField := field.NewConditionField(
		"is_running",
		field.ConditionOperatorEquals,
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
	statusField := field.NewRefField("ApplicationStatus:Status")
	target := createConstantField(t, "target", tsquery.DataTypeString, "running")
	conditionField := field.NewConditionField(
		"not_running",
		field.ConditionOperatorNotEquals,
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
	activeField := field.NewRefField("ApplicationStatus:Active")
	target := createConstantField(t, "target", tsquery.DataTypeBoolean, true)
	conditionField := field.NewConditionField(
		"is_active",
		field.ConditionOperatorEquals,
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
	activeField := field.NewRefField("ApplicationStatus:Active")
	target := createConstantField(t, "target", tsquery.DataTypeBoolean, true)
	conditionField := field.NewConditionField(
		"not_active",
		field.ConditionOperatorNotEquals,
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
	statusField := field.NewRefField("ApplicationStatus:Status")
	target := createConstantField(t, "target", tsquery.DataTypeString, "running")
	conditionField := field.NewConditionField(
		"invalid",
		field.ConditionOperatorGreaterThan,
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
	activeField := field.NewRefField("ApplicationStatus:Active")
	target := createConstantField(t, "target", tsquery.DataTypeBoolean, true)
	conditionField := field.NewConditionField(
		"invalid",
		field.ConditionOperatorLessThan,
		activeField,
		target,
	)

	conditionFilter := filter.NewConditionFilter(conditionField)
	_, err = conditionFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "is not supported for non-numeric type")
	require.Contains(t, err.Error(), "Only equals and not_equals are supported")
}
