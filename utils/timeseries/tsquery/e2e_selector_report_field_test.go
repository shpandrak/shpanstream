package tsquery_test

import (
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/report"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Test structs for selector field tests

type ServerMetrics struct {
	Timestamp       time.Time
	IsProduction    bool
	ProdLatency     float64
	DevLatency      float64
	ProdRequestRate int64
	DevRequestRate  int64
}

type PricingData struct {
	Timestamp         time.Time
	IsPremiumCustomer bool
	PremiumPrice      float64
	StandardPrice     float64
	PremiumDiscount   float64
	StandardDiscount  float64
}

type SystemMode struct {
	Timestamp         time.Time
	IsHighPerformance bool
	HighPerfMode      string
	LowPerfMode       string
}

// --- Basic Selector Tests with Decimals ---

func TestSelectorField_DecimalValues_SelectTrue(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
		{Timestamp: baseTime.Add(1 * time.Hour), IsProduction: true, ProdLatency: 12.3, DevLatency: 30.0, ProdRequestRate: 1200, DevRequestRate: 60},
		{Timestamp: baseTime.Add(2 * time.Hour), IsProduction: true, ProdLatency: 11.8, DevLatency: 28.5, ProdRequestRate: 1100, DevRequestRate: 55},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:IsProduction", "ServerMetrics:ProdLatency", "ServerMetrics:DevLatency"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"", "ms", "ms"},
	)
	require.NoError(t, err)

	// Create selector field: if IsProduction then ProdLatency else DevLatency
	selectorField := report.NewRefFieldValue("ServerMetrics:IsProduction")
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency")
	falseField := report.NewRefFieldValue("ServerMetrics:DevLatency")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "selected_latency"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	selectedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata
	fieldsMeta := selectedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	require.Equal(t, "selected_latency", fieldsMeta[0].Urn())
	require.Equal(t, tsquery.DataTypeDecimal, fieldsMeta[0].DataType())
	require.True(t, fieldsMeta[0].Required())
	require.Equal(t, "ms", fieldsMeta[0].Unit())

	// Verify values - all should be production latency since IsProduction=true
	records := selectedResult.Stream().MustCollect()
	require.Len(t, records, 3)
	require.Equal(t, 10.5, records[0].Value[0])
	require.Equal(t, 12.3, records[1].Value[0])
	require.Equal(t, 11.8, records[2].Value[0])
}

func TestSelectorField_DecimalValues_SelectFalse(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: false, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
		{Timestamp: baseTime.Add(1 * time.Hour), IsProduction: false, ProdLatency: 12.3, DevLatency: 30.0, ProdRequestRate: 1200, DevRequestRate: 60},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:IsProduction", "ServerMetrics:ProdLatency", "ServerMetrics:DevLatency"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"", "ms", "ms"},
	)
	require.NoError(t, err)

	// Create selector field
	selectorField := report.NewRefFieldValue("ServerMetrics:IsProduction")
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency")
	falseField := report.NewRefFieldValue("ServerMetrics:DevLatency")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "selected_latency"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	selectedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify values - all should be dev latency since IsProduction=false
	records := selectedResult.Stream().MustCollect()
	require.Len(t, records, 2)
	require.Equal(t, 25.0, records[0].Value[0])
	require.Equal(t, 30.0, records[1].Value[0])
}

func TestSelectorField_DecimalValues_MixedSelection(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
		{Timestamp: baseTime.Add(1 * time.Hour), IsProduction: false, ProdLatency: 12.3, DevLatency: 30.0, ProdRequestRate: 1200, DevRequestRate: 60},
		{Timestamp: baseTime.Add(2 * time.Hour), IsProduction: true, ProdLatency: 11.8, DevLatency: 28.5, ProdRequestRate: 1100, DevRequestRate: 55},
		{Timestamp: baseTime.Add(3 * time.Hour), IsProduction: false, ProdLatency: 13.0, DevLatency: 32.0, ProdRequestRate: 1300, DevRequestRate: 65},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:IsProduction", "ServerMetrics:ProdLatency", "ServerMetrics:DevLatency"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"", "ms", "ms"},
	)
	require.NoError(t, err)

	// Create selector field
	selectorField := report.NewRefFieldValue("ServerMetrics:IsProduction")
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency")
	falseField := report.NewRefFieldValue("ServerMetrics:DevLatency")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "selected_latency"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	selectedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify values alternate between prod and dev
	records := selectedResult.Stream().MustCollect()
	require.Len(t, records, 4)
	require.Equal(t, 10.5, records[0].Value[0]) // prod (true)
	require.Equal(t, 30.0, records[1].Value[0]) // dev (false)
	require.Equal(t, 11.8, records[2].Value[0]) // prod (true)
	require.Equal(t, 32.0, records[3].Value[0]) // dev (false)

	// Verify timestamps preserved
	require.Equal(t, baseTime, records[0].Timestamp)
	require.Equal(t, baseTime.Add(1*time.Hour), records[1].Timestamp)
	require.Equal(t, baseTime.Add(2*time.Hour), records[2].Timestamp)
	require.Equal(t, baseTime.Add(3*time.Hour), records[3].Timestamp)
}

// --- Integer Selector Tests ---

type RequestRateMetrics struct {
	Timestamp    time.Time
	IsProduction bool
	ProdRate     int64
	DevRate      int64
}

func TestSelectorField_IntegerValues(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []RequestRateMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdRate: 1000, DevRate: 50},
		{Timestamp: baseTime.Add(1 * time.Hour), IsProduction: false, ProdRate: 1200, DevRate: 60},
		{Timestamp: baseTime.Add(2 * time.Hour), IsProduction: true, ProdRate: 1100, DevRate: 55},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"RequestRateMetrics:IsProduction", "RequestRateMetrics:ProdRate", "RequestRateMetrics:DevRate"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeInteger, tsquery.DataTypeInteger},
		[]string{"", "requests/sec", "requests/sec"},
	)
	require.NoError(t, err)

	// Create selector field: if IsProduction then ProdRate else DevRate
	selectorField := report.NewRefFieldValue("RequestRateMetrics:IsProduction")
	trueField := report.NewRefFieldValue("RequestRateMetrics:ProdRate")
	falseField := report.NewRefFieldValue("RequestRateMetrics:DevRate")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "selected_request_rate"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	selectedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata
	fieldsMeta := selectedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	require.Equal(t, "selected_request_rate", fieldsMeta[0].Urn())
	require.Equal(t, tsquery.DataTypeInteger, fieldsMeta[0].DataType())
	require.True(t, fieldsMeta[0].Required())
	require.Equal(t, "requests/sec", fieldsMeta[0].Unit())

	// Verify values
	records := selectedResult.Stream().MustCollect()
	require.Len(t, records, 3)
	require.Equal(t, int64(1000), records[0].Value[0]) // prod (true)
	require.Equal(t, int64(60), records[1].Value[0])   // dev (false)
	require.Equal(t, int64(1100), records[2].Value[0]) // prod (true)
}

// --- String Selector Tests ---

func TestSelectorField_StringValues(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []SystemMode{
		{Timestamp: baseTime, IsHighPerformance: true, HighPerfMode: "turbo", LowPerfMode: "eco"},
		{Timestamp: baseTime.Add(1 * time.Hour), IsHighPerformance: false, HighPerfMode: "turbo", LowPerfMode: "eco"},
		{Timestamp: baseTime.Add(2 * time.Hour), IsHighPerformance: true, HighPerfMode: "turbo", LowPerfMode: "eco"},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"SystemMode:IsHighPerformance", "SystemMode:HighPerfMode", "SystemMode:LowPerfMode"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeString, tsquery.DataTypeString},
		nil,
	)
	require.NoError(t, err)

	// Create a selector field: if IsHighPerformance then HighPerfMode else LowPerfMode
	selectorField := report.NewRefFieldValue("SystemMode:IsHighPerformance")
	trueField := report.NewRefFieldValue("SystemMode:HighPerfMode")
	falseField := report.NewRefFieldValue("SystemMode:LowPerfMode")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "active_mode"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	selectedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify metadata
	fieldsMeta := selectedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	require.Equal(t, "active_mode", fieldsMeta[0].Urn())
	require.Equal(t, tsquery.DataTypeString, fieldsMeta[0].DataType())
	require.True(t, fieldsMeta[0].Required())

	// Verify values
	records := selectedResult.Stream().MustCollect()
	require.Len(t, records, 3)
	require.Equal(t, "turbo", records[0].Value[0]) // high perf (true)
	require.Equal(t, "eco", records[1].Value[0])   // low perf (false)
	require.Equal(t, "turbo", records[2].Value[0]) // high perf (true)
}

// --- Selector with Condition Field ---

func TestSelectorField_WithConditionAsSelector(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []PricingData{
		{Timestamp: baseTime, IsPremiumCustomer: true, PremiumPrice: 100.0, StandardPrice: 150.0, PremiumDiscount: 20.0, StandardDiscount: 5.0},
		{Timestamp: baseTime.Add(1 * time.Hour), IsPremiumCustomer: false, PremiumPrice: 100.0, StandardPrice: 150.0, PremiumDiscount: 20.0, StandardDiscount: 5.0},
		{Timestamp: baseTime.Add(2 * time.Hour), IsPremiumCustomer: true, PremiumPrice: 100.0, StandardPrice: 150.0, PremiumDiscount: 20.0, StandardDiscount: 5.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{
			"PricingData:IsPremiumCustomer",
			"PricingData:PremiumPrice",
			"PricingData:StandardPrice",
			"PricingData:PremiumDiscount",
			"PricingData:StandardDiscount",
		},
		[]tsquery.DataType{
			tsquery.DataTypeBoolean,
			tsquery.DataTypeDecimal,
			tsquery.DataTypeDecimal,
			tsquery.DataTypeDecimal,
			tsquery.DataTypeDecimal,
		},
		[]string{"", "usd", "usd", "usd", "usd"},
	)
	require.NoError(t, err)

	// Create a condition field as the selector
	premiumField := report.NewRefFieldValue("PricingData:IsPremiumCustomer")
	trueConstant := createConstantField(tsquery.DataTypeBoolean, true)
	conditionSelector := report.NewConditionFieldValue(tsquery.ConditionOperatorEquals,
		premiumField,
		trueConstant,
	)

	// Create a selector field: if is_premium then PremiumPrice else StandardPrice
	trueField := report.NewRefFieldValue("PricingData:PremiumPrice")
	falseField := report.NewRefFieldValue("PricingData:StandardPrice")

	priceSelector := report.NewSelectorFieldValue(conditionSelector, trueField, falseField) // URN: final_price

	// Create another selector for discount
	discountTrueField := report.NewRefFieldValue("PricingData:PremiumDiscount")
	discountFalseField := report.NewRefFieldValue("PricingData:StandardDiscount")
	discountSelector := report.NewSelectorFieldValue(conditionSelector, discountTrueField, discountFalseField) // URN: applied_discount

	// Apply both selectors
	priceFilter := report.NewSingleFieldFilter(priceSelector, tsquery.AddFieldMeta{Urn: "final_price"})
	priceResult, err := priceFilter.Filter(result)
	require.NoError(t, err)

	// Need to recreate a result for the second filter
	result2, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{
			"PricingData:IsPremiumCustomer",
			"PricingData:PremiumPrice",
			"PricingData:StandardPrice",
			"PricingData:PremiumDiscount",
			"PricingData:StandardDiscount",
		},
		[]tsquery.DataType{
			tsquery.DataTypeBoolean,
			tsquery.DataTypeDecimal,
			tsquery.DataTypeDecimal,
			tsquery.DataTypeDecimal,
			tsquery.DataTypeDecimal,
		},
		[]string{"", "usd", "usd", "usd", "usd"},
	)
	require.NoError(t, err)

	discountFilter := report.NewSingleFieldFilter(discountSelector, tsquery.AddFieldMeta{Urn: "applied_discount"})
	discountResult, err := discountFilter.Filter(result2)
	require.NoError(t, err)

	// Verify prices
	priceRecords := priceResult.Stream().MustCollect()
	require.Len(t, priceRecords, 3)
	require.Equal(t, 100.0, priceRecords[0].Value[0]) // premium price
	require.Equal(t, 150.0, priceRecords[1].Value[0]) // standard price
	require.Equal(t, 100.0, priceRecords[2].Value[0]) // premium price

	// Verify discounts
	discountRecords := discountResult.Stream().MustCollect()
	require.Len(t, discountRecords, 3)
	require.Equal(t, 20.0, discountRecords[0].Value[0]) // premium discount
	require.Equal(t, 5.0, discountRecords[1].Value[0])  // standard discount
	require.Equal(t, 20.0, discountRecords[2].Value[0]) // premium discount
}

// --- Error Cases ---

func TestSelectorField_ErrorOnMismatchedDataTypes(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:IsProduction", "ServerMetrics:ProdLatency", "ServerMetrics:ProdRequestRate"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeInteger},
		nil,
	)
	require.NoError(t, err)

	// Try to create selector with mismatched types (decimal vs integer)
	selectorField := report.NewRefFieldValue("ServerMetrics:IsProduction")
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency")      // decimal
	falseField := report.NewRefFieldValue("ServerMetrics:ProdRequestRate") // integer

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "invalid"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible datatypes")
}

func TestSelectorField_ErrorOnMismatchedUnits(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []PricingData{
		{Timestamp: baseTime, IsPremiumCustomer: true, PremiumPrice: 100.0, StandardPrice: 150.0, PremiumDiscount: 20.0, StandardDiscount: 5.0},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"PricingData:IsPremiumCustomer", "PricingData:PremiumPrice", "PricingData:PremiumDiscount"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"", "usd", "percent"}, // different units
	)
	require.NoError(t, err)

	// Try to create selector with mismatched units
	selectorField := report.NewRefFieldValue("PricingData:IsPremiumCustomer")
	trueField := report.NewRefFieldValue("PricingData:PremiumPrice")     // usd
	falseField := report.NewRefFieldValue("PricingData:PremiumDiscount") // percent

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "invalid"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible units")
}

func TestSelectorField_ErrorOnNonBooleanSelector(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:ProdLatency", "ServerMetrics:DevLatency"},
		[]tsquery.DataType{tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Try to use a non-boolean field as selector (decimal instead of boolean)
	selectorField := report.NewRefFieldValue("ServerMetrics:ProdLatency") // decimal, not boolean
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency")
	falseField := report.NewRefFieldValue("ServerMetrics:DevLatency")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "invalid"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "selector field")
	require.Contains(t, err.Error(), "must be boolean")
}

func TestSelectorField_ErrorOnOptionalSelector(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:IsProduction", "ServerMetrics:ProdLatency", "ServerMetrics:DevLatency"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Manually make the boolean field optional
	fieldsMeta := result.FieldsMeta()
	optionalBoolMeta, err := tsquery.NewFieldMetaWithCustomData(
		fieldsMeta[0].Urn(),
		fieldsMeta[0].DataType(),
		false, // make it optional
		"",
		nil,
	)
	require.NoError(t, err)

	newMeta := []tsquery.FieldMeta{*optionalBoolMeta, fieldsMeta[1], fieldsMeta[2]}
	result = report.NewResult(newMeta, result.Stream())

	// Try to use optional boolean as selector
	selectorField := report.NewRefFieldValue("ServerMetrics:IsProduction")
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency")
	falseField := report.NewRefFieldValue("ServerMetrics:DevLatency")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "invalid"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "selector field")
	require.Contains(t, err.Error(), "must be required")
}

func TestSelectorField_ErrorOnMismatchedRequiredStatus(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
	}

	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:IsProduction", "ServerMetrics:ProdLatency", "ServerMetrics:DevLatency"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		nil,
	)
	require.NoError(t, err)

	// Make DevLatency optional
	fieldsMeta := result.FieldsMeta()
	optionalDevMeta, err := tsquery.NewFieldMetaWithCustomData(
		fieldsMeta[2].Urn(),
		fieldsMeta[2].DataType(),
		false, // make it optional
		"",
		nil,
	)
	require.NoError(t, err)

	newMeta := []tsquery.FieldMeta{fieldsMeta[0], fieldsMeta[1], *optionalDevMeta}
	result = report.NewResult(newMeta, result.Stream())

	// Try to create selector with mismatched required status
	selectorField := report.NewRefFieldValue("ServerMetrics:IsProduction")
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency") // required
	falseField := report.NewRefFieldValue("ServerMetrics:DevLatency") // optional

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "invalid"
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{Urn: selectorUrn})

	_, err = singleFieldFilter.Filter(result)
	require.Error(t, err)
	require.Contains(t, err.Error(), "incompatible required status")
}

// --- Custom Meta Tests ---

func TestSelectorField_MergesCustomMeta(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	testData := []ServerMetrics{
		{Timestamp: baseTime, IsProduction: true, ProdLatency: 10.5, DevLatency: 25.0, ProdRequestRate: 1000, DevRequestRate: 50},
	}

	// First create result using helper
	result, err := createResultFromStructs(
		stream.Just(testData...),
		[]string{"ServerMetrics:IsProduction", "ServerMetrics:ProdLatency", "ServerMetrics:DevLatency"},
		[]tsquery.DataType{tsquery.DataTypeBoolean, tsquery.DataTypeDecimal, tsquery.DataTypeDecimal},
		[]string{"", "ms", "ms"},
	)
	require.NoError(t, err)

	// Now recreate with custom metadata
	boolMeta, _ := tsquery.NewFieldMetaWithCustomData("ServerMetrics:IsProduction", tsquery.DataTypeBoolean, true, "", nil)
	prodMeta, _ := tsquery.NewFieldMetaWithCustomData("ServerMetrics:ProdLatency", tsquery.DataTypeDecimal, true, "ms", map[string]any{
		"environment": "production",
		"source":      "metrics-api",
	})
	devMeta, _ := tsquery.NewFieldMetaWithCustomData("ServerMetrics:DevLatency", tsquery.DataTypeDecimal, true, "ms", map[string]any{
		"environment": "development",
		"backup":      "enabled",
	})

	result = report.NewResult(
		[]tsquery.FieldMeta{*boolMeta, *prodMeta, *devMeta},
		result.Stream(),
	)

	// Create selector field
	selectorField := report.NewRefFieldValue("ServerMetrics:IsProduction")
	trueField := report.NewRefFieldValue("ServerMetrics:ProdLatency")
	falseField := report.NewRefFieldValue("ServerMetrics:DevLatency")

	selector := report.NewSelectorFieldValue(selectorField, trueField, falseField)
	selectorUrn := "selected_latency"

	// With the new architecture, custom metadata is provided through AddFieldMeta at the filter level,
	// not automatically merged from field values
	customMeta := map[string]any{
		"environment": "development",
		"source":      "metrics-api",
		"backup":      "enabled",
	}
	singleFieldFilter := report.NewSingleFieldFilter(selector, tsquery.AddFieldMeta{
		Urn:        selectorUrn,
		CustomMeta: customMeta,
	})

	selectedResult, err := singleFieldFilter.Filter(result)
	require.NoError(t, err)

	// Verify custom meta is set through AddFieldMeta
	fieldsMeta := selectedResult.FieldsMeta()
	require.Len(t, fieldsMeta, 1)
	resultCustomMeta := fieldsMeta[0].CustomMeta()
	require.NotNil(t, resultCustomMeta)
	require.Equal(t, "development", resultCustomMeta["environment"])
	require.Equal(t, "metrics-api", resultCustomMeta["source"])
	require.Equal(t, "enabled", resultCustomMeta["backup"])
}
