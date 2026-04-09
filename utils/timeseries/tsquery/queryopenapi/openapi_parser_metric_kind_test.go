package queryopenapi

import (
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseStaticDatasource_MetricKind verifies that metricKind on ApiQueryFieldMeta
// flows through parseStaticDatasource into the resulting FieldMeta.
func TestParseStaticDatasource_MetricKind(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		kind     tsquery.MetricKind
		expected tsquery.MetricKind
	}{
		{"gauge", tsquery.MetricKindGauge, tsquery.MetricKindGauge},
		{"delta", tsquery.MetricKindDelta, tsquery.MetricKindDelta},
		{"cumulative", tsquery.MetricKindCumulative, tsquery.MetricKindCumulative},
		{"rate", tsquery.MetricKindRate, tsquery.MetricKindRate},
		{"unspecified defaults to gauge", "", tsquery.MetricKindGauge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			apiDs := ApiStaticQueryDatasource{
				Type: "static",
				FieldMeta: ApiQueryFieldMeta{
					Uri:        "metric",
					DataType:   tsquery.DataTypeDecimal,
					Required:   true,
					MetricKind: tt.kind,
				},
				Data: []ApiMeasurementValue{
					{Timestamp: baseTime, Value: 1.0},
				},
			}

			ds, err := parseStaticDatasource(apiDs)
			require.NoError(t, err)

			result, err := ds.Execute(testContext(), baseTime, baseTime.Add(1*time.Hour))
			require.NoError(t, err)

			assert.Equal(t, tt.expected, result.Meta().MetricKind())
		})
	}
}

// TestParseStaticDatasource_InvalidMetricKind verifies that an invalid metricKind
// value is rejected at parse time (validated by NewFieldMetaFull).
func TestParseStaticDatasource_InvalidMetricKind(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	apiDs := ApiStaticQueryDatasource{
		Type: "static",
		FieldMeta: ApiQueryFieldMeta{
			Uri:        "metric",
			DataType:   tsquery.DataTypeDecimal,
			Required:   true,
			MetricKind: tsquery.MetricKind("bogus"),
		},
		Data: []ApiMeasurementValue{
			{Timestamp: baseTime, Value: 1.0},
		},
	}

	_, err := parseStaticDatasource(apiDs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid metric kind")
}

// TestParseStaticReportDatasource_MetricKind verifies that metricKind on ApiQueryFieldMeta
// flows through parseStaticReportDatasource for multi-field report datasources with
// a mix of different kinds.
func TestParseStaticReportDatasource_MetricKind(t *testing.T) {
	baseTime := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

	apiDs := ApiStaticReportDatasource{
		Type: "static",
		FieldsMeta: []ApiQueryFieldMeta{
			{
				Uri:        "energy",
				DataType:   tsquery.DataTypeDecimal,
				Required:   true,
				Unit:       "kWh",
				MetricKind: tsquery.MetricKindCumulative,
			},
			{
				Uri:        "temperature",
				DataType:   tsquery.DataTypeDecimal,
				Required:   true,
				Unit:       "celsius",
				MetricKind: tsquery.MetricKindGauge,
			},
			{
				Uri:        "power",
				DataType:   tsquery.DataTypeDecimal,
				Required:   true,
				Unit:       "kW",
				MetricKind: tsquery.MetricKindRate,
			},
			{
				Uri:        "hourly_consumption",
				DataType:   tsquery.DataTypeDecimal,
				Required:   true,
				Unit:       "kWh",
				MetricKind: tsquery.MetricKindDelta,
			},
		},
		Data: []ApiReportMeasurementRow{
			{Timestamp: baseTime, Values: []any{100.0, 20.5, 2.5, 5.0}},
		},
	}

	ds, err := parseStaticReportDatasource(apiDs)
	require.NoError(t, err)

	result, err := ds.Execute(testContext(), baseTime, baseTime.Add(1*time.Hour))
	require.NoError(t, err)

	fields := result.FieldsMeta()
	require.Len(t, fields, 4)
	assert.Equal(t, tsquery.MetricKindCumulative, fields[0].MetricKind())
	assert.Equal(t, tsquery.MetricKindGauge, fields[1].MetricKind())
	assert.Equal(t, tsquery.MetricKindRate, fields[2].MetricKind())
	assert.Equal(t, tsquery.MetricKindDelta, fields[3].MetricKind())
}

// TestParseAddFieldMeta_OverrideMetricKind verifies that overrideMetricKind
// flows through ParseAddFieldMeta into tsquery.AddFieldMeta.
func TestParseAddFieldMeta_OverrideMetricKind(t *testing.T) {
	apiMeta := ApiAddFieldMeta{
		Uri:                "computed",
		OverrideMetricKind: tsquery.MetricKindRate,
	}

	parsed := ParseAddFieldMeta(apiMeta)
	assert.Equal(t, "computed", parsed.Urn)
	assert.Equal(t, tsquery.MetricKindRate, parsed.OverrideMetricKind)
}

// TestParseAddFieldMeta_NoOverrideMetricKind verifies that an unset overrideMetricKind
// results in an empty MetricKind (backward compat — nothing was set before Phase 1).
func TestParseAddFieldMeta_NoOverrideMetricKind(t *testing.T) {
	apiMeta := ApiAddFieldMeta{
		Uri: "computed",
	}

	parsed := ParseAddFieldMeta(apiMeta)
	assert.Equal(t, "computed", parsed.Urn)
	assert.Equal(t, tsquery.MetricKind(""), parsed.OverrideMetricKind)
}

// TestParseOverrideFieldMetadataFilter_UpdatedMetricKind verifies that
// updatedMetricKind flows through parseOverrideFieldMetadataFilter into the filter.
func TestParseOverrideFieldMetadataFilter_UpdatedMetricKind(t *testing.T) {
	apiFilter := ApiOverrideFieldMetadataFilter{
		Type:              "overrideFieldMetadata",
		UpdatedMetricKind: tsquery.MetricKindCumulative,
	}

	filter, err := parseOverrideFieldMetadataFilter(apiFilter)
	require.NoError(t, err)
	require.NotNil(t, filter)
	// The parser creates an OverrideFieldMetadataFilter with optUpdatedMetricKind set.
	// End-to-end verification that the filter actually applies the override is covered
	// in datasource/metric_kind_propagation_test.go (TestOverrideFieldMetadataFilter_OverrideKind).
}
