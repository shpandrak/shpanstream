package report

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeFieldWithKind is a test helper that builds a FieldMeta with an explicit MetricKind.
func makeFieldWithKind(t *testing.T, urn string, kind tsquery.MetricKind) tsquery.FieldMeta {
	t.Helper()
	fm, err := tsquery.NewFieldMetaFull(urn, tsquery.DataTypeDecimal, kind, true, "", nil)
	require.NoError(t, err)
	return *fm
}

// TestRefFieldValue_ExtractsMetricKind verifies that RefFieldValue.Execute
// copies MetricKind from the referenced field's FieldMeta into the returned ValueMeta.
func TestRefFieldValue_ExtractsMetricKind(t *testing.T) {
	ctx := context.Background()

	fieldsMeta := []tsquery.FieldMeta{
		makeFieldWithKind(t, "energy", tsquery.MetricKindCumulative),
		makeFieldWithKind(t, "temperature", tsquery.MetricKindGauge),
		makeFieldWithKind(t, "power", tsquery.MetricKindRate),
		makeFieldWithKind(t, "consumption", tsquery.MetricKindDelta),
	}

	tests := []struct {
		urn      string
		expected tsquery.MetricKind
	}{
		{"energy", tsquery.MetricKindCumulative},
		{"temperature", tsquery.MetricKindGauge},
		{"power", tsquery.MetricKindRate},
		{"consumption", tsquery.MetricKindDelta},
	}

	for _, tt := range tests {
		t.Run(tt.urn, func(t *testing.T) {
			ref := NewRefFieldValue(tt.urn)
			valueMeta, _, err := ref.Execute(ctx, fieldsMeta)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, valueMeta.MetricKind)
		})
	}
}

// TestPrepareField_PropagatesKindFromSource verifies that PrepareField carries
// MetricKind through from the underlying value's ValueMeta into the built FieldMeta.
func TestPrepareField_PropagatesKindFromSource(t *testing.T) {
	ctx := context.Background()

	fieldsMeta := []tsquery.FieldMeta{
		makeFieldWithKind(t, "energy", tsquery.MetricKindCumulative),
	}

	// Reference an existing cumulative field; result should keep Cumulative
	addMeta := tsquery.AddFieldMeta{Urn: "energy_ref"}
	fm, _, err := PrepareField(ctx, addMeta, NewRefFieldValue("energy"), fieldsMeta)
	require.NoError(t, err)
	require.NotNil(t, fm)
	assert.Equal(t, "energy_ref", fm.Urn())
	assert.Equal(t, tsquery.MetricKindCumulative, fm.MetricKind())
}

// TestPrepareField_OverrideMetricKindWinsOverSource verifies that
// AddFieldMeta.OverrideMetricKind takes precedence over the source value's kind.
func TestPrepareField_OverrideMetricKindWinsOverSource(t *testing.T) {
	ctx := context.Background()

	fieldsMeta := []tsquery.FieldMeta{
		makeFieldWithKind(t, "energy", tsquery.MetricKindCumulative),
	}

	// Reference cumulative, but override to Delta
	addMeta := tsquery.AddFieldMeta{
		Urn:                "energy_as_delta",
		OverrideMetricKind: tsquery.MetricKindDelta,
	}
	fm, _, err := PrepareField(ctx, addMeta, NewRefFieldValue("energy"), fieldsMeta)
	require.NoError(t, err)
	require.NotNil(t, fm)
	assert.Equal(t, tsquery.MetricKindDelta, fm.MetricKind())
}

// TestPrepareField_NoOverridePreservesKind verifies that when OverrideMetricKind
// is not set, the source kind is preserved unchanged.
func TestPrepareField_NoOverridePreservesKind(t *testing.T) {
	ctx := context.Background()

	fieldsMeta := []tsquery.FieldMeta{
		makeFieldWithKind(t, "temp", tsquery.MetricKindGauge),
	}

	// No OverrideMetricKind set — should preserve Gauge from source
	addMeta := tsquery.AddFieldMeta{Urn: "temp_ref"}
	fm, _, err := PrepareField(ctx, addMeta, NewRefFieldValue("temp"), fieldsMeta)
	require.NoError(t, err)
	assert.Equal(t, tsquery.MetricKindGauge, fm.MetricKind())
}

// TestOverrideFieldMetadataFilter_OverridesKind verifies that the report-side
// OverrideFieldMetadataFilter applies a MetricKind override.
func TestReportOverrideFieldMetadataFilter_OverridesKind(t *testing.T) {
	ctx := context.Background()

	originalMeta, err := tsquery.NewFieldMetaFull(
		"metric",
		tsquery.DataTypeDecimal,
		tsquery.MetricKindGauge,
		true,
		"",
		nil,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0}, Timestamp: time.Unix(0, 0)},
	}
	result := NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override kind to Cumulative
	cumKind := tsquery.MetricKindCumulative
	filter := NewOverrideFieldMetadataFilter("metric", nil, nil, &cumKind, nil)
	filteredResult, err := filter.Filter(ctx, result)
	require.NoError(t, err)

	require.Len(t, filteredResult.FieldsMeta(), 1)
	assert.Equal(t, tsquery.MetricKindCumulative, filteredResult.FieldsMeta()[0].MetricKind())
}

// TestReportOverrideFieldMetadataFilter_PreservesKindWhenNil verifies that
// passing nil for optUpdatedMetricKind preserves the original kind.
func TestReportOverrideFieldMetadataFilter_PreservesKindWhenNil(t *testing.T) {
	ctx := context.Background()

	originalMeta, err := tsquery.NewFieldMetaFull(
		"metric",
		tsquery.DataTypeDecimal,
		tsquery.MetricKindCumulative,
		true,
		"",
		nil,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{1.0}, Timestamp: time.Unix(0, 0)},
	}
	result := NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Override URN but not kind
	newUrn := "renamed"
	filter := NewOverrideFieldMetadataFilter("metric", &newUrn, nil, nil, nil)
	filteredResult, err := filter.Filter(ctx, result)
	require.NoError(t, err)

	require.Len(t, filteredResult.FieldsMeta(), 1)
	assert.Equal(t, "renamed", filteredResult.FieldsMeta()[0].Urn())
	assert.Equal(t, tsquery.MetricKindCumulative, filteredResult.FieldsMeta()[0].MetricKind())
}

// TestAppendFieldFilter_PropagatesKind verifies that end-to-end, a report
// filter pipeline using AppendFieldFilter propagates MetricKind from a referenced
// cumulative source field into the newly-appended field.
func TestAppendFieldFilter_PropagatesKind(t *testing.T) {
	ctx := context.Background()

	originalMeta, err := tsquery.NewFieldMetaFull(
		"energy",
		tsquery.DataTypeDecimal,
		tsquery.MetricKindCumulative,
		true,
		"kWh",
		nil,
	)
	require.NoError(t, err)

	records := []timeseries.TsRecord[[]any]{
		{Value: []any{100.0}, Timestamp: time.Unix(0, 0)},
	}
	result := NewResult([]tsquery.FieldMeta{*originalMeta}, stream.Just(records...))

	// Append a new field that references the cumulative energy field
	appendFilter := NewAppendFieldFilter(
		NewRefFieldValue("energy"),
		tsquery.AddFieldMeta{Urn: "energy_copy"},
	)

	filteredResult, err := appendFilter.Filter(ctx, result)
	require.NoError(t, err)

	// Original field + new field
	require.Len(t, filteredResult.FieldsMeta(), 2)
	// Original kind preserved
	assert.Equal(t, tsquery.MetricKindCumulative, filteredResult.FieldsMeta()[0].MetricKind())
	// Appended field inherits kind from source (via RefFieldValue → ValueMeta → PrepareField)
	assert.Equal(t, tsquery.MetricKindCumulative, filteredResult.FieldsMeta()[1].MetricKind())
}
