package tsquery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetricKind_Validate(t *testing.T) {
	// Valid kinds
	require.NoError(t, MetricKind("").Validate())
	require.NoError(t, MetricKindGauge.Validate())
	require.NoError(t, MetricKindDelta.Validate())
	require.NoError(t, MetricKindCumulative.Validate())
	require.NoError(t, MetricKindRate.Validate())

	// Invalid kinds
	require.Error(t, MetricKind("unknown").Validate())
	require.Error(t, MetricKind("GAUGE").Validate())
}

func TestFieldMeta_MetricKind_DefaultIsGauge(t *testing.T) {
	fm, err := NewFieldMeta("test", DataTypeDecimal, true)
	require.NoError(t, err)
	require.Equal(t, MetricKindGauge, fm.MetricKind())
}

func TestFieldMeta_MetricKind_ExplicitGauge(t *testing.T) {
	fm, err := NewFieldMetaFull("test", DataTypeDecimal, MetricKindGauge, true, "", nil)
	require.NoError(t, err)
	require.Equal(t, MetricKindGauge, fm.MetricKind())
}

func TestFieldMeta_WithMetricKind(t *testing.T) {
	fm, err := NewFieldMeta("test", DataTypeDecimal, true)
	require.NoError(t, err)

	// WithMetricKind returns a copy, original unchanged
	cumulative := fm.WithMetricKind(MetricKindCumulative)
	require.Equal(t, MetricKindGauge, fm.MetricKind())
	require.Equal(t, MetricKindCumulative, cumulative.MetricKind())

	// Chain WithMetricKind
	delta := cumulative.WithMetricKind(MetricKindDelta)
	require.Equal(t, MetricKindDelta, delta.MetricKind())
	require.Equal(t, MetricKindCumulative, cumulative.MetricKind())
}

func TestNewFieldMetaFull(t *testing.T) {
	fm, err := NewFieldMetaFull(
		"lifetimeEnergy",
		DataTypeDecimal,
		MetricKindCumulative,
		true,
		"kWh",
		map[string]any{"source": "meter"},
	)
	require.NoError(t, err)
	require.Equal(t, "lifetimeEnergy", fm.Urn())
	require.Equal(t, DataTypeDecimal, fm.DataType())
	require.Equal(t, MetricKindCumulative, fm.MetricKind())
	require.True(t, fm.Required())
	require.Equal(t, "kWh", fm.Unit())
	require.Equal(t, "meter", fm.GetCustomMetaStringValue("source"))
}

func TestNewFieldMetaFull_InvalidMetricKind(t *testing.T) {
	_, err := NewFieldMetaFull("test", DataTypeDecimal, MetricKind("bogus"), true, "", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid metric kind")
}

func TestNewFieldMetaFull_AllKinds(t *testing.T) {
	for _, kind := range []MetricKind{MetricKindGauge, MetricKindDelta, MetricKindCumulative, MetricKindRate} {
		fm, err := NewFieldMetaFull("test", DataTypeDecimal, kind, true, "", nil)
		require.NoError(t, err, "kind: %s", kind)
		require.Equal(t, kind, fm.MetricKind())
	}
}

func TestNewFieldMetaWithCustomData_BackwardCompat(t *testing.T) {
	// Existing constructor defaults kind to gauge
	fm, err := NewFieldMetaWithCustomData("test", DataTypeDecimal, true, "kWh", nil)
	require.NoError(t, err)
	require.Equal(t, MetricKindGauge, fm.MetricKind())
}

// --- SamplePeriod tests ---

func TestFieldMeta_SamplePeriod_DefaultNil(t *testing.T) {
	fm, err := NewFieldMeta("test", DataTypeDecimal, true)
	require.NoError(t, err)
	require.Nil(t, fm.SamplePeriod())
}

func TestFieldMeta_WithSamplePeriod(t *testing.T) {
	fm, err := NewFieldMeta("test", DataTypeDecimal, true)
	require.NoError(t, err)

	fiveMin := 5 * time.Minute
	withSP := fm.WithSamplePeriod(fiveMin)

	// Original unchanged
	require.Nil(t, fm.SamplePeriod())

	// Copy has the value
	require.NotNil(t, withSP.SamplePeriod())
	require.Equal(t, fiveMin, *withSP.SamplePeriod())
}

func TestFieldMeta_WithSamplePeriod_ChainedWithMetricKind(t *testing.T) {
	fm, err := NewFieldMetaFull("energy", DataTypeDecimal, MetricKindDelta, true, "kWh", nil)
	require.NoError(t, err)

	fiveMin := 5 * time.Minute
	fm2 := fm.WithSamplePeriod(fiveMin)

	require.Equal(t, MetricKindDelta, fm2.MetricKind())
	require.NotNil(t, fm2.SamplePeriod())
	require.Equal(t, fiveMin, *fm2.SamplePeriod())
	require.Equal(t, "kWh", fm2.Unit())
}
