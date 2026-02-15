package datasource

import (
	"context"
	"testing"
	"time"

	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/stretchr/testify/require"
)

// mockFilter is a simple filter that records whether it was applied.
type mockFilter struct {
	name string
}

func (m mockFilter) Filter(_ context.Context, result Result) (Result, error) {
	return result, nil
}

// mockFilterAwareDS is a DataSource that implements FilterAwareDataSource.
// It accepts filters whose name matches acceptFilterName.
type mockFilterAwareDS struct {
	inner            DataSource
	acceptFilterName string
	appliedFilters   []string
}

func (m *mockFilterAwareDS) Execute(ctx context.Context, from time.Time, to time.Time) (Result, error) {
	return m.inner.Execute(ctx, from, to)
}

func (m *mockFilterAwareDS) TryApplyFilter(filter Filter) (DataSource, bool) {
	if mf, ok := filter.(mockFilter); ok && mf.name == m.acceptFilterName {
		m.appliedFilters = append(m.appliedFilters, mf.name)
		return m, true
	}
	return m, false
}

func newTestDataSource(t *testing.T) DataSource {
	t.Helper()
	fieldMeta, err := tsquery.NewFieldMeta("field", tsquery.DataTypeDecimal, false)
	require.NoError(t, err)
	ds, err := NewStaticDatasource(*fieldMeta, stream.Just(
		timeseries.TsRecord[any]{Value: 1.0, Timestamp: time.Unix(0, 0)},
	))
	require.NoError(t, err)
	return ds
}

func TestNewFilteredDataSource_FilterAwareAccepts(t *testing.T) {
	inner := newTestDataSource(t)
	aware := &mockFilterAwareDS{inner: inner, acceptFilterName: "align"}

	result := NewFilteredDataSource(aware, mockFilter{name: "align"})

	// The filter was handled internally, so result should be the aware DS itself (not wrapped)
	require.Equal(t, aware, result)
	require.Equal(t, []string{"align"}, aware.appliedFilters)
}

func TestNewFilteredDataSource_FilterAwareRejects(t *testing.T) {
	inner := newTestDataSource(t)
	aware := &mockFilterAwareDS{inner: inner, acceptFilterName: "align"}

	result := NewFilteredDataSource(aware, mockFilter{name: "delta"})

	// The filter was NOT handled, so result should be a filteredDataSource wrapper
	fds, ok := result.(*filteredDataSource)
	require.True(t, ok, "expected filteredDataSource wrapper")
	require.Len(t, fds.filters, 1)
	require.Empty(t, aware.appliedFilters)
}

func TestNewFilteredDataSource_RegularDataSource(t *testing.T) {
	ds := newTestDataSource(t)

	result := NewFilteredDataSource(ds, mockFilter{name: "align"}, mockFilter{name: "delta"})

	// Regular DS doesn't implement FilterAwareDataSource, all filters applied externally
	fds, ok := result.(*filteredDataSource)
	require.True(t, ok, "expected filteredDataSource wrapper")
	require.Len(t, fds.filters, 2)
}

func TestNewFilteredDataSource_MixedFilters(t *testing.T) {
	inner := newTestDataSource(t)
	aware := &mockFilterAwareDS{inner: inner, acceptFilterName: "align"}

	result := NewFilteredDataSource(aware,
		mockFilter{name: "align"}, // accepted
		mockFilter{name: "delta"}, // rejected
		mockFilter{name: "rate"},  // rejected
	)

	// One filter intercepted, two remain external
	fds, ok := result.(*filteredDataSource)
	require.True(t, ok, "expected filteredDataSource wrapper")
	require.Len(t, fds.filters, 2)
	require.Equal(t, []string{"align"}, aware.appliedFilters)
}
