package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

func PrepareFieldValue(
	ctx context.Context,
	meta tsquery.AddFieldMeta,
	value Value,
	fieldMeta tsquery.FieldMeta,
) (*tsquery.FieldMeta, ValueSupplier, error) {
	valueMeta, valueSupplier, err := value.Execute(ctx, fieldMeta)
	if err != nil {
		return nil, nil, fmt.Errorf("failed executing field %s: %w", meta.Urn, err)
	}
	unit := valueMeta.Unit
	if meta.OverrideUnit != "" {
		unit = meta.OverrideUnit
	}
	metricKind := valueMeta.MetricKind
	if meta.OverrideMetricKind != "" {
		metricKind = meta.OverrideMetricKind
	}
	var samplePeriod *time.Duration
	if valueMeta.SamplePeriod != nil {
		samplePeriod = valueMeta.SamplePeriod
	}
	if meta.OverrideSamplePeriod != "" {
		sp, err := time.ParseDuration(meta.OverrideSamplePeriod)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid OverrideSamplePeriod for field %s: %w", meta.Urn, err)
		}
		samplePeriod = &sp
	}
	fm, err := tsquery.NewFieldMetaFull(
		meta.Urn,
		valueMeta.DataType,
		metricKind,
		valueMeta.Required,
		unit,
		tsquery.MergeCustomMeta(valueMeta.CustomMeta, meta.CustomMeta),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating field meta for %s: %w", meta.Urn, err)
	}
	if samplePeriod != nil {
		*fm = fm.WithSamplePeriod(*samplePeriod)
	}
	return fm, valueSupplier, nil

}

type Filter interface {
	Filter(ctx context.Context, result Result) (Result, error)
}

func ApplyFilters(ctx context.Context, result Result, filters ...Filter) (Result, error) {
	var err error
	for _, filter := range filters {
		result, err = filter.Filter(ctx, result)
		if err != nil {
			return util.DefaultValue[Result](), err
		}
	}
	return result, nil
}

type filteredDataSource struct {
	dataSource DataSource
	filters    []Filter
}

func (f filteredDataSource) Execute(ctx context.Context, from time.Time, to time.Time) (Result, error) {
	result, err := f.dataSource.Execute(ctx, from, to)
	if err != nil {
		return util.DefaultValue[Result](), err
	}
	return ApplyFilters(ctx, result, f.filters...)
}

// FilterAwareDataSource is an optional interface that DataSources can implement
// to intercept and handle filters internally (e.g., pushing alignment to a database query).
type FilterAwareDataSource interface {
	DataSource
	// TryApplyFilter attempts to apply the filter internally.
	// Returns (modified DataSource, true) if the datasource handles the filter,
	// or (self, false) if the filter should be applied externally.
	TryApplyFilter(filter Filter) (DataSource, bool)
}

func NewFilteredDataSource(dataSource DataSource, filters ...Filter) DataSource {
	var externalFilters []Filter
	for _, f := range filters {
		if aware, ok := dataSource.(FilterAwareDataSource); ok {
			if newDS, handled := aware.TryApplyFilter(f); handled {
				dataSource = newDS
				continue
			}
		}
		externalFilters = append(externalFilters, f)
	}
	if len(externalFilters) == 0 {
		return dataSource
	}
	return &filteredDataSource{
		dataSource: dataSource,
		filters:    externalFilters,
	}
}
