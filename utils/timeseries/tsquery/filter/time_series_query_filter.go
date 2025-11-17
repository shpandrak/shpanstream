package filter

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
	"time"
)

type AddFieldMeta struct {
	Urn          string
	CustomMeta   map[string]any
	OverrideUnit string
}

func PrepareField(meta AddFieldMeta, value field.Value, fieldsMeta []tsquery.FieldMeta) (*tsquery.FieldMeta, field.ValueSupplier, error) {
	valueMeta, valueSupplier, err := value.Execute(fieldsMeta)
	if err != nil {
		return nil, nil, fmt.Errorf("failed executing field %s: %w", meta.Urn, err)
	}
	unit := valueMeta.Unit
	if meta.OverrideUnit != "" {
		unit = meta.OverrideUnit
	}
	fm, err := tsquery.NewFieldMetaWithCustomData(
		meta.Urn,
		valueMeta.DataType,
		valueMeta.Required,
		unit,
		meta.CustomMeta,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed creating field meta for %s: %w", meta.Urn, err)
	}
	return fm, valueSupplier, nil

}

type Filter interface {
	Filter(result tsquery.Result) (tsquery.Result, error)
}

func ApplyFilters(result tsquery.Result, filters ...Filter) (tsquery.Result, error) {
	var err error
	for _, filter := range filters {
		result, err = filter.Filter(result)
		if err != nil {
			return tsquery.Result{}, err
		}
	}
	return result, nil
}

type filteredDataSource struct {
	dataSource datasource.DataSource
	filters    []Filter
}

func (f filteredDataSource) Execute(ctx context.Context, from time.Time, to time.Time) (tsquery.Result, error) {
	result, err := f.dataSource.Execute(ctx, from, to)
	if err != nil {
		return util.DefaultValue[tsquery.Result](), err
	}
	return ApplyFilters(result, f.filters...)
}

func NewFilteredDataSource(dataSource datasource.DataSource, filters ...Filter) datasource.DataSource {
	return &filteredDataSource{
		dataSource: dataSource,
		filters:    filters,
	}
}
