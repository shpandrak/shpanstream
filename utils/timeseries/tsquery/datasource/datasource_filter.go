package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

func PrepareFieldValue(
	meta tsquery.AddFieldMeta,
	value Value,
	fieldMeta tsquery.FieldMeta,
) (*tsquery.FieldMeta, ValueSupplier, error) {
	valueMeta, valueSupplier, err := value.Execute(fieldMeta)
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
	Filter(result Result) (Result, error)
}

func ApplyFilters(result Result, filters ...Filter) (Result, error) {
	var err error
	for _, filter := range filters {
		result, err = filter.Filter(result)
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
	return ApplyFilters(result, f.filters...)
}

func NewFilteredDataSource(dataSource DataSource, filters ...Filter) DataSource {
	return &filteredDataSource{
		dataSource: dataSource,
		filters:    filters,
	}
}
