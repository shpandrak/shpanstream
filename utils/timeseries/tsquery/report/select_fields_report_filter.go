package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Filter = SelectFieldsFilter{}

type SelectedField struct {
	Value Value
	Meta  tsquery.AddFieldMeta
}

// SelectFieldsFilter creates final result based only on selected fields (dropping all others)
type SelectFieldsFilter struct {
	selectedFields []SelectedField
}

func (s SelectFieldsFilter) Filter(ctx context.Context, result Result) (Result, error) {
	// Validate list is not empty
	if len(s.selectedFields) == 0 {
		return util.DefaultValue[Result](), fmt.Errorf("cannot select fields: list is empty")
	}

	// Prepare all fields and collect metadata and value suppliers
	newFieldsMeta := make([]tsquery.FieldMeta, 0, len(s.selectedFields))
	valueSuppliers := make([]ValueSupplier, 0, len(s.selectedFields))
	seenUrns := make(map[string]bool)

	for _, sf := range s.selectedFields {
		// Check for duplicate URNs
		if seenUrns[sf.Meta.Urn] {
			return util.DefaultValue[Result](), fmt.Errorf("cannot select fields: duplicate URN %s", sf.Meta.Urn)
		}
		seenUrns[sf.Meta.Urn] = true

		// Prepare field to get metadata and value supplier
		// Concatenate original fields with already-processed new fields so refs can resolve
		availableFields := append(result.FieldsMeta(), newFieldsMeta...)
		fieldMeta, valueSupplier, err := PrepareField(ctx, sf.Meta, sf.Value, availableFields)
		if err != nil {
			return util.DefaultValue[Result](), fmt.Errorf("failed preparing selected field %s: %w", sf.Meta.Urn, err)
		}

		newFieldsMeta = append(newFieldsMeta, *fieldMeta)
		valueSuppliers = append(valueSuppliers, valueSupplier)
	}

	return NewResult(
		newFieldsMeta,
		stream.MapWithErrAndCtx(result.Stream(), func(ctx context.Context, record timeseries.TsRecord[[]any]) (timeseries.TsRecord[[]any], error) {
			newValues := make([]any, len(valueSuppliers))
			for i, supplier := range valueSuppliers {
				value, err := supplier(ctx, record)
				if err != nil {
					return util.DefaultValue[timeseries.TsRecord[[]any]](), err
				}
				newValues[i] = value
			}

			return timeseries.TsRecord[[]any]{
				Timestamp: record.Timestamp,
				Value:     newValues,
			}, nil
		})), nil
}

func NewSelectFieldsFilter(selectedFields []SelectedField) *SelectFieldsFilter {
	return &SelectFieldsFilter{selectedFields: selectedFields}
}
