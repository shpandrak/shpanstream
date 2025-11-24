package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

// ReplaceFieldFilter replaces an existing field with a new field value.
type ReplaceFieldFilter struct {
	fieldUrnToReplace string
	updatedValue      Value
	updatedMeta       tsquery.AddFieldMeta
}

func NewReplaceFieldFilter(fieldUrnToReplace string, updatedValue Value, updatedMeta tsquery.AddFieldMeta) ReplaceFieldFilter {
	return ReplaceFieldFilter{fieldUrnToReplace: fieldUrnToReplace, updatedValue: updatedValue, updatedMeta: updatedMeta}
}

func (r ReplaceFieldFilter) Filter(ctx context.Context, result Result) (Result, error) {
	// Verify the field to replace exists
	if !result.HasField(r.fieldUrnToReplace) {
		return util.DefaultValue[Result](), fmt.Errorf("cannot replace field, since it does not exist: %s", r.fieldUrnToReplace)
	}

	// Prepare field to get metadata and value supplier
	fieldMeta, valueSupplier, err := PrepareField(ctx, r.updatedMeta, r.updatedValue, result.FieldsMeta())
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed preparing field for replacement: %w", err)
	}

	// Find the index of the field to replace
	fieldsMeta := result.FieldsMeta()
	replaceIdx := -1
	for i, fm := range fieldsMeta {
		if fm.Urn() == r.fieldUrnToReplace {
			replaceIdx = i
			break
		}
	}

	// Create new fieldsMeta with the field replaced
	newFieldsMeta := make([]tsquery.FieldMeta, len(fieldsMeta))
	copy(newFieldsMeta, fieldsMeta)
	newFieldsMeta[replaceIdx] = *fieldMeta

	return NewResult(
		newFieldsMeta,
		stream.MapWithErrAndCtx(result.Stream(), func(ctx context.Context, record timeseries.TsRecord[[]any]) (timeseries.TsRecord[[]any], error) {
			value, err := valueSupplier(ctx, record)
			if err != nil {
				return util.DefaultValue[timeseries.TsRecord[[]any]](), err
			}

			// Create a new value slice with the replaced value
			newValue := make([]any, len(record.Value))
			copy(newValue, record.Value)
			newValue[replaceIdx] = value

			return timeseries.TsRecord[[]any]{
				Timestamp: record.Timestamp,
				Value:     newValue,
			}, nil
		})), nil
}
