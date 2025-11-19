package report

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

// DropFieldsFilter drops a field(s) from the existing result
type DropFieldsFilter struct {
	fieldUrnsSetToRemove map[string]bool
}

func NewDropFieldsFilter(fieldUrns ...string) DropFieldsFilter {
	fieldUrnsSet := make(map[string]bool)
	for _, urn := range fieldUrns {
		fieldUrnsSet[urn] = true
	}
	return DropFieldsFilter{
		fieldUrnsSetToRemove: fieldUrnsSet,
	}
}

func (a DropFieldsFilter) Filter(result Result) (Result, error) {
	// We need to verify that fields to drop exist and that the remaining result is not empty
	fieldsMeta := result.FieldsMeta()
	fieldIdxToKeep := make([]int, 0, len(fieldsMeta)-len(a.fieldUrnsSetToRemove))
	countFoundToDrop := 0
	for i, fieldMeta := range fieldsMeta {
		if !a.fieldUrnsSetToRemove[fieldMeta.Urn()] {
			fieldIdxToKeep = append(fieldIdxToKeep, i)
		} else {
			// Field to drop found
			countFoundToDrop++
		}
	}

	if len(fieldIdxToKeep) == 0 {
		return util.DefaultValue[Result](), fmt.Errorf("cannot drop all fields from the result")
	}
	if countFoundToDrop != len(a.fieldUrnsSetToRemove) {
		// Build a list of missing fields
		existingFieldUrns := stream.MustCollectToSet(stream.Map(
			stream.FromSlice(fieldsMeta),
			tsquery.FieldMeta.Urn,
		))
		missingFields := stream.FromMapKeys(a.fieldUrnsSetToRemove).Filter(func(urn string) bool {
			return !existingFieldUrns[urn]
		}).MustCollect()
		return util.DefaultValue[Result](), fmt.Errorf("cannot drop fields, since they do not exist: %v", missingFields)
	}

	// Build new fieldsMeta with pre-allocated capacity
	newFieldsMeta := make([]tsquery.FieldMeta, len(fieldIdxToKeep))
	for i, idx := range fieldIdxToKeep {
		newFieldsMeta[i] = fieldsMeta[idx]
	}

	return NewResult(
		newFieldsMeta,
		stream.Map(result.Stream(), func(record timeseries.TsRecord[[]any]) timeseries.TsRecord[[]any] {
			// Build a new value slice with pre-allocated capacity
			newValue := make([]any, len(fieldIdxToKeep))
			for i, idx := range fieldIdxToKeep {
				newValue[i] = record.Value[idx]
			}
			return timeseries.TsRecord[[]any]{
				Timestamp: record.Timestamp,
				Value:     newValue,
			}
		}),
	), nil
}
