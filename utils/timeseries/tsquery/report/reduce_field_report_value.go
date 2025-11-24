package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = ReduceFieldValue{}

type ReduceFieldValue struct {
	fieldUrnsToReduce map[string]bool // nil means reduce all fields
	reductionType     tsquery.ReductionType
}

func NewReduceAllFieldValues(reductionType tsquery.ReductionType) ReduceFieldValue {
	return ReduceFieldValue{
		fieldUrnsToReduce: nil,
		reductionType:     reductionType,
	}
}

func NewReduceFieldValues(fieldUrns []string, reductionType tsquery.ReductionType) ReduceFieldValue {
	fieldUrnsSet := make(map[string]bool, len(fieldUrns))
	for _, urn := range fieldUrns {
		fieldUrnsSet[urn] = true
	}
	return ReduceFieldValue{
		fieldUrnsToReduce: fieldUrnsSet,
		reductionType:     reductionType,
	}
}

func (r ReduceFieldValue) Execute(ctx context.Context, fieldsMeta []tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	// Determine which fields to reduce
	var fieldsToReduce []tsquery.FieldMeta
	var fieldIndices []int

	if r.fieldUrnsToReduce == nil {
		// Reduce all fields
		fieldsToReduce = fieldsMeta
		fieldIndices = make([]int, len(fieldsMeta))
		for i := range fieldsMeta {
			fieldIndices[i] = i
		}
	} else {
		// Reduce specific fields
		fieldsToReduce = make([]tsquery.FieldMeta, 0, len(r.fieldUrnsToReduce))
		fieldIndices = make([]int, 0, len(r.fieldUrnsToReduce))
		for i, meta := range fieldsMeta {
			if r.fieldUrnsToReduce[meta.Urn()] {
				fieldsToReduce = append(fieldsToReduce, meta)
				fieldIndices = append(fieldIndices, i)
			}
		}

		// Verify all requested fields were found
		if len(fieldsToReduce) != len(r.fieldUrnsToReduce) {
			missingFields := make([]string, 0)
			for urn := range r.fieldUrnsToReduce {
				found := false
				for _, meta := range fieldsMeta {
					if meta.Urn() == urn {
						found = true
						break
					}
				}
				if !found {
					missingFields = append(missingFields, urn)
				}
			}
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("cannot reduce fields, some fields not found: %v", missingFields)
		}
	}

	// Verify we have at least one field to reduce
	if len(fieldsToReduce) == 0 {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("no fields to reduce")
	}

	// Verify all fields are numeric, of the same type, and required
	dataType := fieldsToReduce[0].DataType()
	if dataType != tsquery.DataTypeInteger && dataType != tsquery.DataTypeDecimal {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("cannot reduce field %s: must be numeric (integer or decimal), got %s", fieldsToReduce[0].Urn(), dataType)
	}
	if !fieldsToReduce[0].Required() {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("cannot reduce field %s: must be required", fieldsToReduce[0].Urn())
	}

	// Track unit consistency and collect custom metadata
	firstUnit := fieldsToReduce[0].Unit()
	allSameUnit := true

	for i := 1; i < len(fieldsToReduce); i++ {
		if fieldsToReduce[i].DataType() != dataType {
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("cannot reduce fields: all fields must have the same data type, got %s for %s and %s for %s",
				dataType, fieldsToReduce[0].Urn(), fieldsToReduce[i].DataType(), fieldsToReduce[i].Urn())
		}
		if !fieldsToReduce[i].Required() {
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("cannot reduce field %s: must be required", fieldsToReduce[i].Urn())
		}
		if fieldsToReduce[i].Unit() != firstUnit {
			allSameUnit = false
		}
	}

	// Create result field metadata
	resultDataType := r.reductionType.GetResultDataType(dataType)

	// Determine result unit: preserve if all fields have the same unit
	var resultUnit string
	if allSameUnit {
		resultUnit = firstUnit
	}

	fvm := tsquery.ValueMeta{
		DataType: resultDataType,
		Unit:     resultUnit,
		Required: true,
	}

	// Pre-compute the reduction function based on type and data type
	reducerFunc, err := r.reductionType.GetReducerFunc(dataType)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed to get reduction function: %w", err)
	}

	// Create value supplier
	valueSupplier := func(_ context.Context, record timeseries.TsRecord[[]any]) (any, error) {
		// Extract values at the specified indices
		values := make([]any, len(fieldIndices))
		for i, idx := range fieldIndices {
			values[i] = record.Value[idx]
		}
		return reducerFunc(values), nil
	}

	return fvm, valueSupplier, nil
}
