package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Field = ReduceField{}

type ReductionType string

const (
	ReductionTypeSum   ReductionType = "sum"
	ReductionTypeAvg   ReductionType = "avg"
	ReductionTypeMin   ReductionType = "min"
	ReductionTypeMax   ReductionType = "max"
	ReductionTypeCount ReductionType = "count"
)

type ReduceField struct {
	fieldUrnsToReduce map[string]bool // nil means reduce all fields
	reductionType     ReductionType
	resultUrn         string
	resultCustomMeta  map[string]any
}

func NewReduceAllFields(resultUrn string, reductionType ReductionType, resultCustomMeta map[string]any) ReduceField {
	return ReduceField{
		resultCustomMeta:  resultCustomMeta,
		fieldUrnsToReduce: nil,
		reductionType:     reductionType,
		resultUrn:         resultUrn,
	}
}

func NewReduceFields(resultUrn string, fieldUrns []string, reductionType ReductionType, resultCustomMeta map[string]any) ReduceField {
	fieldUrnsSet := make(map[string]bool, len(fieldUrns))
	for _, urn := range fieldUrns {
		fieldUrnsSet[urn] = true
	}
	return ReduceField{
		fieldUrnsToReduce: fieldUrnsSet,
		reductionType:     reductionType,
		resultUrn:         resultUrn,
		resultCustomMeta:  resultCustomMeta,
	}
}

func (r ReduceField) Execute(fieldsMeta []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
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
			return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("cannot reduce fields, some fields not found: %v", missingFields)
		}
	}

	// Verify we have at least one field to reduce
	if len(fieldsToReduce) == 0 {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("no fields to reduce")
	}

	// Verify all fields are numeric, of the same type, and required
	dataType := fieldsToReduce[0].DataType()
	if dataType != tsquery.DataTypeInteger && dataType != tsquery.DataTypeDecimal {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("cannot reduce field %s: must be numeric (integer or decimal), got %s", fieldsToReduce[0].Urn(), dataType)
	}
	if !fieldsToReduce[0].Required() {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("cannot reduce field %s: must be required", fieldsToReduce[0].Urn())
	}

	// Track unit consistency and collect custom metadata
	firstUnit := fieldsToReduce[0].Unit()
	allSameUnit := true

	for i := 1; i < len(fieldsToReduce); i++ {
		if fieldsToReduce[i].DataType() != dataType {
			return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("cannot reduce fields: all fields must have the same data type, got %s for %s and %s for %s",
				dataType, fieldsToReduce[0].Urn(), fieldsToReduce[i].DataType(), fieldsToReduce[i].Urn())
		}
		if !fieldsToReduce[i].Required() {
			return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("cannot reduce field %s: must be required", fieldsToReduce[i].Urn())
		}
		if fieldsToReduce[i].Unit() != firstUnit {
			allSameUnit = false
		}
	}

	// Create result field metadata
	var resultDataType tsquery.DataType
	if r.reductionType == ReductionTypeAvg {
		// Average always returns decimal
		resultDataType = tsquery.DataTypeDecimal
	} else if r.reductionType == ReductionTypeCount {
		// Count always returns integer
		resultDataType = tsquery.DataTypeInteger
	} else {
		// Sum, Min, Max preserve the input data type
		resultDataType = dataType
	}

	// Determine result unit: preserve if all fields have the same unit
	var resultUnit string
	if allSameUnit {
		resultUnit = firstUnit
	}

	resultMeta, err := tsquery.NewFieldMetaWithCustomData(r.resultUrn, resultDataType, true, resultUnit, r.resultCustomMeta)
	if err != nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed to create result field metadata: %w", err)
	}

	// Pre-compute the reduction function based on type and data type
	reducerFunc := createReducerFunc(r.reductionType, dataType)

	// Create value supplier
	valueSupplier := func(_ context.Context, record timeseries.TsRecord[[]any]) (any, error) {
		// Extract values at the specified indices
		values := make([]any, len(fieldIndices))
		for i, idx := range fieldIndices {
			values[i] = record.Value[idx]
		}
		return reducerFunc(values), nil
	}

	return *resultMeta, valueSupplier, nil
}

// createReducerFunc creates an optimized reducer function based on reduction type and data type
func createReducerFunc(reductionType ReductionType, dataType tsquery.DataType) func([]any) any {
	switch reductionType {
	case ReductionTypeSum:
		if dataType == tsquery.DataTypeInteger {
			return sumInt
		}
		return sumDecimal

	case ReductionTypeAvg:
		if dataType == tsquery.DataTypeInteger {
			return avgInt
		}
		return avgDecimal

	case ReductionTypeMin:
		if dataType == tsquery.DataTypeInteger {
			return minInt
		}
		return minDecimal

	case ReductionTypeMax:
		if dataType == tsquery.DataTypeInteger {
			return maxInt
		}
		return maxDecimal

	case ReductionTypeCount:
		// Count is independent of data type
		return countValues

	default:
		// This should never happen if validation is correct
		panic(fmt.Sprintf("unsupported reduction type: %s", reductionType))
	}
}

// Integer reduction functions
func sumInt(values []any) any {
	var sum int64
	for _, v := range values {
		sum += v.(int64)
	}
	return sum
}

func avgInt(values []any) any {
	var sum int64
	for _, v := range values {
		sum += v.(int64)
	}
	return float64(sum) / float64(len(values))
}

func minInt(values []any) any {
	minVal := values[0].(int64)
	for i := 1; i < len(values); i++ {
		val := values[i].(int64)
		if val < minVal {
			minVal = val
		}
	}
	return minVal
}

func maxInt(values []any) any {
	maxVal := values[0].(int64)
	for i := 1; i < len(values); i++ {
		val := values[i].(int64)
		if val > maxVal {
			maxVal = val
		}
	}
	return maxVal
}

// Decimal reduction functions
func sumDecimal(values []any) any {
	var sum float64
	for _, v := range values {
		sum += v.(float64)
	}
	return sum
}

func avgDecimal(values []any) any {
	var sum float64
	for _, v := range values {
		sum += v.(float64)
	}
	return sum / float64(len(values))
}

func minDecimal(values []any) any {
	minVal := values[0].(float64)
	for i := 1; i < len(values); i++ {
		val := values[i].(float64)
		if val < minVal {
			minVal = val
		}
	}
	return minVal
}

func maxDecimal(values []any) any {
	maxVal := values[0].(float64)
	for i := 1; i < len(values); i++ {
		val := values[i].(float64)
		if val > maxVal {
			maxVal = val
		}
	}
	return maxVal
}

// Count function (works for both integer and decimal)
func countValues(values []any) any {
	return int64(len(values))
}
