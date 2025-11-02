package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"reflect"
	"time"
)

type StaticStructDatasource struct {
	result tsquery.Result
}

func NewStaticStructDatasource[T any](structStream stream.Stream[T]) (*StaticStructDatasource, error) {
	// Get the type of T
	var zero T
	typ := reflect.TypeOf(zero)

	// Ensure T is a struct
	if typ.Kind() != reflect.Struct {
		return nil, fmt.Errorf("T must be a struct, got %v", typ.Kind())
	}

	// Validate struct and build metadata
	var timestampFieldIndex = -1
	var fieldsMeta []tsquery.FieldMeta
	var dataFieldIndices []int

	typeName := typ.Name()
	numFields := typ.NumField()

	for i := 0; i < numFields; i++ {
		field := typ.Field(i)

		// Skip unexported fields
		if !field.IsExported() {
			continue
		}

		// Check if this is the timestamp field
		if field.Type == reflect.TypeOf(time.Time{}) {
			if timestampFieldIndex != -1 {
				return nil, fmt.Errorf("struct must have exactly one time.Time field, found multiple")
			}
			timestampFieldIndex = i
			continue
		}

		// Validate and get data type for other fields
		dataType, err := tsquery.DataTypeByGoType(field.Type.Kind())
		if err != nil {
			return nil, fmt.Errorf("field %s has invalid type %v: %w", field.Name, field.Type, err)
		}

		// Create field metadata with URN as "TypeName:FieldName"
		urn := fmt.Sprintf("%s:%s", typeName, field.Name)
		meta, err := tsquery.NewFieldMeta(urn, dataType, false)
		if err != nil {
			return nil, fmt.Errorf("failed to create field meta for %s: %w", field.Name, err)
		}

		fieldsMeta = append(fieldsMeta, *meta)
		dataFieldIndices = append(dataFieldIndices, i)
	}

	// Ensure we found exactly one timestamp field
	if timestampFieldIndex == -1 {
		return nil, fmt.Errorf("struct must have exactly one public time.Time field")
	}

	// Create the record stream using stream.Map
	recordStream := stream.Map(structStream, func(s T) timeseries.TsRecord[[]any] {
		val := reflect.ValueOf(s)

		// Extract timestamp
		timestamp := val.Field(timestampFieldIndex).Interface().(time.Time)

		// Extract data fields
		data := make([]any, len(dataFieldIndices))
		for i, fieldIdx := range dataFieldIndices {
			data[i] = val.Field(fieldIdx).Interface()
		}

		return timeseries.TsRecord[[]any]{
			Timestamp: timestamp,
			Value:     data,
		}
	})

	return &StaticStructDatasource{
		result: tsquery.NewResult(fieldsMeta, recordStream),
	}, nil
}

func (s StaticStructDatasource) Execute(
	_ context.Context,
	from time.Time,
	to time.Time,
) (tsquery.Result, error) {
	return tsquery.NewResult(
		s.result.FieldsMeta(),
		s.result.Stream().Filter(func(src timeseries.TsRecord[[]any]) bool {
			return !src.Timestamp.Before(from) && src.Timestamp.Before(to)
		}),
	), nil
}
