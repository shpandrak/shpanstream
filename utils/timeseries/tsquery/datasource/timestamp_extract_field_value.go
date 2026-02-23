package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

var _ Value = TimestampExtractFieldValue{}

type TimestampExtractFieldValue struct {
	source    Value
	component tsquery.TimestampExtractComponent
	location  *time.Location
}

func NewTimestampExtractFieldValue(source Value, component tsquery.TimestampExtractComponent, location *time.Location) TimestampExtractFieldValue {
	return TimestampExtractFieldValue{source: source, component: component, location: location}
}

func (te TimestampExtractFieldValue) Execute(ctx context.Context, fieldMeta tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error) {
	if te.location == nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("timestampExtract location must not be nil")
	}

	sourceMeta, sourceSupplier, err := te.source.Execute(ctx, fieldMeta)
	if err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed to execute source for timestampExtract: %w", err)
	}

	if sourceMeta.DataType != tsquery.DataTypeTimestamp {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf(
			"timestampExtract source must be timestamp type, got %s", sourceMeta.DataType,
		)
	}

	if err := te.component.Validate(); err != nil {
		return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("invalid timestampExtract component: %w", err)
	}

	valueMeta := tsquery.ValueMeta{
		DataType: tsquery.DataTypeInteger,
		Required: sourceMeta.Required,
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[any]) (any, error) {
		val, err := sourceSupplier(ctx, currRow)
		if err != nil {
			return nil, fmt.Errorf("failed to get source value for timestampExtract: %w", err)
		}
		if val == nil {
			return nil, nil
		}
		t := val.(time.Time).In(te.location)
		return tsquery.ExtractTimestampComponent(t, te.component)
	}

	return valueMeta, valueSupplier, nil
}
