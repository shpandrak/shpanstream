package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = ConstantFieldValue{}

type ConstantFieldValue struct {
	meta  tsquery.ValueMeta
	value any
}

func NewConstantFieldValue(meta tsquery.ValueMeta, value any) ConstantFieldValue {
	return ConstantFieldValue{meta, value}
}

func (cf ConstantFieldValue) Execute(
	_ context.Context,
	_ tsquery.FieldMeta,
) (tsquery.ValueMeta, ValueSupplier, error) {
	// Validate on execute (lazy validation)
	if cf.value == nil {
		if cf.meta.Required {
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("cannot execute constant field: value is required")
		}
	} else {
		err := cf.meta.DataType.ValidateData(cf.value)
		if err != nil {
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed validating constant field data: %w", err)
		}
	}

	valueSupplier := func(_ context.Context, _ timeseries.TsRecord[any]) (any, error) {
		return cf.value, nil
	}
	return cf.meta, valueSupplier, nil
}
