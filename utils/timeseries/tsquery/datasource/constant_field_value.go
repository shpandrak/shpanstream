package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Value = ConstantFieldValue{}
var _ StaticValue = ConstantFieldValue{}

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
	var valueToUse = cf.value
	if valueToUse == nil {
		if cf.meta.Required {
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("cannot execute constant field: value is required")
		}
	} else {
		var err error
		valueToUse, err = cf.meta.DataType.ForceCastAndValidate(valueToUse)
		if err != nil {
			return util.DefaultValue[tsquery.ValueMeta](), nil, fmt.Errorf("failed validating constant field data: %w", err)
		}
	}

	valueSupplier := func(_ context.Context, _ timeseries.TsRecord[any]) (any, error) {
		return valueToUse, nil
	}
	return cf.meta, valueSupplier, nil
}

func (cf ConstantFieldValue) ExecuteStatic(ctx context.Context) (tsquery.ValueMeta, ValueSupplier, error) {
	return cf.Execute(ctx, tsquery.FieldMeta{})
}
