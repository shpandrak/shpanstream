package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Field = ConstantField{}

type ConstantField struct {
	meta  tsquery.FieldMeta
	value any
}

func NewConstantField(meta tsquery.FieldMeta, value any) ConstantField {
	return ConstantField{meta, value}
}

func (cf ConstantField) Execute(_ []tsquery.FieldMeta) (tsquery.FieldMeta, ValueSupplier, error) {
	// Validate on execute (lazy validation)
	if cf.value == nil {
		if cf.meta.Required() {
			return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("cannot execute constant field %s: value is required", cf.meta.Urn())
		}
	} else {
		err := cf.meta.DataType().ValidateData(cf.value)
		if err != nil {
			return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("failed validating constant field data %s: %w", cf.meta.Urn(), err)
		}
	}

	valueSupplier := func(_ context.Context, _ timeseries.TsRecord[[]any]) (any, error) {
		return cf.value, nil
	}
	return cf.meta, valueSupplier, nil
}
