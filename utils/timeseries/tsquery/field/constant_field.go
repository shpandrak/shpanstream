package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type ConstantField struct {
	meta  tsquery.FieldMeta
	value any
}

func NewConstantField(meta tsquery.FieldMeta, value any) (*ConstantField, error) {
	if value == nil {
		if meta.Required() {
			return nil, fmt.Errorf("cannot create constant field %s: value is required", meta.Urn())
		}
	} else {
		err := meta.DataType().ValidateData(value)
		if err != nil {
			return nil, fmt.Errorf("failed valiedating constant field data %s: %w", meta.Urn(), err)
		}
	}

	return &ConstantField{meta, value}, nil
}

func (cf *ConstantField) Meta() tsquery.FieldMeta {
	return cf.meta
}

func (cf *ConstantField) GetValue(_ context.Context) (any, error) {
	return cf.value, nil
}
