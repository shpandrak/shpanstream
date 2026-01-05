package datasource

import (
	"context"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type ValueSupplier func(ctx context.Context, currRow timeseries.TsRecord[any]) (any, error)

type Value interface {
	Execute(ctx context.Context, fieldMeta tsquery.FieldMeta) (tsquery.ValueMeta, ValueSupplier, error)
}

// StaticValue represents a value that doesn't depend on row/field context.
// Examples: constants, nil values.
// This interface allows execution without requiring a placeholder FieldMeta.
type StaticValue interface {
	ExecuteStatic(ctx context.Context) (tsquery.ValueMeta, ValueSupplier, error)
}
