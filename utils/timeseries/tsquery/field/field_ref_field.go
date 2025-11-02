package field

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Field = FieldRefField{}

// FieldRefField references a field from the source result by URN
type FieldRefField struct {
	urn string
}

// NewFieldRefField creates a new field reference that looks up a field by URN
func NewFieldRefField(urn string) FieldRefField {
	return FieldRefField{urn: urn}
}

// contextKey is a type for context keys to avoid collisions
type contextKey string

const sourceFieldMetasKey contextKey = "sourceFieldMetas"

// WithSourceFieldMetas adds source field metadata to the context
func WithSourceFieldMetas(ctx context.Context, fieldMetas []tsquery.FieldMeta) context.Context {
	return context.WithValue(ctx, sourceFieldMetasKey, fieldMetas)
}

func (frf FieldRefField) Execute(ctx context.Context) (tsquery.FieldMeta, ValueSupplier, error) {
	// Get source field metas from context
	sourceFieldMetas, ok := ctx.Value(sourceFieldMetasKey).([]tsquery.FieldMeta)
	if !ok {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("source field metadata not found in context for field reference %s", frf.urn)
	}

	// Find field with given URN
	var fieldMeta *tsquery.FieldMeta
	var fieldIndex int
	for i, fm := range sourceFieldMetas {
		if fm.Urn() == frf.urn {
			tmp := fm
			fieldMeta = &tmp
			fieldIndex = i
			break
		}
	}

	if fieldMeta == nil {
		return util.DefaultValue[tsquery.FieldMeta](), nil, fmt.Errorf("field with URN %s not found in source result", frf.urn)
	}

	valueSupplier := func(ctx context.Context, currRow timeseries.TsRecord[[]any]) (any, error) {
		if fieldIndex >= len(currRow.Value) {
			return nil, fmt.Errorf("field index %d out of bounds (row has %d values)", fieldIndex, len(currRow.Value))
		}
		return currRow.Value[fieldIndex], nil
	}

	return *fieldMeta, valueSupplier, nil
}
