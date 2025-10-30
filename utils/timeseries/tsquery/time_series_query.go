package tsquery

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"time"
)

type Record timeseries.TsRecord[[]any]

type DataSource interface {
	Execute(ctx context.Context, from time.Time, to time.Time) (Result, error)
}

type FieldMeta struct {
	urn        string
	dataType   DataType
	required   bool
	customMeta map[string]any
}

func (f FieldMeta) Required() bool {
	return f.required
}

func (f FieldMeta) Urn() string {
	return f.urn
}

func (f FieldMeta) DataType() DataType {
	return f.dataType
}

func (f FieldMeta) CustomMeta() map[string]any {
	return f.customMeta
}

func NewFieldMeta(urn string, dataType DataType, required bool) (*FieldMeta, error) {
	return NewFieldMetaWithCustomData(urn, dataType, required, nil)
}

func NewFieldMetaWithCustomData(
	urn string,
	dataType DataType,
	required bool,
	customMeta map[string]any,
) (*FieldMeta, error) {
	if urn == "" {
		return nil, fmt.Errorf("field urn must not be empty")
	}
	if err := dataType.Validate(); err != nil {
		return nil, err
	}

	return &FieldMeta{
		urn:        urn,
		dataType:   dataType,
		required:   required,
		customMeta: customMeta,
	}, nil

}

type Result struct {
	s          stream.Stream[Record]
	fieldsMeta []FieldMeta
}

func NewResult(fieldsMeta []FieldMeta, s stream.Stream[Record]) *Result {
	return &Result{s: s, fieldsMeta: fieldsMeta}
}

func (r Result) FieldsMeta() []FieldMeta {
	return r.fieldsMeta
}

func (r Result) GetField(sourceUrn string, urn string) (FieldMeta, error) {
	for _, fm := range r.fieldsMeta {
		if fm.Urn() == urn {
			return fm, nil
		}
	}
	return util.DefaultValue[FieldMeta](), fmt.Errorf("field meta not found for source urn %s and urn %s", sourceUrn, urn)
}

func (r Result) HasField(urn string) bool {
	for _, fm := range r.fieldsMeta {
		if fm.Urn() == urn {
			return true
		}
	}
	return false
}

func (r Result) Stream() stream.Stream[Record] {
	return r.s
}
