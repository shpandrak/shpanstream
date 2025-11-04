package tsquery

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
)

type Record timeseries.TsRecord[[]any]

type FieldMeta struct {
	urn        string
	dataType   DataType
	unit       string
	required   bool
	customMeta map[string]any
}

func (fm FieldMeta) GetCustomMetaStringValue(key string) string {
	if fm.customMeta == nil {
		return ""
	}
	if val, ok := fm.customMeta[key]; ok {
		return fmt.Sprintf("%s", val)
	}
	return ""

}

func (fm FieldMeta) Required() bool {
	return fm.required
}

func (fm FieldMeta) Urn() string {
	return fm.urn
}

func (fm FieldMeta) Unit() string {
	return fm.unit
}

func (fm FieldMeta) DataType() DataType {
	return fm.dataType
}

func (fm FieldMeta) CustomMeta() map[string]any {
	return fm.customMeta
}

func NewFieldMeta(urn string, dataType DataType, required bool) (*FieldMeta, error) {
	return NewFieldMetaWithCustomData(urn, dataType, required, "", nil)
}

func NewFieldMetaWithCustomData(
	urn string,
	dataType DataType,
	required bool,
	unit string,
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
		unit:       unit,
		customMeta: customMeta,
	}, nil

}

type Result struct {
	s          stream.Stream[timeseries.TsRecord[[]any]]
	fieldsMeta []FieldMeta
}

func NewResult(fieldsMeta []FieldMeta, s stream.Stream[timeseries.TsRecord[[]any]]) Result {
	return Result{s: s, fieldsMeta: fieldsMeta}
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

func (r Result) Stream() stream.Stream[timeseries.TsRecord[[]any]] {
	return r.s
}
