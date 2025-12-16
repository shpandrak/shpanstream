package tsquery

import (
	"fmt"
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

type ValueMeta struct {
	DataType   DataType
	Unit       string
	Required   bool
	CustomMeta map[string]any
}

type AddFieldMeta struct {
	Urn          string
	CustomMeta   map[string]any
	OverrideUnit string
}
