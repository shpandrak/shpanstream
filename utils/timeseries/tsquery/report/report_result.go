package report

import (
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

type Result struct {
	s          stream.Stream[timeseries.TsRecord[[]any]]
	fieldsMeta []tsquery.FieldMeta
}

func NewResult(fieldsMeta []tsquery.FieldMeta, s stream.Stream[timeseries.TsRecord[[]any]]) Result {
	return Result{s: s, fieldsMeta: fieldsMeta}
}

func (r Result) FieldsMeta() []tsquery.FieldMeta {
	return r.fieldsMeta
}

func (r Result) GetField(sourceUrn string, urn string) (tsquery.FieldMeta, error) {
	for _, fm := range r.fieldsMeta {
		if fm.Urn() == urn {
			return fm, nil
		}
	}
	return util.DefaultValue[tsquery.FieldMeta](), fmt.Errorf("field meta not found for source urn %s and urn %s", sourceUrn, urn)
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
