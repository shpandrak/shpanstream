package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

type StaticDatasource struct {
	result tsquery.Result
}

func NewStaticDatasource(fieldsMeta []tsquery.FieldMeta, recordStream stream.Stream[timeseries.TsRecord[[]any]]) (*StaticDatasource, error) {
	// Validate fields meta is not empty
	if len(fieldsMeta) == 0 {
		return nil, fmt.Errorf("fieldsMeta cannot be empty")
	}

	// Validate no collisions in field URNs
	urnMap := make(map[string]bool)
	for _, meta := range fieldsMeta {
		urn := meta.Urn()
		if urnMap[urn] {
			return nil, fmt.Errorf("duplicate field URN found: %s", urn)
		}
		urnMap[urn] = true
	}

	return &StaticDatasource{
		result: tsquery.NewResult(fieldsMeta, recordStream),
	}, nil
}

func (s StaticDatasource) Execute(
	_ context.Context,
	from time.Time,
	to time.Time,
) (tsquery.Result, error) {
	return tsquery.NewResult(
		s.result.FieldsMeta(),
		s.result.Stream().Filter(func(src timeseries.TsRecord[[]any]) bool {
			return !src.Timestamp.Before(from) && src.Timestamp.Before(to)
		}),
	), nil
}