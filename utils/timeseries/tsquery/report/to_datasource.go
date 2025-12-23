package report

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"time"
)

type toDatasource struct {
	reportDs DataSource
	fieldUrn string
}

// ToDatasource creates a standard datasource from a report datasource by extracting a single field by URN
func ToDatasource(reportDs DataSource, fieldUrn string) datasource.DataSource {
	return toDatasource{
		reportDs: reportDs,
		fieldUrn: fieldUrn,
	}
}

func (t toDatasource) Execute(ctx context.Context, from time.Time, to time.Time) (datasource.Result, error) {
	reportResult, err := t.reportDs.Execute(ctx, from, to)
	if err != nil {
		return datasource.Result{}, err
	}

	// Find the field index by URN
	fieldsMeta := reportResult.FieldsMeta()
	fieldIdx := -1
	var fieldMeta tsquery.FieldMeta
	for i, fm := range fieldsMeta {
		if fm.Urn() == t.fieldUrn {
			fieldIdx = i
			fieldMeta = fm
			break
		}
	}

	if fieldIdx == -1 {
		return util.DefaultValue[datasource.Result](), fmt.Errorf("field URN %s not found in report datasource", t.fieldUrn)
	}

	// Map the report stream to extract just the single field value
	dataStream := stream.Map(
		reportResult.Stream(),
		func(record timeseries.TsRecord[[]any]) timeseries.TsRecord[any] {
			var value any
			if fieldIdx < len(record.Value) {
				value = record.Value[fieldIdx]
			}
			return timeseries.TsRecord[any]{
				Timestamp: record.Timestamp,
				Value:     value,
			}
		},
	)

	return datasource.NewResult(fieldMeta, dataStream), nil
}
