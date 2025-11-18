package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

type JoinType int

const (
	InnerJoin JoinType = iota
	LeftJoin
	FullJoin
)

type JoinDatasource struct {
	joinType JoinType
	multiDs  MultiDataSource
}

func NewJoinDatasource(multiDs MultiDataSource, joinType JoinType) JoinDatasource {
	return JoinDatasource{
		multiDs:  multiDs,
		joinType: joinType,
	}
}

func (mds JoinDatasource) Execute(ctx context.Context, from time.Time, to time.Time) (tsquery.Result, error) {
	var sourceStreams []stream.Stream[timeseries.TsRecord[[]any]]
	downStreamResults, err := stream.MapWithErrAndCtx(
		mds.multiDs.GetDatasources(ctx),
		func(ctx context.Context, ds DataSource) (tsquery.Result, error) {
			return ds.Execute(ctx, from, to)
		},
	).Collect(ctx)

	if err != nil {
		return util.DefaultValue[tsquery.Result](),
			fmt.Errorf("merge datasource failed collecting down downstream datasources: %w", err)
	}
	idxToNumberOfExpectedFields := map[int]int{}
	var joinedFieldMeta []tsquery.FieldMeta
	fieldUrnsSet := map[string]bool{}

	for idx, ds := range downStreamResults {
		sourceStreams = append(sourceStreams, ds.Stream())
		currFieldsMeta := ds.FieldsMeta()
		idxToNumberOfExpectedFields[idx] = len(currFieldsMeta)
		joinedFieldMeta = append(joinedFieldMeta, currFieldsMeta...)
		// Check for duplicate field urns
		for fIdx := range currFieldsMeta {
			if b := fieldUrnsSet[currFieldsMeta[fIdx].Urn()]; b {
				return util.DefaultValue[tsquery.Result](), fmt.Errorf("duplicate field urn %s found while joining datasources", currFieldsMeta[fIdx].Urn())
			}
		}
	}
	var joinedStreams stream.Stream[timeseries.TsRecord[[]any]]
	switch mds.joinType {
	case InnerJoin:
		joinedStreams = timeseries.InnerJoinStreams[[]any, []any](
			sourceStreams,
			func(values [][]any) []any {
				// Concatenate all values from all streams
				var ret []any
				for _, currValue := range values {
					ret = append(ret, currValue...)
				}
				return ret
			},
		)
	case FullJoin:
		joinedStreams = timeseries.FullJoinStreams[[]any, []any](
			sourceStreams,
			func(values []*[]any) []any {
				var ret []any
				for i, currValue := range values {
					if currValue != nil {
						ret = append(ret, *currValue...)
					} else {
						// Append nil values for missing fields
						ret = append(ret, make([]any, idxToNumberOfExpectedFields[i])...)
					}
				}
				return ret
			},
		)
	case LeftJoin:
		joinedStreams = timeseries.LeftJoinStreams[[]any, []any](
			sourceStreams,
			func(left []any, others []*[]any) []any {
				ret := left
				for i, currOther := range others {
					if currOther != nil {
						ret = append(ret, *currOther...)
					} else {
						ret = append(ret, make([]any, idxToNumberOfExpectedFields[i+1])...)
					}
				}
				return ret
			},
		)

	default:
		return util.DefaultValue[tsquery.Result](), fmt.Errorf("unsupported join type %d", mds.joinType)
	}
	return tsquery.NewResult(
		joinedFieldMeta,
		joinedStreams,
	), nil

}
