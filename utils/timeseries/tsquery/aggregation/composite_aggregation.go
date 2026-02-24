package aggregation

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/lazy"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"time"
)

// CompositeAggregation runs multiple independent aggregators and concatenates their results.
// URNs across all sub-aggregations must be unique.
type CompositeAggregation struct {
	aggregators []Aggregator
}

func NewCompositeAggregation(aggregators ...Aggregator) *CompositeAggregation {
	return &CompositeAggregation{aggregators: aggregators}
}

func (c *CompositeAggregation) Execute(ctx context.Context, from, to time.Time) (Result, error) {
	// EAGER: execute all sub-aggregators, collect results and metadata
	subResults := make([]Result, len(c.aggregators))
	var allMeta []fieldMetaWithSource
	for i, agg := range c.aggregators {
		r, err := agg.Execute(ctx, from, to)
		if err != nil {
			return Result{}, fmt.Errorf("composite aggregation[%d]: %w", i, err)
		}
		subResults[i] = r
		for _, fm := range r.FieldsMeta() {
			allMeta = append(allMeta, fieldMetaWithSource{meta: fm, aggIndex: i})
		}
	}

	// Validate URN uniqueness across all sub-aggregations (fail fast)
	urnSet := make(map[string]bool, len(allMeta))
	for _, fms := range allMeta {
		urn := fms.meta.Urn()
		if urnSet[urn] {
			return Result{}, fmt.Errorf("composite aggregation: duplicate URN %q across sub-aggregations (aggregation %d)", urn, fms.aggIndex)
		}
		urnSet[urn] = true
	}

	// Collect flat metadata slice
	resultFieldsMeta := make([]tsquery.FieldMeta, 0, len(allMeta))
	for _, fms := range allMeta {
		resultFieldsMeta = append(resultFieldsMeta, fms.meta)
	}

	// LAZY: resolve all sub-results and concatenate
	lazyFields := lazy.NewLazy(func(ctx context.Context) ([]AggregatedValue, error) {
		var all []AggregatedValue
		for i, sub := range subResults {
			fields, err := sub.Fields().Get(ctx)
			if err != nil {
				return nil, fmt.Errorf("composite aggregation[%d]: failed to get fields: %w", i, err)
			}
			all = append(all, fields...)
		}
		return all, nil
	})

	return NewResult(resultFieldsMeta, lazyFields), nil
}

type fieldMetaWithSource struct {
	meta     tsquery.FieldMeta
	aggIndex int
}
