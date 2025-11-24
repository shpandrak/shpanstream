package datasource

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
)

var _ Filter = ConditionFilter{}

type ConditionFilter struct {
	booleanField Value
}

func NewConditionFilter(booleanField Value) ConditionFilter {
	return ConditionFilter{booleanField: booleanField}
}

func (cf ConditionFilter) Filter(ctx context.Context, result Result) (Result, error) {
	fieldMeta := result.Meta()

	// Execute the boolean field to get metadata and value supplier
	valueMeta, valueSupplier, err := cf.booleanField.Execute(ctx, fieldMeta)
	if err != nil {
		return util.DefaultValue[Result](), fmt.Errorf("failed to execute boolean field for condition filter: %w", err)
	}

	// Validate that the field is of a boolean type
	if valueMeta.DataType != tsquery.DataTypeBoolean {
		return util.DefaultValue[Result](), fmt.Errorf(
			"condition filter requires a boolean field, got %s",
			valueMeta.DataType,
		)
	}

	// Field must be required (non-optional)
	if !valueMeta.Required {
		return util.DefaultValue[Result](), fmt.Errorf(
			"condition filter requires a required (non-optional) boolean field, got optional",
		)
	}

	// Filter the stream to keep only rows where the condition is true
	filteredStream := result.Data().FilterWithErAndCtx(
		func(ctx context.Context, record timeseries.TsRecord[any]) (bool, error) {
			conditionValue, err := valueSupplier(ctx, record)
			if err != nil {
				return false, fmt.Errorf("failed to evaluate condition for filter: %w", err)
			}

			// Cast to bool and return
			return conditionValue.(bool), nil
		})

	return Result{
		meta: fieldMeta,
		data: filteredStream,
	}, nil
}
