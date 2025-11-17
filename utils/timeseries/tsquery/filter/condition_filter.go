package filter

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/internal/util"
	"github.com/shpandrak/shpanstream/utils/timeseries"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/field"
)

var _ Filter = ConditionFilter{}

type ConditionFilter struct {
	booleanField field.Value
}

func NewConditionFilter(booleanField field.Value) ConditionFilter {
	return ConditionFilter{booleanField: booleanField}
}

func (cf ConditionFilter) Filter(result tsquery.Result) (tsquery.Result, error) {
	fieldsMeta := result.FieldsMeta()

	// Execute the boolean field to get metadata and value supplier
	fieldMeta, valueSupplier, err := cf.booleanField.Execute(fieldsMeta)
	if err != nil {
		return util.DefaultValue[tsquery.Result](), fmt.Errorf("failed to execute boolean field for condition filter: %w", err)
	}

	// Validate that the field is of a boolean type
	if fieldMeta.DataType != tsquery.DataTypeBoolean {
		return util.DefaultValue[tsquery.Result](), fmt.Errorf(
			"condition filter requires a boolean field, got %s",
			fieldMeta.DataType,
		)
	}

	// Field must be required (non-optional)
	if !fieldMeta.Required {
		return util.DefaultValue[tsquery.Result](), fmt.Errorf(
			"condition filter requires a required (non-optional) boolean field, got optional",
		)
	}

	// Filter the stream to keep only rows where the condition is true
	filteredStream := result.Stream().FilterWithErAndCtx(
		func(ctx context.Context, record timeseries.TsRecord[[]any]) (bool, error) {
			conditionValue, err := valueSupplier(ctx, record)
			if err != nil {
				return false, fmt.Errorf("failed to evaluate condition for filter: %w", err)
			}

			// Cast to bool and return
			return conditionValue.(bool), nil
		})

	return tsquery.NewResult(fieldsMeta, filteredStream), nil
}
