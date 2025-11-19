package queryopenapi

import (
	"context"
	"fmt"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
)

type ParsingContext struct {
	context.Context
	PluginApiParser
}

func NewParsingContext(ctx context.Context, pp PluginApiParser) *ParsingContext {
	if pp == nil {
		pp = noPluginApiParser{}
	}
	return &ParsingContext{ctx, pp}
}

type invalidQueryError struct {
	err     error
	element any
}

func (iqe invalidQueryError) Error() string {
	return fmt.Sprintf("invalid query: %s\nfor element %v", iqe.err.Error(), iqe.element)
}

func badInputErrorf(entity any, format string, a ...any) invalidQueryError {

	return invalidQueryError{
		err:     fmt.Errorf(format, a...),
		element: entity,
	}
}

func badInputError(entity any, err error) invalidQueryError {

	return invalidQueryError{
		err:     err,
		element: entity,
	}
}
func badInputErrorWrap(entity any, err error, format string, a ...any) invalidQueryError {

	return invalidQueryError{
		err:     fmt.Errorf(fmt.Sprintf(format, a...)+": %w", err),
		element: entity,
	}
}

type PluginApiParser interface {
	ParseDatasource(pCtx *ParsingContext, queryDatasource ApiQueryDatasource) (datasource.DataSource, error)
	ParseMultiDatasource(pCtx *ParsingContext, multiDatasource ApiMultiDatasource) (datasource.MultiDataSource, error)
	ParseFilter(pCtx *ParsingContext, filter ApiQueryFilter) (datasource.Filter, error)
	ParseFieldValue(pCtx *ParsingContext, field ApiQueryFieldValue) (datasource.Value, error)
}

type noPluginApiParser struct {
}

func (n noPluginApiParser) ParseDatasource(_ *ParsingContext, queryDatasource ApiQueryDatasource) (datasource.DataSource, error) {
	discriminator, err := queryDatasource.Discriminator()
	if err != nil {
		return nil, fmt.Errorf("failed to get datasource discriminator: %w", err)
	}
	return nil, fmt.Errorf("unsupported datasource discriminator %s", discriminator)
}

func (n noPluginApiParser) ParseMultiDatasource(_ *ParsingContext, multiDatasource ApiMultiDatasource) (datasource.MultiDataSource, error) {
	discriminator, err := multiDatasource.Discriminator()
	if err != nil {
		return nil, fmt.Errorf("failed to get multi datasource discriminator: %w", err)
	}
	return nil, fmt.Errorf("unsupported multi datasource discriminator %s", discriminator)
}

func (n noPluginApiParser) ParseFilter(_ *ParsingContext, filter ApiQueryFilter) (datasource.Filter, error) {
	discriminator, err := filter.Discriminator()
	if err != nil {
		return nil, fmt.Errorf("failed to get filter discriminator: %w", err)
	}
	return nil, fmt.Errorf("unsupported filter discriminator %s", discriminator)

}

func (n noPluginApiParser) ParseFieldValue(_ *ParsingContext, field ApiQueryFieldValue) (datasource.Value, error) {
	discriminator, err := field.Discriminator()
	if err != nil {
		return nil, fmt.Errorf("failed to get field discriminator: %w", err)
	}
	return nil, fmt.Errorf("unsupported field discriminator %s", discriminator)
}
