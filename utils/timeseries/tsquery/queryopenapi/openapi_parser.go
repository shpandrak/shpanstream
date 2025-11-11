package queryopenapi

import (
	"fmt"
	"github.com/shpandrak/shpanstream/stream"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/filter"
)

func ParseFilteredDatasource(
	filteredDs ApiFilteredQueryDatasource,
	rawDsProvider func(rawDsArgs ApiRawQueryDatasource) (datasource.DataSource, error),
) (datasource.DataSource, error) {
	unfilteredDatasource, err := ParseDatasource(filteredDs.Datasource, rawDsProvider)
	if err != nil {
		return nil, err
	}
	if len(filteredDs.Filters) == 0 {
		return unfilteredDatasource, nil
	}
	var parsedFilters []filter.Filter
	for _, rawFilter := range filteredDs.Filters {
		parsedFilter, err := ParseFilter(rawFilter)
		if err != nil {
			return nil, err
		}
		parsedFilters = append(parsedFilters, parsedFilter)
	}
	return filter.NewFilteredDataSource(unfilteredDatasource, parsedFilters...), nil

}

func ParseDatasource(ds ApiQueryDatasource, rawDsProvider func(rawDsArgs ApiRawQueryDatasource) (datasource.DataSource, error)) (datasource.DataSource, error) {
	rawDs, err := ds.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedDs := rawDs.(type) {
	case ApiRawQueryDatasource:
		// Directly use the raw datasource provider
		return rawDsProvider(typedDs)
	case ApiJoinQueryDatasource:
		return parseJoinDatasource(typedDs, rawDsProvider)
	default:
		return nil, nil
	}
}

func parseJoinDatasource(ds ApiJoinQueryDatasource, provider func(rawDsArgs ApiRawQueryDatasource) (datasource.DataSource, error)) (datasource.DataSource, error) {
	var datasources []datasource.DataSource
	for _, joinedDs := range ds.Datasources {
		parsedDs, err := ParseFilteredDatasource(joinedDs, provider)
		if err != nil {
			return nil, err
		}
		datasources = append(datasources, parsedDs)
	}
	if len(datasources) == 0 {
		return nil, fmt.Errorf("no datasources provided for join datasource")
	} else if len(datasources) == 1 {
		return datasources[0], nil
	}
	var joinType datasource.JoinType
	switch ds.JoinType {
	case Inner:
		joinType = datasource.InnerJoin
	case Left:
		joinType = datasource.LeftJoin
	case Full:
		joinType = datasource.FullJoin
	default:
		return nil, fmt.Errorf("unsupported join type %v", ds.JoinType)
	}
	return datasource.NewJoinDatasource(stream.FromSlice(datasources), joinType), nil

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
