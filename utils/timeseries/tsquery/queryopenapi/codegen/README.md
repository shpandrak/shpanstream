# OpenAPI Parser Codegen

This directory contains tools and examples for extending the Time Series Query OpenAPI specification with custom datasources, filters, and field types.

## Quick Start

Install the codegen tool:

```bash
go install github.com/shpandrak/shpanstream/utils/timeseries/tsquery/queryopenapi/cmd/tsquery-parser-codegen@latest
```

See the [tool README](../cmd/tsquery-parser-codegen/README.md) for usage instructions.

## Problem Statement

The OpenAPI specification uses discriminators for polymorphic types (datasources, filters, fields). However, OpenAPI `$ref` doesn't allow discriminator extension across different specifications. This creates a challenge when users want to add custom types (e.g., database-specific datasources like Postgres, ClickHouse, etc.).

## Solution

The codegen tool intelligently merges your custom extensions with the base specification:
1. Automatically ensures all base types and discriminators are present
2. Preserves your custom discriminators while managing built-in ones
3. Generates parser files that work seamlessly with your extended types
4. Allows you to implement only custom parsing logic via `PluginApiParser`

This approach ensures:
- Base parser logic is reused and tested
- Custom types integrate seamlessly with the base types
- Type-safe parsing with compile-time guarantees
- Idempotent operation - safe to run multiple times
- Clear marking of managed vs. custom types with comments

## Usage

### 1. Create Your Swagger File

Create a swagger file with your custom types (you don't need to copy the entire base spec):

```yaml
openapi: 3.2.0
info:
  title: My Extended Time Series Query API
  version: "0.1"

components:
  schemas:
    # Define your custom datasource
    ApiPostgresQueryDatasource:
      type: object
      required:
        - type
        - connectionString
        - tableName
      properties:
        type:
          type: string
          enum: [postgres]
        connectionString:
          type: string
        tableName:
          type: string
```

The codegen tool will automatically:
- Add all base types if missing
- Add your custom `postgres` discriminator to `ApiQueryDatasource`
- Merge your custom types with built-in types
- Mark managed types with comments

### 2. Set Up Your Package

Create a `go.mod` with a replace directive pointing to the shpanstream root:

```go
module your-module/queryopenapi

go 1.24.2

replace github.com/shpandrak/shpanstream => /path/to/shpanstream

require github.com/shpandrak/shpanstream v0.0.0
```

### 3. Generate Code

#### Option A: Using go:generate

Create a `generate.go` file:

```go
package yourpackage

//go:generate tsquery-parser-codegen --swagger-file your-swagger.yaml --target-package yourpackage --target-dir .
//go:generate oapi-codegen -config /path/to/oapi-conf.yaml -o openapi_generated.gen.go your-swagger.yaml
```

Then run:
```bash
go generate
```

#### Option B: Using Shell Script

Create a `generate.sh` script:

```bash
#!/usr/bin/env bash
set -e

# Update swagger and generate parser files
tsquery-parser-codegen --swagger-file your-swagger.yaml --target-package yourpackage --target-dir .

# Generate OpenAPI code
oapi-codegen -config /path/to/oapi-conf.yaml -o openapi_generated.gen.go your-swagger.yaml
```

### 4. Implement Custom Parser

Create a file implementing the `PluginApiParser` interface:

```go
package yourpackage

import (
	"github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"
)

type CustomParser struct{}

func (p CustomParser) ParseDatasource(pCtx *ParsingContext, ds ApiQueryDatasource) (datasource.DataSource, error) {
	rawDs, err := ds.ValueByDiscriminator()
	if err != nil {
		return nil, err
	}

	switch typedDs := rawDs.(type) {
	case ApiPostgresQueryDatasource:
		return parsePostgresDatasource(typedDs)
	default:
		return nil, fmt.Errorf("unsupported custom datasource type: %T", rawDs)
	}
}

func (p CustomParser) ParseFilter(pCtx *ParsingContext, filter ApiQueryFilter) (datasource.Filter, error) {
	return nil, fmt.Errorf("no custom filter types supported")
}

func (p CustomParser) ParseFieldValue(pCtx *ParsingContext, field ApiQueryFieldValue) (datasource.Value, error) {
	return nil, fmt.Errorf("no custom field value types supported")
}

func (p CustomParser) ParseMultiDatasource(pCtx *ParsingContext, multiDs ApiMultiDatasource) (datasource.MultiDataSource, error) {
	return nil, fmt.Errorf("no custom multi-datasource types supported")
}

func parsePostgresDatasource(ds ApiPostgresQueryDatasource) (datasource.DataSource, error) {
	// Your implementation here
}
```

### 5. Use the Parser

```go
pCtx := NewParsingContext(context.Background(), CustomParser{})
datasource, err := ParseDatasource(pCtx, apiDatasource)
```

## Files Generated

The codegen tool copies and transforms these files:
- `openapi_parser.go` - Core parsing logic and PluginApiParser interface
- `openapi_parser_datasource.go` - Datasource parsing
- `openapi_parser_filter.go` - Filter parsing
- `openapi_parser_field.go` - Field value parsing
- `openapi_parser_test.go` - Comprehensive test suite

## Transformations Applied

1. **Package Declaration**: `package queryopenapi` → `package yourpackage`
2. **Import Paths**: Updates imports of Api* structs to reference your package
3. **Qualified References**: Updates `queryopenapi.Api*` to `yourpackage.Api*`

## Example

See the [testdata](./testdata) directory for a complete working example that extends the spec with a Postgres datasource.

## Testing

The generated parser files include a comprehensive test suite that validates all base functionality. Add your own tests for custom types:

```go
func TestCustomParser_Postgres(t *testing.T) {
	var ds ApiQueryDatasource
	err := ds.FromApiPostgresQueryDatasource(ApiPostgresQueryDatasource{
		Type: "postgres",
		ConnectionString: "postgresql://localhost/test",
		TableName: "metrics",
	})
	require.NoError(t, err)

	pCtx := NewParsingContext(context.Background(), CustomParser{})
	result, err := ParseDatasource(pCtx, ds)
	require.NoError(t, err)
	require.NotNil(t, result)
}
```
