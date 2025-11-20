# Time Series Query Parser Codegen Tool

A command-line tool that generates parser code for extended OpenAPI specifications.

## What It Does

This tool intelligently merges your custom OpenAPI spec extensions with the base specification and generates parser code. It:
1. Automatically ensures all base types and discriminators are present in your swagger file
2. Preserves your custom datasources/filters/fields while adding/updating built-in types
3. Generates parser files that work with your extended types
4. Allows you to implement only the custom parsing logic via `PluginApiParser`

The tool is idempotent - you can run it multiple times safely as it preserves custom extensions while keeping built-in types up to date.

## Installation

```bash
go install github.com/shpandrak/shpanstream/utils/timeseries/tsquery/queryopenapi/cmd/tsquery-parser-codegen@latest
```

## Usage

```bash
tsquery-parser-codegen --swagger-file your-swagger.yaml --target-package mypkg --target-dir ./mypkg
```

### Arguments

- `--swagger-file` (required): Path to your swagger file (will be created/updated)
- `--target-package` (required): Name of the target package (e.g., `mypkg` or `github.com/user/repo/mypkg`)
- `--target-dir` (required): Directory where parser files will be generated

## Example: Extending with Postgres Datasource

### 1. Create your swagger file with custom types

Create `my-swagger.yaml` with just your custom types (you don't need to copy the entire base spec):

```yaml
openapi: 3.2.0
info:
  title: My Extended Time Series Query API
  version: "0.1"

components:
  schemas:
    # Add your custom datasource
    ApiPostgresQueryDatasource:
      type: object
      required: [type, connectionString, tableName]
      properties:
        type:
          type: string
          enum: [postgres]
        connectionString:
          type: string
        tableName:
          type: string
```

### 2. Run the codegen tool

The tool will automatically:
- Add all base types if missing
- Add your custom `postgres` discriminator to `ApiQueryDatasource`
- Mark built-in types with comments so you know not to edit them

```bash
tsquery-parser-codegen --swagger-file my-swagger.yaml --target-package mypkg --target-dir .
```

After running, your swagger will have:
```yaml
ApiQueryDatasource:
  discriminator:
    mapping:
      filtered: '#/components/schemas/ApiFilteredQueryDatasource' # Managed by tsquery-parser-codegen
      reduction: '#/components/schemas/ApiReductionQueryDatasource' # Managed by tsquery-parser-codegen
      static: '#/components/schemas/ApiStaticQueryDatasource' # Managed by tsquery-parser-codegen
      postgres: '#/components/schemas/ApiPostgresQueryDatasource'  # Your custom type!
```

### 3. Generate OpenAPI types

```bash
oapi-codegen -config oapi-conf.yaml -package mypkg my-swagger.yaml > openapi_generated.gen.go
```

### 4. Implement custom parser

Create `custom_parser.go`:

```go
package mypkg

import "github.com/shpandrak/shpanstream/utils/timeseries/tsquery/datasource"

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
        return nil, fmt.Errorf("unsupported custom datasource")
    }
}

// Implement other PluginApiParser methods...
```

### 5. Use the parser

```go
pCtx := NewParsingContext(context.Background(), CustomParser{})
datasource, err := ParseDatasource(pCtx, apiDatasource)
```

## How It Works

The tool:
1. Contains embedded copies of all base parser files (`openapi_parser*.go`)
2. Transforms package declarations and imports to match your target package
3. Generates the files in your target directory

## Development

### For External Users

Just install the tool - no building needed:

```bash
go install github.com/shpandrak/shpanstream/utils/timeseries/tsquery/queryopenapi/cmd/tsquery-parser-codegen@latest
```

### For Developers

If you're modifying the parser files in the parent directory (`../../openapi_parser*.go`), run:

```bash
# From the cmd/tsquery-parser-codegen directory
./build.sh
```

This script will:
1. Copy the latest parser files from `../../openapi_parser*.go`
2. Add `//go:build ignore` tags if needed (for embedding)
3. Copy the latest base swagger from `../../tsquery-swagger.yaml`
4. Rebuild and install the tool

**Important:** The copied files with build tags ARE committed to the repo so that `go install` works for external users.

### Manual Build

If you prefer to build manually:

```bash
# Copy files
cp ../../openapi_parser*.go .
cp ../../tsquery-swagger.yaml .

# Build and install
go install
```

## See Also

- [Parent README](../../README.md) - Full documentation
- [Test Example](../../codegen/testdata/) - Complete working example
