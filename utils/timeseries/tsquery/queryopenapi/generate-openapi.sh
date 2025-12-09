#!/usr/bin/env bash

OAPI_CODEGEN="go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.5.0"

# Generate OpenAPI code
rm -rf *.gen.go
$OAPI_CODEGEN -config oapi-conf.yaml -package queryopenapi tsquery-swagger.yaml > ./openapi_generated.gen.go

# Post-process: Remove Merge functions and runtime import
echo "Post-processing generated code..."
go run tools/cleanup-generated.go

