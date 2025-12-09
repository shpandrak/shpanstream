#!/usr/bin/env bash

set -e

OAPI_CODEGEN="go run github.com/oapi-codegen/oapi-codegen/v2/cmd/oapi-codegen@v2.5.0"

# Ensure GOPATH/bin is in PATH for tsquery-parser-codegen
export PATH="$(go env GOPATH)/bin:$PATH"

# Update swagger and generate parser files using codegen tool
echo "Updating swagger and generating parser files..."
tsquery-parser-codegen --swagger-file extended-swagger.yaml --target-package testdata --target-dir .

# Generate OpenAPI code from updated swagger
echo "Generating OpenAPI code from extended-swagger.yaml..."
rm -f openapi_generated.gen.go
$OAPI_CODEGEN -config oapi-conf.yaml -package testdata extended-swagger.yaml > ./openapi_generated.gen.go

echo "✓ Generation complete"
