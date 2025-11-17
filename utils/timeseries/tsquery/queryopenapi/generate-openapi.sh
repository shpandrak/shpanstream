#!/usr/bin/env bash

# Ensure GOPATH/bin is in PATH for oapi-codegen
export PATH="$(go env GOPATH)/bin:$PATH"

# Generate OpenAPI code
rm -rf *.gen.go
oapi-codegen -config oapi-conf.yaml -package queryopenapi tsquery-swagger.yaml > ./openapi_generated.gen.go

# Post-process: Remove Merge functions and runtime import
echo "Post-processing generated code..."
go run tools/cleanup-generated.go

