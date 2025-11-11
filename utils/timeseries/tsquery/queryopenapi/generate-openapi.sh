#!/usr/bin/env bash

# Ensure GOPATH/bin is in PATH for oapi-codegen
export PATH="$(go env GOPATH)/bin:$PATH"

rm -rf *.gen.go
oapi-codegen -config oapi-conf.yaml -package queryopenapi tsquery-swagger.yaml > ./openapi_generated.gen.go

