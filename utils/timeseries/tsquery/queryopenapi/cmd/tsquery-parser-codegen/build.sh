#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"

echo "Copying and preparing parser files from parent directory..."

# Copy each parser file and add the build ignore tag
for file in ../../openapi_parser*.go; do
    filename=$(basename "$file")
    echo "  Processing $filename..."

    # Check if file already has the build tag
    if head -1 "$file" | grep -q "//go:build"; then
        # Already has build tag, just copy
        cp "$file" .
    else
        # Add build tag
        echo "//go:build ignore" > "$filename"
        echo "" >> "$filename"
        cat "$file" >> "$filename"
    fi
done

echo "Copying base swagger file..."
cp ../../tsquery-swagger.yaml .

echo "Building and installing tool..."
go install

echo "✓ Tool built and installed successfully"
echo ""
echo "Embedded files:"
ls -1 openapi_parser*.go tsquery-swagger.yaml
