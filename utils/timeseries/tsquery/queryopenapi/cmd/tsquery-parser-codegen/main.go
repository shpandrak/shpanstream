package main

import (
	_ "embed"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
)

const (
	sourcePackage = "queryopenapi"
)

// Embedded parser source files (symlinked from parent directory)
//go:embed openapi_parser.go
var parserFile string

//go:embed openapi_parser_datasource.go
var parserDatasourceFile string

//go:embed openapi_parser_filter.go
var parserFilterFile string

//go:embed openapi_parser_field.go
var parserFieldFile string

//go:embed openapi_parser_report_field.go
var parserReportFieldFile string

//go:embed openapi_parser_report_datasource.go
var parserReportDatasourceFile string

//go:embed report_field_refs.go
var reportFieldRefsFile string

//go:embed report_field_ordering.go
var reportFieldOrderingFile string

//go:embed report_field_ordering_test.go
var reportFieldOrderingTestFile string

//go:embed openapi_parser_test.go
var parserTestFile string

//go:embed tsquery-swagger.yaml
var baseSwaggerFile string

var embeddedFiles = map[string]string{
	"openapi_parser.go":                    parserFile,
	"openapi_parser_datasource.go":         parserDatasourceFile,
	"openapi_parser_filter.go":             parserFilterFile,
	"openapi_parser_field.go":              parserFieldFile,
	"openapi_parser_report_field.go":       parserReportFieldFile,
	"openapi_parser_report_datasource.go":  parserReportDatasourceFile,
	"report_field_refs.go":                 reportFieldRefsFile,
	"report_field_ordering.go":             reportFieldOrderingFile,
	"report_field_ordering_test.go":        reportFieldOrderingTestFile,
	"openapi_parser_test.go":               parserTestFile,
}

type Config struct {
	TargetPackage string
	TargetDir     string
	SwaggerFile   string
}

func main() {
	config := parseFlags()

	if err := run(config); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() Config {
	var config Config

	flag.StringVar(&config.TargetPackage, "target-package", "", "Target package name (required)")
	flag.StringVar(&config.TargetDir, "target-dir", "", "Target directory path (required)")
	flag.StringVar(&config.SwaggerFile, "swagger-file", "", "Path to swagger file to update/generate (required)")
	flag.Parse()

	if config.TargetPackage == "" {
		fmt.Fprintln(os.Stderr, "Error: --target-package is required")
		flag.Usage()
		os.Exit(1)
	}

	if config.TargetDir == "" {
		fmt.Fprintln(os.Stderr, "Error: --target-dir is required")
		flag.Usage()
		os.Exit(1)
	}

	if config.SwaggerFile == "" {
		fmt.Fprintln(os.Stderr, "Error: --swagger-file is required")
		flag.Usage()
		os.Exit(1)
	}

	return config
}

func run(config Config) error {
	// Ensure target directory exists
	if err := os.MkdirAll(config.TargetDir, 0755); err != nil {
		return fmt.Errorf("failed to create target directory: %w", err)
	}

	// Process swagger file first
	fmt.Println("Processing swagger file...")
	if err := processSwaggerFile(config.SwaggerFile); err != nil {
		return fmt.Errorf("failed to process swagger file: %w", err)
	}

	// Extract just the package name from the full path (last component)
	targetPackageName := filepath.Base(config.TargetPackage)

	// Process each embedded file
	for filename, content := range embeddedFiles {
		targetPath := filepath.Join(config.TargetDir, filename)

		fmt.Printf("Generating %s\n", filename)

		// Transform the content
		transformed := transformFile(content, targetPackageName, config.TargetPackage)

		// Write to target
		if err := os.WriteFile(targetPath, []byte(transformed), 0644); err != nil {
			return fmt.Errorf("failed to write %s: %w", targetPath, err)
		}
	}

	fmt.Println("✓ Successfully generated parser files")
	return nil
}

func transformFile(content, targetPackageName, targetPackagePath string) string {
	// 1. Remove build ignore directive (needed for embedding but not for generated files)
	buildIgnoreRegex := regexp.MustCompile(`(?m)^//go:build ignore\n\n`)
	content = buildIgnoreRegex.ReplaceAllString(content, "")

	// 2. Replace package declaration
	packageRegex := regexp.MustCompile(`^package\s+` + sourcePackage)
	content = packageRegex.ReplaceAllString(content, "package "+targetPackageName)

	// 3. Update imports for Api* structs
	// Replace import of queryopenapi package with target package
	importRegex := regexp.MustCompile(`"github\.com/shpandrak/shpanstream/utils/timeseries/tsquery/queryopenapi"`)
	content = importRegex.ReplaceAllString(content, `"`+targetPackagePath+`"`)

	// 4. Handle qualified references to the original package (e.g., queryopenapi.Api*)
	// These should be updated to use the target package name
	qualifiedRegex := regexp.MustCompile(`\b` + sourcePackage + `\.`)
	content = qualifiedRegex.ReplaceAllString(content, targetPackageName+".")

	return content
}
