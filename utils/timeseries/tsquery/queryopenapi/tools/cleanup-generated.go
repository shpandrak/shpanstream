package main

import (
	"fmt"
	"os"
	"regexp"
	"strings"
)

func main() {
	filename := "openapi_generated.gen.go"

	// Read the file
	content, err := os.ReadFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading file: %v\n", err)
		os.Exit(1)
	}

	// Remove Merge functions
	cleaned := removeMergeFunctions(string(content))

	// Remove runtime import if no longer used
	cleaned = removeRuntimeImport(cleaned)

	// Write back
	err = os.WriteFile(filename, []byte(cleaned), 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing file: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✓ Cleaned up", filename)
	fmt.Println("  - Removed Merge* functions containing runtime.JSONMerge")
	fmt.Println("  - Removed runtime import if unused")
}

// removeMergeFunctions removes all Merge* methods that contain runtime.JSONMerge
func removeMergeFunctions(content string) string {
	lines := strings.Split(content, "\n")
	var output []string
	var functionBuffer []string
	inMergeFunction := false
	var commentLineIdx int = -1

	mergeFuncPattern := regexp.MustCompile(`^func \(t \*\w+\) Merge\w+\(.*\) error \{`)

	for i := 0; i < len(lines); i++ {
		line := lines[i]

		// Check if this is a Merge function definition
		if mergeFuncPattern.MatchString(line) {
			// Start buffering to check if it contains runtime.JSONMerge
			inMergeFunction = true
			functionBuffer = []string{line}

			// Check if previous line is a Merge comment
			if len(output) > 0 && strings.HasPrefix(strings.TrimSpace(output[len(output)-1]), "// Merge") {
				commentLineIdx = len(output) - 1
			} else {
				commentLineIdx = -1
			}
			continue
		}

		// If we're buffering a merge function
		if inMergeFunction {
			functionBuffer = append(functionBuffer, line)

			// Check if we've reached the end of the function (closing brace not in a comment)
			if strings.HasPrefix(line, "}") && !strings.HasPrefix(strings.TrimSpace(line), "//") {
				// Check if buffer contains runtime.JSONMerge
				bufferText := strings.Join(functionBuffer, "\n")
				if strings.Contains(bufferText, "runtime.JSONMerge") {
					// This is a Merge function we want to remove
					// Remove the comment line if we found one
					if commentLineIdx >= 0 {
						output = output[:commentLineIdx]
					}
					// Don't add the buffered function to output
				} else {
					// Keep this function, it's not a Merge function we want to remove
					output = append(output, functionBuffer...)
				}

				// Reset state
				functionBuffer = nil
				inMergeFunction = false
				commentLineIdx = -1
				continue
			}
			continue
		}

		// Normal line, add to output
		output = append(output, line)
	}

	return strings.Join(output, "\n")
}

// removeRuntimeImport removes the oapi-codegen/runtime import if it's no longer used
func removeRuntimeImport(content string) string {
	// Check if runtime is still used
	if !strings.Contains(content, "runtime.") {
		// Remove the import line using regex
		runtimeImportPattern := regexp.MustCompile(`(?m)^\s*"github\.com/oapi-codegen/runtime"\n`)
		content = runtimeImportPattern.ReplaceAllString(content, "")
	}

	return content
}
