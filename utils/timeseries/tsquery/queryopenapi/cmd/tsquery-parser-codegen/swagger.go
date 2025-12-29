//go:build !ignore

package main

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/yaml.v3"
)

// Base discriminated types that we manage (hardcoded - these are the types we control)
var baseDiscriminatedTypes = []string{
	"ApiQueryDatasource",
	"ApiMultiDatasource",
	"ApiQueryFilter",
	"ApiQueryFieldValue",
	"ApiReportFieldValue",
	"ApiReportDatasource",
	"ApiReportMultiDatasource",
	"ApiReportFilter",
}

func processSwaggerFile(swaggerPath string) error {
	// Parse base swagger
	var baseSpec yaml.Node
	if err := yaml.Unmarshal([]byte(baseSwaggerFile), &baseSpec); err != nil {
		return fmt.Errorf("failed to parse base swagger: %w", err)
	}

	// Get the schemas node from base spec
	baseSchemas := findSchemasNode(&baseSpec)
	if baseSchemas == nil {
		return fmt.Errorf("base swagger missing components.schemas")
	}

	// Extract built-in discriminators from base swagger
	fmt.Println("  Extracting built-in discriminators from base swagger...")
	builtInDiscriminators, err := extractBuiltInDiscriminators(baseSchemas)
	if err != nil {
		return fmt.Errorf("failed to extract built-in discriminators: %w", err)
	}

	// Read and parse user swagger (if exists)
	var userSpec yaml.Node
	userSwaggerData, err := os.ReadFile(swaggerPath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to read swagger file: %w", err)
		}
		// File doesn't exist, start with base
		fmt.Println("  Creating new swagger file from base")
		userSpec = baseSpec
	} else {
		if err := yaml.Unmarshal(userSwaggerData, &userSpec); err != nil {
			return fmt.Errorf("failed to parse user swagger: %w", err)
		}
		fmt.Println("  Merging with existing swagger file")
	}

	// Get the schemas node from user spec
	userSchemas := findSchemasNode(&userSpec)
	if userSchemas == nil {
		return fmt.Errorf("user swagger missing components.schemas")
	}

	// First, save custom discriminators from user schemas before we overwrite
	fmt.Println("  Saving custom discriminators...")
	savedCustomDiscriminators := make(map[string]map[string]string)
	for _, typeName := range baseDiscriminatedTypes {
		userType := findSchemaByName(userSchemas, typeName)
		if userType != nil {
			customDiscs := extractCustomDiscriminators(typeName, userType, builtInDiscriminators)
			if len(customDiscs) > 0 {
				fmt.Printf("    Found %d custom discriminators in %s\n", len(customDiscs), typeName)
				savedCustomDiscriminators[typeName] = customDiscs
			}
		}
	}

	// Copy ALL base schemas (except path/command args)
	fmt.Println("  Copying all base schemas...")
	if err := copyAllBaseSchemas(baseSchemas, userSchemas); err != nil {
		return fmt.Errorf("failed to copy base schemas: %w", err)
	}

	// Then merge back the custom discriminators
	for _, typeName := range baseDiscriminatedTypes {
		customDiscs, hasCustom := savedCustomDiscriminators[typeName]
		if hasCustom {
			fmt.Printf("  Merging custom discriminators for %s\n", typeName)
			if err := mergeCustomDiscriminators(typeName, userSchemas, customDiscs, builtInDiscriminators); err != nil {
				return fmt.Errorf("failed to merge custom discriminators for %s: %w", typeName, err)
			}
		}
	}

	// Write back the merged swagger
	output, err := yaml.Marshal(&userSpec)
	if err != nil {
		return fmt.Errorf("failed to marshal swagger: %w", err)
	}

	if err := os.WriteFile(swaggerPath, output, 0644); err != nil {
		return fmt.Errorf("failed to write swagger file: %w", err)
	}

	fmt.Println("  ✓ Swagger file updated successfully")
	return nil
}

func findSchemasNode(root *yaml.Node) *yaml.Node {
	// Navigate to components.schemas
	if root.Kind != yaml.DocumentNode || len(root.Content) == 0 {
		return nil
	}

	doc := root.Content[0]
	if doc.Kind != yaml.MappingNode {
		return nil
	}

	// Find "components" key
	for i := 0; i < len(doc.Content); i += 2 {
		if doc.Content[i].Value == "components" {
			components := doc.Content[i+1]
			if components.Kind != yaml.MappingNode {
				return nil
			}

			// Find "schemas" key
			for j := 0; j < len(components.Content); j += 2 {
				if components.Content[j].Value == "schemas" {
					return components.Content[j+1]
				}
			}
		}
	}
	return nil
}

// extractBuiltInDiscriminators reads discriminators from the base swagger schemas
func extractBuiltInDiscriminators(baseSchemas *yaml.Node) (map[string]map[string]string, error) {
	builtIns := make(map[string]map[string]string)

	for _, typeName := range baseDiscriminatedTypes {
		typeNode := findSchemaByName(baseSchemas, typeName)
		if typeNode == nil {
			return nil, fmt.Errorf("base discriminated type %s not found in base swagger", typeName)
		}

		// Find discriminator
		discriminator := findMapKey(typeNode, "discriminator")
		if discriminator == nil {
			return nil, fmt.Errorf("type %s missing discriminator in base swagger", typeName)
		}

		// Find mapping
		mapping := findMapKey(discriminator, "mapping")
		if mapping == nil || mapping.Kind != yaml.MappingNode {
			return nil, fmt.Errorf("type %s missing discriminator mapping in base swagger", typeName)
		}

		// Extract all mappings
		typeDiscriminators := make(map[string]string)
		for i := 0; i < len(mapping.Content); i += 2 {
			key := mapping.Content[i].Value
			value := mapping.Content[i+1].Value
			typeDiscriminators[key] = value
		}

		builtIns[typeName] = typeDiscriminators
		fmt.Printf("    Extracted %d built-in discriminators for %s\n", len(typeDiscriminators), typeName)
	}

	return builtIns, nil
}

// Schemas that are NOT part of the base query spec (only path/command args)
// Note: ApiMeasurementValue IS included because it's used by query types like ApiStaticQueryDatasource
var excludedSchemas = map[string]bool{
	"ApiExecuteQueryCommandArgs": true,
}

func copyAllBaseSchemas(baseSchemas, userSchemas *yaml.Node) error {
	if baseSchemas.Kind != yaml.MappingNode {
		return fmt.Errorf("base schemas is not a mapping")
	}

	// Iterate through all base schemas
	for i := 0; i < len(baseSchemas.Content); i += 2 {
		schemaName := baseSchemas.Content[i].Value
		schemaValue := baseSchemas.Content[i+1]

		// Skip excluded schemas (path/command args)
		if excludedSchemas[schemaName] {
			fmt.Printf("    Skipping %s (not part of query spec)\n", schemaName)
			continue
		}

		// Check if it exists in user schemas
		existingIdx := findSchemaIndex(userSchemas, schemaName)
		if existingIdx >= 0 {
			// Replace existing schema
			fmt.Printf("    Updating %s\n", schemaName)
			userSchemas.Content[existingIdx].HeadComment = "Generated by tsquery-parser-codegen - DO NOT EDIT"
			userSchemas.Content[existingIdx+1] = cloneNode(schemaValue)
		} else {
			// Add new schema
			fmt.Printf("    Adding %s\n", schemaName)
			addSchemaWithComment(userSchemas, schemaName, schemaValue, true)
		}
	}

	return nil
}

func findSchemaIndex(schemas *yaml.Node, name string) int {
	if schemas.Kind != yaml.MappingNode {
		return -1
	}

	for i := 0; i < len(schemas.Content); i += 2 {
		if schemas.Content[i].Value == name {
			return i
		}
	}
	return -1
}

// extractCustomDiscriminators extracts custom (non-built-in) discriminators from a type
func extractCustomDiscriminators(typeName string, typeNode *yaml.Node, builtInDiscriminators map[string]map[string]string) map[string]string {
	customDiscs := make(map[string]string)

	// Get built-ins for this type
	builtIns := builtInDiscriminators[typeName]

	// Find discriminator node
	discriminator := findMapKey(typeNode, "discriminator")
	if discriminator == nil {
		return customDiscs
	}

	// Find mapping
	mapping := findMapKey(discriminator, "mapping")
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return customDiscs
	}

	// Extract custom ones
	for i := 0; i < len(mapping.Content); i += 2 {
		key := mapping.Content[i].Value
		value := mapping.Content[i+1].Value

		if _, isBuiltIn := builtIns[key]; !isBuiltIn {
			customDiscs[key] = value
		}
	}

	return customDiscs
}

// mergeCustomDiscriminators merges custom discriminators back into the type
func mergeCustomDiscriminators(typeName string, userSchemas *yaml.Node, customDiscs map[string]string, builtInDiscriminators map[string]map[string]string) error {
	userType := findSchemaByName(userSchemas, typeName)
	if userType == nil {
		return fmt.Errorf("type %s not found", typeName)
	}

	// Get built-ins
	builtIns := builtInDiscriminators[typeName]

	// Find discriminator and mapping
	discriminator := findMapKey(userType, "discriminator")
	if discriminator == nil {
		return fmt.Errorf("discriminator not found in %s", typeName)
	}

	mapping := findMapKey(discriminator, "mapping")
	if mapping == nil {
		return fmt.Errorf("mapping not found in %s discriminator", typeName)
	}

	// Build merged mapping with all refs
	allMappings := make(map[string]string)

	// Add built-ins first
	for key, ref := range builtIns {
		allMappings[key] = ref
	}

	// Add custom ones
	for key, ref := range customDiscs {
		allMappings[key] = ref
	}

	// Rebuild mapping node
	newMapping := &yaml.Node{
		Kind:    yaml.MappingNode,
		Content: make([]*yaml.Node, 0),
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(allMappings))
	for k := range allMappings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Add all mappings
	for _, key := range keys {
		ref := allMappings[key]

		keyNode := &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: key,
		}

		// Mark built-ins
		if _, isBuiltIn := builtIns[key]; isBuiltIn {
			keyNode.LineComment = "# Managed by tsquery-parser-codegen"
		}

		valueNode := &yaml.Node{
			Kind:  yaml.ScalarNode,
			Value: ref,
		}

		newMapping.Content = append(newMapping.Content, keyNode, valueNode)
	}

	// Replace mapping
	replaceMapValue(discriminator, "mapping", newMapping)

	// Update oneOf
	return updateOneOfWithCustom(typeName, userType, allMappings, builtInDiscriminators)
}

// updateOneOfWithCustom updates the oneOf array with all mappings
func updateOneOfWithCustom(typeName string, userType *yaml.Node, allMappings map[string]string, builtInDiscriminators map[string]map[string]string) error {
	builtIns := builtInDiscriminators[typeName]

	oneOfNode := &yaml.Node{
		Kind:    yaml.SequenceNode,
		Content: make([]*yaml.Node, 0),
	}

	// Sort keys for consistent output
	keys := make([]string, 0, len(allMappings))
	for k := range allMappings {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, key := range keys {
		ref := allMappings[key]

		refItem := &yaml.Node{
			Kind: yaml.MappingNode,
			Content: []*yaml.Node{
				{
					Kind:  yaml.ScalarNode,
					Value: "$ref",
				},
				{
					Kind:  yaml.ScalarNode,
					Value: ref,
				},
			},
		}

		// Mark built-ins
		if _, isBuiltIn := builtIns[key]; isBuiltIn {
			refItem.HeadComment = "Managed by tsquery-parser-codegen"
		}

		oneOfNode.Content = append(oneOfNode.Content, refItem)
	}

	replaceMapValue(userType, "oneOf", oneOfNode)
	return nil
}

func findSchemaByName(schemas *yaml.Node, name string) *yaml.Node {
	if schemas.Kind != yaml.MappingNode {
		return nil
	}

	for i := 0; i < len(schemas.Content); i += 2 {
		if schemas.Content[i].Value == name {
			return schemas.Content[i+1]
		}
	}
	return nil
}

func addSchemaWithComment(schemas *yaml.Node, name string, schema *yaml.Node, isManaged bool) {
	if schemas.Kind != yaml.MappingNode {
		return
	}

	// Create key node with comment
	keyNode := &yaml.Node{
		Kind:  yaml.ScalarNode,
		Value: name,
	}
	if isManaged {
		keyNode.HeadComment = "Generated by tsquery-parser-codegen - DO NOT EDIT"
	}

	// Clone the schema node
	schemaClone := cloneNode(schema)

	// Add to schemas
	schemas.Content = append(schemas.Content, keyNode, schemaClone)
}

func findMapKey(node *yaml.Node, key string) *yaml.Node {
	if node.Kind != yaml.MappingNode {
		return nil
	}

	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func replaceMapValue(node *yaml.Node, key string, newValue *yaml.Node) {
	if node.Kind != yaml.MappingNode {
		return
	}

	for i := 0; i < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			node.Content[i+1] = newValue
			return
		}
	}
}

func cloneNode(node *yaml.Node) *yaml.Node {
	if node == nil {
		return nil
	}

	clone := &yaml.Node{
		Kind:        node.Kind,
		Style:       node.Style,
		Tag:         node.Tag,
		Value:       node.Value,
		Anchor:      node.Anchor,
		Alias:       node.Alias,
		Content:     make([]*yaml.Node, len(node.Content)),
		HeadComment: node.HeadComment,
		LineComment: node.LineComment,
		FootComment: node.FootComment,
		Line:        node.Line,
		Column:      node.Column,
	}

	for i, child := range node.Content {
		clone.Content[i] = cloneNode(child)
	}

	return clone
}
