//go:build ignore

package queryopenapi

import (
	"fmt"
	"strings"
)

// ReportFieldForOrdering is a minimal struct for dependency ordering.
// Use this to pass field data to OrderReportFieldUrnsByDependency without
// requiring a specific ApiReportField type definition.
type ReportFieldForOrdering struct {
	Uri   string
	Value ApiReportFieldValue
}

// OrderReportFieldUrnsByDependency returns URNs in dependency order (dependencies first).
// Fields that don't depend on other fields in the list come first.
// External refs (URNs not in the input list) are silently ignored.
// Returns error on circular dependency.
func OrderReportFieldUrnsByDependency(fields []ReportFieldForOrdering) ([]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	// Build set of URNs we're ordering
	urnSet := make(map[string]bool, len(fields))
	for _, f := range fields {
		urnSet[f.Uri] = true
	}

	// Build adjacency list: for each URN, list of URNs it depends on (that are in our set)
	deps := make(map[string][]string, len(fields))
	// Build reverse adjacency: for each URN, list of URNs that depend on it
	dependents := make(map[string][]string, len(fields))
	// Track in-degree (number of dependencies in our set) for each URN
	inDegree := make(map[string]int, len(fields))

	for _, f := range fields {
		refs := f.Value.GetReferencedUrns()
		var relevantDeps []string
		for _, ref := range refs {
			// Only consider refs that are in our input set
			if urnSet[ref] && ref != f.Uri {
				relevantDeps = append(relevantDeps, ref)
				dependents[ref] = append(dependents[ref], f.Uri)
			}
		}
		deps[f.Uri] = relevantDeps
		inDegree[f.Uri] = len(relevantDeps)
	}

	// Kahn's algorithm for topological sort
	// Start with nodes that have no dependencies (in-degree = 0)
	var queue []string
	for _, f := range fields {
		if inDegree[f.Uri] == 0 {
			queue = append(queue, f.Uri)
		}
	}

	var result []string
	for len(queue) > 0 {
		// Take first element from queue
		curr := queue[0]
		queue = queue[1:]
		result = append(result, curr)

		// For each node that depends on curr, reduce in-degree
		for _, dependent := range dependents[curr] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	// If we didn't process all nodes, there's a cycle
	if len(result) != len(fields) {
		// Find nodes involved in cycles (those with remaining in-degree > 0)
		var cycleNodes []string
		for _, f := range fields {
			if inDegree[f.Uri] > 0 {
				cycleNodes = append(cycleNodes, f.Uri)
			}
		}
		return nil, fmt.Errorf("circular dependency detected involving: %s", strings.Join(cycleNodes, ", "))
	}

	return result, nil
}
