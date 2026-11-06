package stream

import (
	"go/build"
	"path/filepath"
	"strings"
	"testing"
)

// TestStreamDoesNotImportLazy guards the decoupling of package stream from package lazy: every
// plain method of a generic type is compiled for every element type it is instantiated with, so a
// Stream API that mentions lazy.Lazy[T] in the wrong place makes every Stream[T] compile all of
// Lazy's methods (see docs/stream-api-generic-methods.md). It walks the imports of stream (including its in-package
// tests) and, transitively, of every package of this module that stream reaches. While package
// lazy imports stream (its bridges), such an import is already an import cycle the compiler
// rejects; this test is the guard that remains if the bridges ever move out of lazy.
func TestStreamDoesNotImportLazy(t *testing.T) {
	const module = "github.com/shpandrak/shpanstream"
	const forbidden = module + "/lazy"
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}

	seen := map[string]bool{}
	var visit func(importPath string, via []string)
	visit = func(importPath string, via []string) {
		if seen[importPath] {
			return
		}
		seen[importPath] = true
		dir := filepath.Join(root, filepath.FromSlash(strings.TrimPrefix(strings.TrimPrefix(importPath, module), "/")))
		pkg, err := build.ImportDir(dir, build.ImportComment)
		if err != nil {
			t.Fatalf("reading %s: %v", importPath, err)
		}
		imports := pkg.Imports
		if importPath == module+"/stream" {
			// The in-package tests count too (lazy importing stream makes that an import cycle
			// anyway); an external stream_test package may use lazy, it costs consumers nothing.
			imports = append(append([]string{}, imports...), pkg.TestImports...)
		}
		path := append(via, importPath)
		for _, imp := range imports {
			if imp == forbidden {
				t.Errorf("%s imports %s; package stream must not depend on package lazy", strings.Join(path, " -> "), forbidden)
				continue
			}
			if imp == module || strings.HasPrefix(imp, module+"/") {
				visit(imp, path)
			}
		}
	}
	visit(module+"/stream", nil)
	if len(seen) < 2 {
		t.Fatalf("import walk reached only %v; the walk itself is broken", seen)
	}
}
