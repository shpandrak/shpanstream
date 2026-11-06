package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/packages"
)

// testdata is absolute, since the tests change the working directory.
var testdata, _ = filepath.Abs("testdata")

// copyTo copies the given testdata directories, in order, into a new temporary
// directory and makes it the working directory.
func copyTo(t *testing.T, dirs ...string) string {
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range dirs {
		if err := os.CopyFS(dir, os.DirFS(d)); err != nil {
			t.Fatal(err)
		}
	}
	t.Chdir(dir)
	return dir
}

func runTool(t *testing.T, args ...string) (string, int) {
	var out, errOut bytes.Buffer
	code := run(args, &out, &errOut)
	return out.String() + errOut.String(), code
}

// snapshot returns the content of every file under dir.
func snapshot(t *testing.T, dir string) map[string]string {
	files := map[string]string{}
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, err := os.ReadFile(path)
		files[path] = string(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

// TestGolden rewrites a copy of testdata/in (a stand-in for shpanstream v0.5 and a user
// module), first with -n, and compares every Go file with testdata/out.
func TestGolden(t *testing.T) {
	dir := copyTo(t, filepath.Join(testdata, "in"))
	before := snapshot(t, dir)
	dryOut, dryCode := runTool(t, "-n")
	if got := snapshot(t, dir); len(got) != len(before) {
		t.Fatal("-n changed files")
	} else {
		for f, c := range got {
			if before[f] != c {
				t.Errorf("-n changed %s", f)
			}
		}
	}
	out, code := runTool(t)
	if code != 1 || dryCode != 1 {
		t.Errorf("exit codes: run %d, -n %d, want 1 for both", code, dryCode)
	}
	if want := strings.Replace(dryOut, "would rewrite", "rewrote", 1); out != want {
		t.Errorf("-n reported:\n%s\nthe run reported:\n%s", dryOut, out)
	}

	err := filepath.WalkDir(filepath.Join(testdata, "out"), func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		want, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(filepath.Join(testdata, "out"), path)
		got, err := os.ReadFile(filepath.Join(dir, rel))
		if err != nil {
			return err
		}
		if string(got) != string(want) {
			t.Errorf("%s differs from the golden file; got:\n%s", rel, got)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out, "rewrote 42 call sites (40 in non-test files) and 1 FilterWithErAndCtx renames in 6 files") {
		t.Errorf("unexpected summary:\n%s", out)
	}
	for _, want := range []string{
		"note: user/user.go:69:11: method value s.FindFirst became a function literal",
		"note: user/ignored.go: uses the stream package but is marked //go:build ignore",
		// Evaluation order (the stream and its context are both calls, receives or indexes).
		"hand edit: user/handedit.go:25:9: Collect: neither the stream nor its context",
		"hand edit: user/handedit.go:26:9: Count: neither",
		"hand edit: user/handedit.go:27:9: Count: neither",
		"hand edit: user/handedit.go:28:9: IsEmpty: neither",
		// Code that would not compile.
		"hand edit: user/handedit.go:33:6: Page takes the results of a multi-value call",
		"hand edit: user/handedit.go:34:12: s.FilterWithErAndCtx is not called",
		"hand edit: user/handedit.go:36:13: method expression stream.Stream[Person].Collect",
		"hand edit: user/handedit.go:38:6: Wrapped{people()}.MustCount is promoted through an embedded Stream",
		`hand edit: user/handedit.go:46:9: "stream" here is var stream`,
		// Interfaces a Stream stops satisfying.
		"hand edit: user/handedit.go:10:14: this interface has Count(ctx context.Context) (int, error)",
		"hand edit: user/handedit.go:39:17: type assertion to Counter, which has Count(",
		"hand edit: user/handedit.go:43:14: this interface has MustCollect() []Person",
		// Files that were not migrated.
		"hand edit: user/tagged.go: uses the stream package and was not migrated: excluded by build constraints",
		"hand edit: user/vendor/v/v.go: uses the stream package and was not migrated: not loaded",
		"hand edit: cgo/cgo.go: uses the stream package and was not migrated: uses cgo",
		"hand edit: broken/broken.go:13:12: expected ')', found '{' (the file does not parse",
		"hand edit: untyped/untyped.go:10:9: the receiver of Collect does not type-check",
		"streamrewrite: 17 places need a hand edit",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("the report does not mention %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"Query", "ignored.go: uses the stream package and was not"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("the report mentions %q:\n%s", unwanted, out)
		}
	}
}

// TestGoldenCompiles type-checks the golden output against a stub of v0.6, then checks
// that the tool refuses to run on it: the module is already upgraded.
func TestGoldenCompiles(t *testing.T) {
	dir := copyTo(t, filepath.Join(testdata, "v06"))
	// user/handedit.go is not in testdata/out: its hand edits are left as they were.
	if err := os.CopyFS(filepath.Join(dir, "user"), os.DirFS(filepath.Join(testdata, "out/user"))); err != nil {
		t.Fatal(err)
	}
	pkgs, err := packages.Load(&packages.Config{Mode: packages.LoadAllSyntax, Tests: true, Dir: dir}, "./...")
	if err != nil {
		t.Fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("the golden output does not compile against v0.6")
	}

	if err := os.WriteFile(filepath.Join(dir, "other.go"), []byte("package other\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if out, code := runTool(t, "."); code != 2 {
		t.Errorf("a package that does not import stream: exit code %d, output:\n%s", code, out)
	}
	before := snapshot(t, dir)
	out, code := runTool(t)
	if code != 2 || !strings.Contains(out, "Revert the shpanstream version in go.mod to v0.5.x, run streamrewrite, then upgrade") {
		t.Errorf("exit code %d, output:\n%s", code, out)
	}
	for f, c := range snapshot(t, dir) {
		if before[f] != c {
			t.Errorf("%s changed", f)
		}
	}
}

// TestTags checks that -tags reaches the loader and the file check, and that a run with
// nothing left to do exits 0 even with a //go:build ignore file around.
func TestTags(t *testing.T) {
	dir := copyTo(t, filepath.Join(testdata, "in"))
	for _, d := range []string{"user/handedit.go", "user/vendor", "broken", "untyped", "cgo"} {
		if err := os.RemoveAll(filepath.Join(dir, d)); err != nil {
			t.Fatal(err)
		}
	}
	out, code := runTool(t, "-tags", "sometag")
	if code != 0 || strings.Contains(out, "hand edit") || !strings.Contains(out, "ignored.go") {
		t.Errorf("exit code %d, output:\n%s", code, out)
	}
	got, err := os.ReadFile(filepath.Join(dir, "user", "tagged.go"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "return stream.Count(ctx, s)") {
		t.Errorf("tagged.go was not rewritten:\n%s", got)
	}
	if _, code := runTool(t, "-nosuchflag"); code != 2 {
		t.Errorf("a bad flag exits %d, want 2", code)
	}
}
