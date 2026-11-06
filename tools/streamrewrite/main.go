// Command streamrewrite migrates a module's call sites to the shpanstream v0.6
// stream API, using type information (see docs/migration.md).
//
// v0.6 moved the stream.Stream[T] methods that take no function argument to
// package functions, and renamed FilterWithErAndCtx to FilterWithErrAndCtx:
//
//	s.Collect(ctx)                    -> stream.Collect(ctx, s)
//	s.MustCollect()                   -> stream.MustCollect(s)
//	s.Count(ctx), s.MustCount()       -> stream.Count(ctx, s), stream.MustCount(s)
//	s.IsEmpty(ctx)                    -> stream.IsEmpty(ctx, s)
//	s.FindFirst(ctx), s.FindLast(ctx) -> stream.FindFirst(ctx, s), stream.FindLast(ctx, s)
//	s.CollectRandomSample(ctx, n)     -> stream.CollectRandomSample(ctx, s, n)
//	s.Limit(n), s.Skip(n)             -> stream.Limit(s, n), stream.Skip(s, n)
//	s.Page(p, size)                   -> stream.Page(s, p, size)
//	s.RandomSample(n)                 -> stream.RandomSample(s, n)
//	s.WithAdditionalLifecycle(l)      -> stream.WithAdditionalLifecycle(s, l)
//	s.WithLockWhileMaterializing(mu)  -> stream.WithLockWhileMaterializing(s, mu)
//	s.Untyped()                       -> stream.Untyped(s)
//	range s.Iterator                  -> range stream.MustValues(s)
//	range s.IndexedIterator           -> range stream.MustAll(s)
//	s.FilterWithErAndCtx(p)           -> s.FilterWithErrAndCtx(p)
//
// Usage, from inside the module to migrate:
//
//	go run github.com/shpandrak/shpanstream/tools/streamrewrite@v0.6.0 [-n] [-v] [-tags t1,t2] [packages]
//
// The packages default to ./... and test files are included. Run it while the
// module still requires shpanstream v0.5.x or earlier, then bump the dependency;
// run against v0.6 it refuses, with status 2. It makes one pass, and -n reports
// exactly what a run would do without writing anything.
//
// Every site it cannot rewrite without risking a change of behaviour or code that
// does not compile is listed as needing a hand edit, and the command then exits
// with status 1. So is every file of the module that imports the stream package
// and was not rewritten (build constraints, cgo, a parse error, outside the
// package patterns), and every interface a v0.5 Stream satisfies through a moved
// method. Files marked //go:build ignore are only noted.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/build/constraint"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
)

const streamPath = "github.com/shpandrak/shpanstream/stream"

const (
	oldFilterName = "FilterWithErAndCtx"
	newFilterName = "FilterWithErrAndCtx"
)

type kind int

const (
	transform  kind = iota // stream first: stream.Limit(s, n)
	terminal               // ctx first, then the stream: stream.Collect(ctx, s)
	iteratorFn             // stream.MustValues(s) returns the iter.Seq
)

// moved lists the methods that became package functions.
var moved = map[string]kind{
	"Collect":                    terminal,
	"MustCollect":                transform,
	"Count":                      terminal,
	"MustCount":                  transform,
	"IsEmpty":                    terminal,
	"FindFirst":                  terminal,
	"FindLast":                   terminal,
	"CollectRandomSample":        terminal,
	"Limit":                      transform,
	"Skip":                       transform,
	"Page":                       transform,
	"RandomSample":               transform,
	"WithAdditionalLifecycle":    transform,
	"WithLockWhileMaterializing": transform,
	"Untyped":                    transform,
	"Iterator":                   iteratorFn,
	"IndexedIterator":            iteratorFn,
}

// fnName is the package function the moved method name became.
func fnName(method string) string {
	switch method {
	case "Iterator":
		return "MustValues"
	case "IndexedIterator":
		return "MustAll"
	}
	return method
}

var errUpgraded = errors.New("the module already requires shpanstream v0.6 (stream.Stream has no Collect method). " +
	"Revert the shpanstream version in go.mod to v0.5.x, run streamrewrite, then upgrade")

type result struct {
	stream    *types.Named      // the generic stream.Stream type, nil if not loaded
	files     map[string][]byte // rewritten content by absolute path
	processed map[string]bool   // files whose call sites were examined
	sites     int               // moved-method sites rewritten
	testSites int               // of which in _test.go files
	renames   int               // FilterWithErAndCtx renamed
	byName    map[string]int
	notes     []string // rewrites done, but worth a look
	handEdits []string // sites and files left as they were
}

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// run is the command: it returns 0 when nothing is left to do, 1 when hand edits
// remain, and 2 on a usage or loading error, or when the module already uses v0.6.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("streamrewrite", flag.ContinueOnError)
	flags.SetOutput(stderr)
	tags := flags.String("tags", "", "comma-separated build tags to load files with")
	dry := flags.Bool("n", false, "report what would change without writing any file")
	verbose := flags.Bool("v", false, "also print the errors of the loaded packages")
	flags.Usage = func() {
		fmt.Fprintln(stderr, "usage: streamrewrite [-n] [-v] [-tags t1,t2] [packages (default ./...)]")
		flags.PrintDefaults()
	}
	if flags.Parse(args) != nil {
		return 2
	}
	patterns := flags.Args()
	if len(patterns) == 0 {
		patterns = []string{"./..."}
	}
	fail := func(err error) int {
		fmt.Fprintln(stderr, "streamrewrite:", err)
		return 2
	}
	dir, err := os.Getwd()
	if err == nil {
		dir, err = filepath.EvalSymlinks(dir)
	}
	if err != nil {
		return fail(err)
	}
	res, err := migrate(dir, patterns, *tags, *verbose, stderr)
	if err != nil {
		return fail(err)
	}
	verb := "would rewrite"
	if !*dry {
		verb = "rewrote"
		for f, content := range res.files {
			if err := os.WriteFile(f, content, 0o644); err != nil {
				return fail(err)
			}
		}
	}

	fmt.Fprintf(stdout, "streamrewrite: %s %d call sites (%d in non-test files) and %d %s renames in %d files\n",
		verb, res.sites, res.sites-res.testSites, res.renames, oldFilterName, len(res.files))
	names := make([]string, 0, len(res.byName))
	for k := range res.byName {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		fmt.Fprintf(stdout, "  %5d %s\n", res.byName[k], k)
	}
	for _, n := range dedupe(res.notes) {
		fmt.Fprintln(stdout, "note:", rel(dir, n))
	}
	hand := dedupe(res.handEdits)
	for _, h := range hand {
		fmt.Fprintln(stdout, "needs a hand edit:", rel(dir, h))
	}
	if len(hand) > 0 {
		fmt.Fprintf(stdout, "streamrewrite: %d places need a hand edit\n", len(hand))
		return 1
	}
	return 0
}

func dedupe(s []string) []string {
	out := slices.Clone(s)
	sort.Strings(out)
	return slices.Compact(out)
}

func rel(dir, msg string) string {
	if r, ok := strings.CutPrefix(msg, dir+string(filepath.Separator)); ok {
		return r
	}
	return msg
}

// moduleRoot is the directory of the go.mod that governs dir.
func moduleRoot(dir string) string {
	for d := dir; ; {
		if _, err := os.Stat(filepath.Join(d, "go.mod")); err == nil {
			return d
		}
		parent := filepath.Dir(d)
		if parent == d {
			return dir
		}
		d = parent
	}
}

// migrate loads the packages, computes the rewritten content of every file that
// changes, and lists what it could not do.
func migrate(dir string, patterns []string, tags string, verbose bool, stderr io.Writer) (*result, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
		Tests: true,
		Dir:   dir,
	}
	if tags != "" {
		cfg.BuildFlags = []string{"-tags=" + tags}
	}
	// The stream package itself is loaded too, to tell which API the module requires even
	// when the packages to migrate import it only indirectly.
	pkgs, err := packages.Load(cfg, append(patterns, streamPath)...)
	if err != nil {
		return nil, err
	}
	res := &result{files: map[string][]byte{}, processed: map[string]bool{}, byName: map[string]int{}}
	packages.Visit(pkgs, nil, func(p *packages.Package) {
		if verbose {
			for _, e := range p.Errors {
				fmt.Fprintln(stderr, e)
			}
		}
		if p.Types == nil {
			return
		}
		for _, imp := range append(p.Types.Imports(), p.Types) {
			if tn, ok := imp.Scope().Lookup("Stream").(*types.TypeName); ok && imp.Path() == streamPath {
				res.stream, _ = tn.Type().(*types.Named)
			}
		}
	})
	if res.stream != nil && !slices.ContainsFunc(slices.Collect(res.stream.Methods()), func(m *types.Func) bool {
		return m.Name() == "Collect" && m.Signature().TypeParams().Len() == 0
	}) {
		return nil, errUpgraded
	}

	root := moduleRoot(dir)
	sep := string(filepath.Separator)
	inModule := func(fname string) bool {
		return strings.HasPrefix(fname, root+sep) && !strings.HasPrefix(fname, root+sep+"vendor"+sep)
	}
	for _, p := range pkgs {
		if p.TypesInfo == nil {
			continue
		}
		for _, f := range p.Syntax {
			fname := p.Fset.File(f.Pos()).Name()
			if res.processed[fname] || !inModule(fname) {
				continue
			}
			src, err := os.ReadFile(fname)
			if err != nil {
				return nil, err
			}
			// The loader keeps what it could parse of a broken file: leave it to the check below.
			if _, err := parser.ParseFile(token.NewFileSet(), fname, src, parser.SkipObjectResolution); err != nil {
				continue
			}
			res.processed[fname] = true
			if err := rewriteFile(res, p, f, fname, src); err != nil {
				return nil, err
			}
		}
	}
	return res, checkUnprocessed(res, root, tags)
}

// checkUnprocessed lists every file of the module under root that imports the stream
// package and was not processed, so that no call site is left behind without a word.
func checkUnprocessed(res *result, root, tags string) error {
	bctx := build.Default
	if tags != "" {
		bctx.BuildTags = strings.Split(tags, ",")
	}
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != root && (name == "testdata" || strings.HasPrefix(name, ".") || path == filepath.Join(root, "vendor")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(name, ".go") || res.processed[path] {
			return nil
		}
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			res.handEdits = append(res.handEdits, fmt.Sprintf("%v (the file does not parse, so it was not migrated)", err))
			return nil
		}
		if !slices.ContainsFunc(f.Imports, func(s *ast.ImportSpec) bool { return s.Path.Value == `"`+streamPath+`"` }) {
			return nil
		}
		if ignoreTagged(f) {
			res.notes = append(res.notes, path+": uses the stream package but is marked //go:build ignore, so it was not migrated")
			return nil
		}
		why := "not loaded: outside the package patterns, in a vendor directory or in another module"
		if slices.ContainsFunc(f.Imports, func(s *ast.ImportSpec) bool { return s.Path.Value == `"C"` }) {
			why = "uses cgo, which this tool does not rewrite"
		} else if ok, _ := bctx.MatchFile(filepath.Dir(path), name); !ok {
			why = "excluded by build constraints: rerun with -tags (or GOOS/GOARCH) to migrate it"
		}
		res.handEdits = append(res.handEdits, fmt.Sprintf("%s: uses the stream package and was not migrated: %s", path, why))
		return nil
	})
}

// ignoreTagged reports whether f is marked //go:build ignore.
func ignoreTagged(f *ast.File) bool {
	for _, g := range f.Comments {
		if g.Pos() > f.Package {
			break
		}
		for _, c := range g.List {
			if x, err := constraint.Parse(c.Text); err == nil && x.String() == "ignore" {
				return true
			}
		}
	}
	return false
}

type fileRewriter struct {
	res        *result
	pkg        *packages.Package
	file       *ast.File
	fset       *token.FileSet
	info       *types.Info
	src        []byte
	fname      string
	isTest     bool
	qual       string            // "stream.", "ss." or "" (inside package stream, or dot import)
	qualName   string            // the identifier in qual, "" when qual is ""
	needImport bool              // the file does not import the stream package yet
	imports    map[string]string // import path -> local name, for spelling closure types
	renames    map[*ast.Ident]bool
	sites      int
}

func rewriteFile(res *result, p *packages.Package, f *ast.File, fname string, src []byte) error {
	w := &fileRewriter{
		res: res, pkg: p, file: f, fset: p.Fset, info: p.TypesInfo, src: src, fname: fname,
		isTest:  strings.HasSuffix(fname, "_test.go"),
		imports: map[string]string{},
		renames: map[*ast.Ident]bool{},
	}
	for _, imp := range f.Imports {
		path := strings.Trim(imp.Path.Value, `"`)
		name := ""
		if imp.Name != nil {
			name = imp.Name.Name
		} else if pn, ok := w.info.Implicits[imp].(*types.PkgName); ok {
			name = pn.Imported().Name()
		}
		if name != "_" {
			w.imports[path] = name
		}
	}
	switch name, ok := w.imports[streamPath]; {
	case p.Types.Path() == streamPath || (ok && name == "."):
		w.qual = ""
	case ok:
		w.qual, w.qualName = name+".", name
	default:
		w.qual, w.qualName, w.needImport = "stream.", "stream", true
	}

	called := map[ast.Expr]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			called[ast.Unparen(x.Fun)] = true
		case *ast.SelectorExpr:
			w.checkSelector(x, called[x])
		case *ast.InterfaceType:
			if m := w.lostMethod(w.info.TypeOf(x)); m != "" {
				w.handEdit(x.Pos(), "this interface has %s, a method a Stream loses in v0.6: Stream no longer satisfies it, also in type assertions", m)
			}
		case *ast.TypeAssertExpr:
			w.checkAssertion(x.Type)
		case *ast.TypeSwitchStmt:
			for _, c := range x.Body.List {
				for _, e := range c.(*ast.CaseClause).List {
					w.checkAssertion(e)
				}
			}
		}
		return true
	})

	var edits []edit
	for _, t := range w.collect(f) {
		edits = append(edits, edit{w.off(t.Pos()), w.off(t.End()), w.render(t)})
	}
	if len(edits) == 0 {
		return nil
	}
	out := append([]byte(nil), src...)
	for i := len(edits) - 1; i >= 0; i-- {
		e := edits[i]
		out = append(out[:e.start], append([]byte(e.text), out[e.end:]...)...)
	}
	if bytes.Equal(out, src) {
		return nil
	}
	fs := token.NewFileSet()
	af, err := parser.ParseFile(fs, fname, out, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("%s: the rewritten file does not parse (please report this): %v", fname, err)
	}
	if w.needImport && w.sites > 0 {
		astutil.AddImport(fs, af, streamPath)
	}
	var b bytes.Buffer
	if err := format.Node(&b, fs, af); err != nil {
		return fmt.Errorf("%s: %v", fname, err)
	}
	if ast.IsGenerated(f) {
		w.note(f.Package, "generated file rewritten; also update its generator")
	}
	res.files[fname] = b.Bytes()
	return nil
}

// checkSelector marks FilterWithErAndCtx for renaming, or reports it when it is not
// called (v0.6 makes it a generic method, which has no method value or expression),
// and reports a moved method name whose receiver did not type-check.
func (w *fileRewriter) checkSelector(sel *ast.SelectorExpr, isCalled bool) {
	name := sel.Sel.Name
	_, isMoved := moved[name]
	s := w.info.Selections[sel]
	switch {
	case s == nil && w.info.Uses[sel.Sel] == nil && !known(w.info.TypeOf(sel.X)) && (isMoved || name == oldFilterName) && !w.isPackage(sel.X):
		w.handEdit(sel.Pos(), "the receiver of %s does not type-check, so whether it is a Stream is unknown: fix the type errors (see -v), then rerun", name)
	case name != oldFilterName || !w.selectsStreamMethod(sel):
	case isCalled && s.Kind() == types.MethodVal:
		w.renames[sel.Sel] = true
	default:
		w.handEdit(sel.Pos(), "%s is not called, and %s is a generic method in v0.6: write a function literal calling it", w.text(sel), newFilterName)
	}
}

// isPackage reports whether e names an imported package.
func (w *fileRewriter) isPackage(e ast.Expr) bool {
	id, ok := e.(*ast.Ident)
	_, isPkg := w.info.Uses[id].(*types.PkgName)
	return ok && isPkg
}

// checkAssertion reports a type assertion (or type switch case) to an interface that a
// Stream satisfies in v0.5 and not in v0.6: it would start failing at run time.
func (w *fileRewriter) checkAssertion(e ast.Expr) {
	if _, lit := e.(*ast.InterfaceType); e == nil || lit {
		return // an interface literal is reported as such
	}
	if m := w.lostMethod(w.info.TypeOf(e)); m != "" {
		w.handEdit(e.Pos(), "type assertion to %s, which has %s: it no longer matches a Stream in v0.6", w.text(e), m)
	}
}

// lostMethod returns the first method of interface t that Stream[X] has, with an
// identical signature, for some X, and that moved to a function; or "".
func (w *fileRewriter) lostMethod(t types.Type) string {
	if t == nil || w.res.stream == nil {
		return ""
	}
	it, ok := t.Underlying().(*types.Interface)
	if !ok {
		return ""
	}
	for m := range it.Methods() {
		if _, isMoved := moved[m.Name()]; !isMoved {
			continue
		}
		var cands []types.Type
		typesIn(m.Type(), &cands)
		for _, x := range cands {
			st, err := types.Instantiate(nil, w.res.stream, []types.Type{x}, true)
			if err != nil {
				continue
			}
			obj, _, _ := types.LookupFieldOrMethod(st, false, nil, m.Name())
			if fn, ok := obj.(*types.Func); ok && types.Identical(fn.Type(), m.Type()) {
				return m.Name() + strings.TrimPrefix(types.TypeString(m.Type(), types.RelativeTo(w.pkg.Types)), "func")
			}
		}
	}
	return ""
}

// typesIn appends t and the types it is built from: the candidates for a Stream's T.
func typesIn(t types.Type, out *[]types.Type) {
	*out = append(*out, t)
	switch x := t.(type) {
	case *types.Signature:
		typesIn(x.Params(), out)
		typesIn(x.Results(), out)
	case *types.Tuple:
		for v := range x.Variables() {
			typesIn(v.Type(), out)
		}
	case *types.Slice:
		typesIn(x.Elem(), out)
	case *types.Pointer:
		typesIn(x.Elem(), out)
	case *types.Named:
		for a := range x.TypeArgs().Types() {
			typesIn(a, out)
		}
	}
}

type edit struct {
	start, end int
	text       string
}

func (w *fileRewriter) off(pos token.Pos) int { return w.fset.Position(pos).Offset }

func (w *fileRewriter) where(pos token.Pos) string {
	p := w.fset.Position(pos)
	return fmt.Sprintf("%s:%d:%d", p.Filename, p.Line, p.Column)
}

func (w *fileRewriter) note(pos token.Pos, format string, args ...any) {
	w.res.notes = append(w.res.notes, w.where(pos)+": "+fmt.Sprintf(format, args...))
}

func (w *fileRewriter) handEdit(pos token.Pos, format string, args ...any) {
	w.res.handEdits = append(w.res.handEdits, w.where(pos)+": "+fmt.Sprintf(format, args...))
}

// isStream reports whether t is stream.Stream[...] or a pointer to it.
func isStream(t types.Type) bool {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	n, ok := types.Unalias(t).(*types.Named)
	if !ok {
		return false
	}
	obj := n.Origin().Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == streamPath && obj.Name() == "Stream"
}

func isPointer(t types.Type) bool {
	if t == nil {
		return false
	}
	_, ok := t.Underlying().(*types.Pointer)
	return ok
}

func known(t types.Type) bool {
	return t != nil && t != types.Typ[types.Invalid]
}

// isTuple reports whether t is the type of a call with several results.
func isTuple(t types.Type) bool {
	tup, ok := t.(*types.Tuple)
	return ok && tup.Len() > 1
}

// selectsStreamMethod reports whether sel is x.M for a method M of stream.Stream.
func (w *fileRewriter) selectsStreamMethod(sel *ast.SelectorExpr) bool {
	s := w.info.Selections[sel]
	if s == nil {
		return false
	}
	fn, ok := s.Obj().(*types.Func)
	return ok && fn.Signature().Recv() != nil && isStream(fn.Signature().Recv().Type())
}

// movedMethod reports whether sel selects one of the moved methods in a form this tool
// rewrites, and whether the receiver must be dereferenced. It records why not when it is
// a moved method it cannot rewrite.
func (w *fileRewriter) movedMethod(sel *ast.SelectorExpr) (deref, ok bool) {
	name := sel.Sel.Name
	if _, isMoved := moved[name]; !isMoved || !w.selectsStreamMethod(sel) {
		return false, false
	}
	switch s := w.info.Selections[sel]; {
	case s.Kind() == types.MethodExpr:
		w.handEdit(sel.Pos(), "method expression %s: replace it with a function literal calling %s%s", w.text(sel), w.qual, fnName(name))
		return false, false
	case len(s.Index()) > 1:
		w.handEdit(sel.Pos(), "%s is promoted through an embedded Stream: call %s%s on the embedded field", w.text(sel), w.qual, fnName(name))
		return false, false
	}
	return isPointer(w.info.TypeOf(sel.X)), w.qualifierResolves(sel.Pos(), fnName(name))
}

// qualifierResolves reports whether the stream package (or, without a qualifier, the
// function itself) is what its name refers to at pos.
func (w *fileRewriter) qualifierResolves(pos token.Pos, fn string) bool {
	scope := w.pkg.Types.Scope().Innermost(pos)
	if scope == nil {
		return true
	}
	ident := w.qualName
	if ident == "" {
		ident = fn
	}
	_, obj := scope.LookupParent(ident, pos)
	var ok bool
	switch {
	case obj == nil:
		ok = true
	case w.qualName == "":
		ok = obj.Pkg() != nil && obj.Pkg().Path() == streamPath && obj.Parent() == obj.Pkg().Scope()
	case !w.needImport:
		pn, isPkg := obj.(*types.PkgName)
		ok = isPkg && pn.Imported().Path() == streamPath
	}
	if !ok {
		w.handEdit(pos, "%q here is %s, not the stream package: rename it, then rerun", ident,
			types.ObjectString(obj, types.RelativeTo(w.pkg.Types)))
	}
	return ok
}

func (w *fileRewriter) isTarget(n ast.Node) bool {
	switch x := n.(type) {
	case *ast.CallExpr:
		if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
			_, ok := w.movedMethod(sel)
			return ok
		}
	case *ast.SelectorExpr:
		_, ok := w.movedMethod(x)
		return ok
	case *ast.Ident:
		return w.renames[x]
	}
	return false
}

// collect returns the outermost targets strictly below n.
func (w *fileRewriter) collect(n ast.Node) []ast.Node {
	var out []ast.Node
	ast.Inspect(n, func(x ast.Node) bool {
		if x == nil || x == n {
			return true
		}
		if w.isTarget(x) {
			out = append(out, x)
			return false
		}
		return true
	})
	return out
}

// collectIn is collect including n itself.
func (w *fileRewriter) collectIn(n ast.Node) []ast.Node {
	if w.isTarget(n) {
		return []ast.Node{n}
	}
	return w.collect(n)
}

func (w *fileRewriter) text(n ast.Node) string {
	return string(w.src[w.off(n.Pos()):w.off(n.End())])
}

func (w *fileRewriter) render(n ast.Node) string {
	if w.isTarget(n) {
		switch x := n.(type) {
		case *ast.CallExpr:
			return w.renderCall(x)
		case *ast.SelectorExpr:
			return w.renderValue(x)
		case *ast.Ident:
			w.res.renames++
			return newFilterName
		}
	}
	return w.renderSpan(n.Pos(), n.End(), w.collect(n))
}

// renderSpan renders the source between start and end with the given targets (which lie
// inside it, in order) rewritten.
func (w *fileRewriter) renderSpan(start, end token.Pos, targets []ast.Node) string {
	var b strings.Builder
	cur := w.off(start)
	for _, t := range targets {
		if t.Pos() < start || t.End() > end {
			continue
		}
		b.Write(w.src[cur:w.off(t.Pos())])
		b.WriteString(w.render(t))
		cur = w.off(t.End())
	}
	b.Write(w.src[cur:w.off(end)])
	return b.String()
}

func (w *fileRewriter) argTargets(args []ast.Expr) []ast.Node {
	var out []ast.Node
	for _, a := range args {
		out = append(out, w.collectIn(a)...)
	}
	return out
}

// comments returns the comments between from and to (the ones a chain keeps between a
// receiver and the method name), one per line.
func (w *fileRewriter) comments(from, to token.Pos) string {
	var lines []string
	for _, g := range w.file.Comments {
		for _, c := range g.List {
			if c.Pos() >= from && c.End() <= to {
				lines = append(lines, c.Text)
			}
		}
	}
	return strings.Join(lines, "\n")
}

func (w *fileRewriter) receiver(sel *ast.SelectorExpr) string {
	deref, _ := w.movedMethod(sel)
	r := w.render(sel.X)
	if deref {
		r = "*" + r
	}
	return r
}

func (w *fileRewriter) count(name string) {
	w.sites++
	w.res.sites++
	if w.isTest {
		w.res.testSites++
	}
	w.res.byName[name]++
}

func (w *fileRewriter) renderCall(call *ast.CallExpr) string {
	sel := call.Fun.(*ast.SelectorExpr)
	name := sel.Sel.Name
	k := moved[name]
	if k == terminal && len(call.Args) == 0 {
		w.handEdit(call.Pos(), "%s called without a context", name)
		return w.renderUnmoved(call)
	}
	if k == terminal && !w.pure(sel.X) && !w.pure(call.Args[0]) {
		w.handEdit(call.Pos(), "%s: neither the stream nor its context argument is known to be free of side effects, and %s%s(ctx, s) evaluates the context first; assign one of them to a variable, then rerun", name, w.qual, name)
		return w.renderUnmoved(call)
	}
	if len(call.Args) == 1 && isTuple(w.info.TypeOf(call.Args[0])) {
		w.handEdit(call.Pos(), "%s takes the results of a multi-value call, which cannot follow the stream argument: assign them to variables, then rerun", name)
		return w.renderUnmoved(call)
	}
	w.count(name)
	recv := w.receiver(sel)
	// A comment between the receiver and the method name (a commented chain) goes right after
	// the receiver argument; the comma and newline keep a line comment from eating the rest.
	cm := w.comments(sel.X.End(), call.Lparen)
	targets := w.argTargets(call.Args)
	fn := w.qual + fnName(name)
	switch k {
	case iteratorFn:
		if cm != "" {
			recv += ", " + cm + "\n"
		}
		return fn + "(" + recv + ")(" + w.renderSpan(call.Lparen+1, call.Rparen, targets) + ")"
	case terminal:
		first := w.renderSpan(call.Lparen+1, call.Args[0].End(), targets)
		rest := w.renderSpan(call.Args[0].End(), call.Rparen, targets)
		if cm != "" {
			rest = strings.TrimPrefix(strings.TrimLeft(rest, " \t"), ",")
			return fn + "(" + first + ", " + recv + ", " + cm + "\n" + rest + ")"
		}
		return fn + "(" + first + ", " + recv + rest + ")"
	default:
		switch {
		case cm != "":
			recv += ", " + cm + "\n"
		case len(call.Args) > 0:
			recv += ", "
		}
		return fn + "(" + recv + w.renderSpan(call.Lparen+1, call.Rparen, targets) + ")"
	}
}

// renderUnmoved renders a call left for a hand edit, with only its receiver and arguments
// rewritten (not the method itself, as a method value).
func (w *fileRewriter) renderUnmoved(call *ast.CallExpr) string {
	sel := call.Fun.(*ast.SelectorExpr)
	return w.renderSpan(call.Pos(), call.End(), append(w.collectIn(sel.X), w.argTargets(call.Args)...))
}

// pure reports whether evaluating e provably has no side effects, from a short list:
// identifiers, selectors and map index expressions over those, literals,
// context.Background(), context.TODO(), and the Context method of a testing type.
func (w *fileRewriter) pure(e ast.Expr) bool {
	switch x := ast.Unparen(e).(type) {
	case *ast.Ident, *ast.BasicLit, *ast.FuncLit:
		return true
	case *ast.SelectorExpr:
		return w.pure(x.X)
	case *ast.IndexExpr:
		_, isMap := types.Unalias(w.info.TypeOf(x.X)).Underlying().(*types.Map)
		return isMap && w.pure(x.X) && w.pure(x.Index)
	case *ast.CallExpr:
		sel, ok := ast.Unparen(x.Fun).(*ast.SelectorExpr)
		if !ok || len(x.Args) != 0 {
			return false
		}
		fn, ok := w.info.Uses[sel.Sel].(*types.Func)
		if !ok || fn.Pkg() == nil {
			return false
		}
		switch fn.Pkg().Path() {
		case "context":
			return fn.Name() == "Background" || fn.Name() == "TODO"
		case "testing":
			return fn.Name() == "Context" && fn.Signature().Recv() != nil && w.pure(sel.X)
		}
	}
	return false
}

// renderValue rewrites a moved method used as a value: s.Iterator becomes the iterator
// stream.MustValues(s); anything else becomes a function literal.
func (w *fileRewriter) renderValue(sel *ast.SelectorExpr) string {
	name := sel.Sel.Name
	cm := w.comments(sel.X.End(), sel.Sel.Pos())
	if moved[name] == iteratorFn {
		w.count(name)
		recv := w.receiver(sel)
		if cm != "" {
			recv += ", " + cm + "\n"
		}
		return w.qual + fnName(name) + "(" + recv + ")"
	}
	lit, why := w.closure(sel)
	if lit == "" {
		w.handEdit(sel.Pos(), "method value %s: %s; replace it with a function literal calling %s%s", w.text(sel), why, w.qual, name)
		return w.renderSpan(sel.Pos(), sel.End(), w.collect(sel))
	}
	w.count(name)
	if cm != "" {
		w.note(sel.Pos(), "comment dropped while rewriting method value %s", name)
	}
	w.note(sel.Pos(), "method value %s became a function literal", w.text(sel))
	return lit
}

// closure spells the function literal equivalent to the method value sel, or says why not.
// A method value evaluates its receiver when it is taken, so the literal binds the
// receiver first, unless it is a local variable that is never reassigned.
func (w *fileRewriter) closure(sel *ast.SelectorExpr) (string, string) {
	s := w.info.Selections[sel]
	if s == nil {
		return "", "its type is not known"
	}
	sig, ok := s.Type().(*types.Signature)
	if !ok {
		return "", "its type is not a function"
	}
	unqualifiable := ""
	qf := func(p *types.Package) string {
		if p.Path() == w.pkg.Types.Path() {
			return ""
		}
		if name, ok := w.imports[p.Path()]; ok {
			if name == "." {
				return ""
			}
			return name
		}
		unqualifiable = p.Path()
		return p.Name()
	}
	root := rootIdent(sel.X)
	var params, args []string
	for i := 0; i < sig.Params().Len(); i++ {
		v := sig.Params().At(i)
		n := v.Name()
		if n == "" || n == "_" || n == root || n == w.qualName {
			n = fmt.Sprintf("a%d", i)
		}
		params = append(params, n+" "+types.TypeString(v.Type(), qf))
		args = append(args, n)
	}
	var results []string
	for i := 0; i < sig.Results().Len(); i++ {
		results = append(results, types.TypeString(sig.Results().At(i).Type(), qf))
	}
	if unqualifiable != "" {
		return "", "its type needs package " + unqualifiable + ", which this file does not import"
	}
	recvExpr := w.receiver(sel)
	recv, bind := recvExpr, ""
	if !w.stableReceiver(sel) {
		bind = "r"
		for i := 0; slices.Contains(args, bind) || bind == w.qualName; i++ {
			bind = fmt.Sprintf("r%d", i)
		}
		recv = bind
	}
	var call []string
	if moved[sel.Sel.Name] == terminal && len(args) > 0 {
		call = append([]string{args[0], recv}, args[1:]...)
	} else {
		call = append([]string{recv}, args...)
	}
	res := ""
	switch len(results) {
	case 0:
	case 1:
		res = " " + results[0]
	default:
		res = " (" + strings.Join(results, ", ") + ")"
	}
	body := w.qual + sel.Sel.Name + "(" + strings.Join(call, ", ") + ")"
	if len(results) > 0 {
		body = "return " + body
	}
	ftype := "func(" + strings.Join(params, ", ") + ")" + res
	lit := ftype + " { " + body + " }"
	if bind != "" {
		lit = "func() " + ftype + " {\n" + bind + " := " + recvExpr + "\nreturn " + lit + "\n}()"
	}
	return lit, ""
}

// stableReceiver reports whether the receiver of the method value sel is a local
// variable (not dereferenced) that is never assigned to or has its address taken after
// its declaration, so reading it later reads the same stream.
func (w *fileRewriter) stableReceiver(sel *ast.SelectorExpr) bool {
	id, ok := sel.X.(*ast.Ident)
	if !ok || isPointer(w.info.TypeOf(id)) {
		return false
	}
	v, ok := w.info.Uses[id].(*types.Var)
	if !ok || v.Parent() == nil || v.Parent() == w.pkg.Types.Scope() {
		return false
	}
	isV := func(e ast.Expr) bool {
		for {
			switch x := e.(type) {
			case *ast.Ident:
				return w.info.Uses[x] == v
			case *ast.ParenExpr:
				e = x.X
			case *ast.SelectorExpr:
				e = x.X
			case *ast.IndexExpr:
				e = x.X
			default:
				return false
			}
		}
	}
	stable := true
	ast.Inspect(w.file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.AssignStmt:
			stable = stable && !slices.ContainsFunc(x.Lhs, isV)
		case *ast.RangeStmt:
			stable = stable && (x.Tok != token.ASSIGN || !isV(x.Key) && (x.Value == nil || !isV(x.Value)))
		case *ast.UnaryExpr:
			stable = stable && (x.Op != token.AND || !isV(x.X))
		}
		return stable
	})
	return stable
}

func rootIdent(e ast.Expr) string {
	for {
		switch x := e.(type) {
		case *ast.Ident:
			return x.Name
		case *ast.SelectorExpr:
			e = x.X
		default:
			return ""
		}
	}
}
