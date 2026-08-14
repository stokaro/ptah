// Package qtshape enforces the two quicktest shape rules this repository holds
// absolute: every assertion goes through a *qt.C, and every subtest is spelled
// t.Run with a fresh qt.New inside.
//
// The analysis is pure syntax. It runs go/parser over individual files and never
// type-checks, which is the point: 56% of the qt.Assert call sites in this tree
// live in files behind //go:build integration, and 3 more live in the separate
// testkit module. Anything built on go/packages sees none of them without an
// extra -tags invocation per build tag and an extra run per module, and a gate
// that silently covers less than half of its own subject is the failure mode
// this gate exists to avoid.
//
// Pure syntax is not the same as matching text. Every rule below asks resolve.go
// which declaration an identifier names at the position it is written, so a
// selector counts as quicktest's only when this file imports quicktest and
// nothing nearer binds that name, and a callback counts whether it was written
// inline or bound to a name first.
package qtshape

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"sort"
	"strconv"
)

// QuicktestImportPath is the module path of the assertion library these rules
// are about.
const QuicktestImportPath = "github.com/frankban/quicktest"

// RequiredImportAlias is the only name quicktest may be imported under. R0
// reports every other spelling, and the rule messages name this one as the
// shape to write.
const RequiredImportAlias = "qt"

// testingImportPath is the standard testing package, whose T, B, F and TB the
// subtest rules recognize.
const testingImportPath = "testing"

// Rule identifies which prohibition a Finding reports.
type Rule string

const (
	// RuleImportAlias reports quicktest imported under a name other than qt.
	RuleImportAlias Rule = "R0"
	// RulePackageAssert reports a use of the package-level qt.Assert or qt.Check.
	RulePackageAssert Rule = "R1"
	// RuleCheckerSubtest reports a subtest driven by a *qt.C: either the callback
	// consumes one, or the receiver .Run is called on is one.
	RuleCheckerSubtest Rule = "R2"
	// RuleBorrowedChecker reports a t.Run or f.Fuzz closure that reaches back out
	// to the enclosing scope's checker or testing.TB instead of building its own
	// from the parameter it was handed.
	RuleBorrowedChecker Rule = "R3"
)

// Finding is one violation, located precisely enough to jump to.
type Finding struct {
	Path    string
	Line    int
	Col     int
	Rule    Rule
	Message string
}

// String renders a finding in the file:line:col: rule: message form editors and
// CI log scrapers already understand.
func (f Finding) String() string {
	return fmt.Sprintf("%s:%d:%d: %s: %s", f.Path, f.Line, f.Col, f.Rule, f.Message)
}

// ErrNoFiles is returned when a scan was handed nothing to look at. A gate whose
// file selection breaks must go red, never green: an empty scan reporting
// success is how a check keeps passing after it has stopped checking anything.
var ErrNoFiles = errors.New("qtshape: no test files to scan; refusing to report success")

// ScanFile parses src as Go source attributed to path and reports every
// violation in it, ordered by position.
//
// src is passed rather than read so callers can scan fixtures and buffers, and
// so a parse error is attributed to a path the reader can open.
func ScanFile(path string, src []byte) ([]Finding, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, fmt.Errorf("qtshape: parsing %s: %w", path, err)
	}

	a := newAnalysis(fset, path, file)

	var findings []Finding
	findings = append(findings, a.importAliasFindings()...)
	findings = append(findings, a.packageAssertFindings()...)
	findings = append(findings, a.subtestFindings()...)

	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		return findings[i].Col < findings[j].Col
	})

	return dedupe(findings), nil
}

// ScanFiles reads and scans every path, returning the findings and the number of
// files actually scanned. The count is returned rather than logged so callers can
// assert on it: this gate's subject is spread across build tags and modules, and
// the count is the only evidence that the selection did not quietly shrink.
func ScanFiles(paths []string) ([]Finding, int, error) {
	if len(paths) == 0 {
		return nil, 0, ErrNoFiles
	}

	var (
		findings []Finding
		scanned  int
	)

	for _, path := range paths {
		src, err := os.ReadFile(path)
		if err != nil {
			return nil, scanned, fmt.Errorf("qtshape: reading %s: %w", path, err)
		}
		fileFindings, err := ScanFile(path, src)
		if err != nil {
			return nil, scanned, err
		}
		scanned++
		findings = append(findings, fileFindings...)
	}

	return findings, scanned, nil
}

// dedupe drops findings that repeat a position and rule already reported.
//
// Nested subtests are the reason it exists: a borrowed checker used inside two
// levels of t.Run is a violation of the inner closure and of the outer one, and
// both walks land on the same identifier. The finding is about the identifier,
// so it is reported once. A callback bound to a name and handed to two Run calls
// lands there too.
func dedupe(findings []Finding) []Finding {
	type key struct {
		line, col int
		rule      Rule
	}

	seen := make(map[key]struct{}, len(findings))
	out := findings[:0]
	for _, f := range findings {
		k := key{line: f.Line, col: f.Col, rule: f.Rule}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, f)
	}

	return out
}

// analysis is one parsed file plus the name table every rule needs to tell a
// package selector from a local name that happens to be spelled the same.
type analysis struct {
	fset  *token.FileSet
	path  string
	file  *ast.File
	names *bindings

	// quicktest and testing are the import specs, nil when the file does not
	// import that package, and qtName and tbName are the identifiers those
	// imports bind. Every rule keyed on qt.X or testing.X asks these two rather
	// than comparing against a fixed spelling.
	quicktest *ast.ImportSpec
	testing   *ast.ImportSpec
	qtName    string
	tbName    string

	fileScope  scope
	declScopes []declScope
}

// newAnalysis resolves the file's imports and builds its lexical name table.
func newAnalysis(fset *token.FileSet, path string, file *ast.File) *analysis {
	a := &analysis{fset: fset, path: path, file: file, names: newBindings(file)}
	a.quicktest, a.qtName = importedAs(file, QuicktestImportPath)
	a.testing, a.tbName = importedAs(file, testingImportPath)

	return a
}

// importedAs finds the file's import of a path and the name it binds.
func importedAs(file *ast.File, path string) (*ast.ImportSpec, string) {
	for _, spec := range file.Imports {
		imported, err := strconv.Unquote(spec.Path.Value)
		if err != nil || imported != path {
			continue
		}
		name := importBindingName(spec)
		if name == "" {
			continue
		}
		return spec, name
	}

	return nil, ""
}

// isQuicktest reports whether expr is the selector qt.name, with qt resolving to
// this file's quicktest import at the position it is written.
//
// The receiver is resolved rather than compared to the string "qt", because a
// file that does not import quicktest and declares its own qt was reported for
// calls that cannot reach the prohibited functions, and because a file that
// imports quicktest under another name used to silence R1 and R2 outright.
func (a *analysis) isQuicktest(expr ast.Expr, name string) bool {
	return a.isImported(expr, a.quicktest, a.qtName, name)
}

// isTesting reports whether expr is the selector testing.name, with testing
// resolving to this file's testing import.
func (a *analysis) isTesting(expr ast.Expr, name string) bool {
	return a.isImported(expr, a.testing, a.tbName, name)
}

// isImported reports whether expr is a selector on the given import.
func (a *analysis) isImported(expr ast.Expr, spec *ast.ImportSpec, pkgName, name string) bool {
	if spec == nil {
		return false
	}
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	ident, ok := sel.X.(*ast.Ident)
	if !ok || ident.Name != pkgName {
		return false
	}

	return a.names.resolvesToImport(ident, spec)
}

// importAliasFindings reports quicktest imported under any name but qt.
//
// The package clause of github.com/frankban/quicktest is `package quicktest`, so
// an unaliased import binds `quicktest` and is a violation too. importas in
// .golangci.yml already requires the alias; duplicating it here is deliberate,
// because the alias is what every example in AGENTS.md is written in and a file
// that renames it is unreadable against the house style even though R1, R2 and
// R3 now follow the import rather than the spelling.
func (a *analysis) importAliasFindings() []Finding {
	var findings []Finding

	for _, spec := range a.file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil || importPath != QuicktestImportPath {
			continue
		}
		if spec.Name != nil && spec.Name.Name == RequiredImportAlias {
			continue
		}
		spelled := "no alias, binding the package name quicktest"
		if spec.Name != nil {
			spelled = fmt.Sprintf("alias %q", spec.Name.Name)
		}
		findings = append(findings, a.finding(spec.Pos(), RuleImportAlias, fmt.Sprintf(
			"import %q with %s; it must be imported as %q, because the assertion and subtest rules and every example in AGENTS.md are written in that identifier",
			QuicktestImportPath, spelled, RequiredImportAlias,
		)))
	}

	return findings
}

// packageAssertFindings reports qt.Assert and qt.Check.
//
// It matches the selector rather than the call, so `assert := qt.Assert` cannot
// launder the prohibition through a variable. It never looks at the argument
// list: qt.Assert(subT, ...) and qt.Assert(c.TB, ...) are the same violation as
// qt.Assert(t, ...), and a rule keyed on a first argument named `t` would miss
// both.
func (a *analysis) packageAssertFindings() []Finding {
	if a.quicktest == nil {
		return nil
	}

	var findings []Finding

	ast.Inspect(a.file, func(node ast.Node) bool {
		sel, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name != "Assert" && sel.Sel.Name != "Check" {
			return true
		}
		if !a.isQuicktest(sel, sel.Sel.Name) {
			return true
		}
		findings = append(findings, a.finding(sel.Pos(), RulePackageAssert, fmt.Sprintf(
			"%s.%s is forbidden; create a checker from the testing.TB that owns this scope (c := qt.New(t)) and call c.%s(...) without the TB argument",
			a.qtName, sel.Sel.Name, sel.Sel.Name,
		)))
		return true
	})

	return findings
}

// binding is a name bound to a *qt.C or to a testing.TB, carried with the
// position of its declaration. The position is what lets a closure tell the
// bindings it made from the ones it inherited, which is the whole of R3.
type binding struct {
	name string
	pos  token.Pos
}

// scope is what one top-level declaration can see: the checker and TB names
// bound anywhere inside it, plus the ones bound at file level.
type scope struct {
	checkers []binding
	tbs      []binding
}

// declScope pairs a top-level declaration with the bindings visible inside it.
type declScope struct {
	decl   ast.Decl
	extent span
	sc     scope
}

// subtestFindings reports R2 and R3.
//
// Bindings are gathered per top-level declaration rather than per file so a `c`
// that is a checker in one test function cannot make an unrelated `c` in another
// function look like one. testdata/decoys.go.txt pins that: its `var c runner`
// has a Run method of its own.
func (a *analysis) subtestFindings() []Finding {
	a.buildScopes()

	var findings []Finding
	for _, ds := range a.declScopes {
		findings = append(findings, a.declarationFindings(ds)...)
	}

	return findings
}

// buildScopes computes the file's bindings and then each top-level
// declaration's.
func (a *analysis) buildScopes() {
	a.fileScope = scope{}
	for _, decl := range a.file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		checkers, tbs := a.collectBindings(gen)
		a.fileScope.checkers = append(a.fileScope.checkers, checkers...)
		a.fileScope.tbs = append(a.fileScope.tbs, tbs...)
	}

	a.declScopes = make([]declScope, 0, len(a.file.Decls))
	for _, decl := range a.file.Decls {
		a.declScopes = append(a.declScopes, declScope{
			decl:   decl,
			extent: span{start: decl.Pos(), end: decl.End()},
			sc:     a.declarationScope(decl),
		})
	}
}

// declarationScope adds a function declaration's own bindings to the file's.
func (a *analysis) declarationScope(decl ast.Decl) scope {
	fn, ok := decl.(*ast.FuncDecl)
	if !ok {
		return a.fileScope
	}

	checkers, tbs := a.collectBindings(fn)
	return scope{
		checkers: concatBindings(a.fileScope.checkers, checkers),
		tbs:      concatBindings(a.fileScope.tbs, tbs),
	}
}

// scopeAt returns the bindings visible at a position: those of the top-level
// declaration containing it, or the file's when it belongs to none.
//
// R3 needs this because a callback may be named in one declaration and called
// from another. Analyzing a file-scope callback's body against the caller's
// bindings would report the caller's local `c` as a checker borrowed by a
// closure that cannot see it.
func (a *analysis) scopeAt(pos token.Pos) scope {
	for _, ds := range a.declScopes {
		if ds.extent.contains(pos) {
			return ds.sc
		}
	}

	return a.fileScope
}

// concatBindings joins two binding lists without aliasing either backing array.
func concatBindings(x, y []binding) []binding {
	out := make([]binding, 0, len(x)+len(y))
	out = append(out, x...)
	out = append(out, y...)
	return out
}

// declarationFindings walks one declaration and reports every subtest violation
// in it.
func (a *analysis) declarationFindings(ds declScope) []Finding {
	var findings []Finding

	ast.Inspect(ds.decl, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		if f, ok := a.checkerSubtestFinding(call, ds.sc); ok {
			findings = append(findings, f)
		}
		findings = append(findings, a.borrowedCheckerFindings(call)...)
		return true
	})

	return findings
}

// checkerSubtestFinding reports a subtest driven by a *qt.C.
//
// Three spellings reach the same prohibited shape and all three are reported:
// an inline func(*qt.C) closure, a callback named elsewhere and handed over as
// an identifier, and any .Run at all whose receiver is a checker. The last one
// is what covers a method value or any other callback expression this package
// cannot resolve without types -- (*qt.C).Run is the only Run a checker has.
//
// Keying on the callback alone was the gap: `callback := func(c *qt.C) { ... }`
// followed by `c.Run(name, callback)` is the same object graph as the inline
// form and used to pass. Keying on the receiver alone would miss a checker held
// in a struct field, so both are asked and either one is enough.
func (a *analysis) checkerSubtestFinding(call *ast.CallExpr, sc scope) (Finding, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Run" || len(call.Args) < 2 {
		return Finding{}, false
	}
	reason, ok := a.checkerSubtestReason(call.Args[1], sel.X, sc)
	if !ok {
		return Finding{}, false
	}

	return a.finding(call.Pos(), RuleCheckerSubtest, fmt.Sprintf(
		"%s.Run %s; write t.Run(name, func(t *testing.T) { c := qt.New(t); ... }) so the subtest checker is a visible declaration instead of a shadowing parameter",
		receiverName(sel.X), reason,
	)), true
}

// checkerSubtestReason names which of the three spellings matched, so the
// message tells the reader what the gate saw rather than only that it objected.
func (a *analysis) checkerSubtestReason(callback, receiver ast.Expr, sc scope) (string, bool) {
	if lit, ok := callback.(*ast.FuncLit); ok && a.signatureTakesChecker(lit.Type) {
		return "with a func(*qt.C) closure is forbidden", true
	}
	if ident, ok := callback.(*ast.Ident); ok && a.namesCheckerCallback(ident) {
		return fmt.Sprintf("with the func(*qt.C) callback %s is forbidden", ident.Name), true
	}
	if a.isCheckerExpr(receiver, sc.checkers) {
		return "is a (*qt.C).Run subtest and is forbidden", true
	}

	return "", false
}

// namesCheckerCallback reports whether an identifier names a function taking a
// *qt.C, resolved where it is written.
//
// The lookup is lexical rather than a file-wide set of names, because one local
// `callback := func(c *qt.C) { ... }` must not classify an unrelated
// `callback := func(int) { ... }` in another function as a quicktest subtest and
// fail the whole repository's gate on correct code.
func (a *analysis) namesCheckerCallback(ident *ast.Ident) bool {
	decl, ok := a.names.lookup(ident.Name, ident.Pos())
	return ok && a.signatureTakesChecker(decl.sig)
}

// borrowedCheckerFindings reports a subtest closure that asserts through a
// checker, or through a testing.TB, that belongs to the scope outside it.
//
// This is the half a callback-shape rule cannot see. `t.Run(name, func(t
// *testing.T) { c.Assert(...) })` has the required signature and the required
// receiver and is still wrong: `c` is the parent's checker, so the assertion
// fails the parent test from the subtest's goroutine instead of failing the
// subtest. `func(subT *testing.T) { c := qt.New(t) }` is the same defect spelled
// with the parent's TB.
//
// A name the closure declares itself is never reported, whatever it is declared
// as, so an intentional shadow costs nothing. Only names bound outside the
// closure and never rebound inside it are.
func (a *analysis) borrowedCheckerFindings(call *ast.CallExpr) []Finding {
	lit, ok := a.subtestClosure(call)
	if !ok {
		return nil
	}

	sc := a.scopeAt(lit.Pos())
	shadowed := declaredNames(lit)
	inherited := map[string]string{}
	for name := range outerNames(sc.checkers, lit, shadowed) {
		inherited[name] = fmt.Sprintf(
			"asserts through %s, a *qt.C declared outside this subtest, so the failure is reported against the parent test; open the closure with its own c := qt.New(t)",
			name,
		)
	}
	for name := range outerNames(sc.tbs, lit, shadowed) {
		inherited[name] = fmt.Sprintf(
			"uses %s, a testing.TB from the enclosing scope, so anything built from it belongs to the parent test; use the testing.TB this closure was handed",
			name,
		)
	}

	var findings []Finding
	for _, use := range firstUses(lit, inherited) {
		findings = append(findings, a.finding(use.Pos(), RuleBorrowedChecker, inherited[use.Name]))
	}

	return findings
}

// subtestClosure returns the callback of a subtest-shaped call whose parameter
// list makes it a testing.TB consumer.
//
// (*testing.T).Run, (*testing.B).Run and (*testing.F).Fuzz are the three ways
// this repository enters a child test. R2 covers the (*qt.C).Run spelling; this
// is deliberately not a check on the receiver, because a receiver named `sub` or
// reached through a field is the same subtest.
func (a *analysis) subtestClosure(call *ast.CallExpr) (*ast.FuncLit, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return nil, false
	}

	var index int
	switch {
	case sel.Sel.Name == "Run" && len(call.Args) >= 2:
		index = 1
	case sel.Sel.Name == "Fuzz" && len(call.Args) == 1:
		index = 0
	default:
		return nil, false
	}

	lit, ok := a.callbackLiteral(call.Args[index])
	if !ok || !a.signatureTakesTB(lit.Type) {
		return nil, false
	}

	return lit, true
}

// callbackLiteral resolves a callback argument to the function literal it
// denotes: the literal itself when it is written inline, or the literal a name
// in scope was bound to.
//
// A callback bound to a name is the same object graph as an inline one.
// `callback := func(t *testing.T) { c.Assert(...) }` followed by
// `t.Run(name, callback)` produces exactly the parent-FailNow failure R3 exists
// to prevent, and reading only the inline spelling let it through.
func (a *analysis) callbackLiteral(expr ast.Expr) (*ast.FuncLit, bool) {
	if lit, ok := expr.(*ast.FuncLit); ok {
		return lit, true
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return nil, false
	}
	decl, ok := a.names.lookup(ident.Name, ident.Pos())
	if !ok || decl.lit == nil {
		return nil, false
	}

	return decl.lit, true
}

// outerNames reduces bindings to the names declared outside lit and not rebound
// inside it.
func outerNames(bindings []binding, lit *ast.FuncLit, shadowed map[string]bool) map[string]bool {
	names := map[string]bool{}
	for _, b := range bindings {
		if b.pos >= lit.Pos() && b.pos < lit.End() {
			continue
		}
		if shadowed[b.name] {
			continue
		}
		names[b.name] = true
	}

	return names
}

// firstUses finds the first reference to each wanted name inside lit, in source
// order.
func firstUses(lit *ast.FuncLit, wanted map[string]string) []*ast.Ident {
	spelled := nonReferences(lit.Body)
	found := map[string]*ast.Ident{}

	ast.Inspect(lit.Body, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if !ok || wanted[ident.Name] == "" || spelled[ident.Pos()] {
			return true
		}
		if _, seen := found[ident.Name]; seen {
			return true
		}
		found[ident.Name] = ident
		return true
	})

	uses := make([]*ast.Ident, 0, len(found))
	for _, ident := range found {
		uses = append(uses, ident)
	}
	sort.Slice(uses, func(i, j int) bool { return uses[i].Pos() < uses[j].Pos() })

	return uses
}

// nonReferences collects the positions of identifiers that are spelled like a
// variable but do not read one.
//
// The selector half of x.c, a name in a field or parameter declaration, a bare
// key in a composite literal and a loop label are all the identifier `c` in the
// syntax tree and none of them is the checker. Missing one of these would report
// a struct field named c as a borrowed checker.
func nonReferences(body *ast.BlockStmt) map[token.Pos]bool {
	spelled := map[token.Pos]bool{}

	mark := func(expr ast.Expr) {
		ident, ok := expr.(*ast.Ident)
		if !ok {
			return
		}
		spelled[ident.Pos()] = true
	}

	ast.Inspect(body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			mark(typed.Sel)
		case *ast.KeyValueExpr:
			mark(typed.Key)
		case *ast.LabeledStmt:
			mark(typed.Label)
		case *ast.BranchStmt:
			if typed.Label != nil {
				mark(typed.Label)
			}
		case *ast.Field:
			for _, name := range typed.Names {
				mark(name)
			}
		}
		return true
	})

	return spelled
}

// declaredNames collects every name lit binds anywhere inside itself, including
// its own parameters and the bindings of nested closures.
//
// It is deliberately indiscriminate about what the name is bound to. The
// question R3 asks is whether an identifier reaches out of the closure, and any
// declaration of that name inside it means the answer is no.
func declaredNames(lit *ast.FuncLit) map[string]bool {
	names := map[string]bool{}

	record := func(exprs []ast.Expr) {
		for _, expr := range exprs {
			ident, ok := expr.(*ast.Ident)
			if !ok {
				continue
			}
			names[ident.Name] = true
		}
	}

	recordIdents := func(idents []*ast.Ident) {
		for _, ident := range idents {
			names[ident.Name] = true
		}
	}

	recordFields(lit.Type, recordIdents)

	ast.Inspect(lit.Body, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.AssignStmt:
			if typed.Tok == token.DEFINE {
				record(typed.Lhs)
			}
		case *ast.ValueSpec:
			recordIdents(typed.Names)
		case *ast.TypeSpec:
			recordIdents([]*ast.Ident{typed.Name})
		case *ast.RangeStmt:
			if typed.Tok == token.DEFINE {
				record([]ast.Expr{typed.Key, typed.Value})
			}
		case *ast.FuncLit:
			recordFields(typed.Type, recordIdents)
		}
		return true
	})

	return names
}

// recordFields feeds every parameter and result name of a signature to record.
func recordFields(sig *ast.FuncType, record func([]*ast.Ident)) {
	if sig == nil {
		return
	}
	for _, list := range []*ast.FieldList{sig.Params, sig.Results} {
		if list == nil {
			continue
		}
		for _, field := range list.List {
			record(field.Names)
		}
	}
}

// collectBindings gathers every *qt.C and testing.TB name bound anywhere under
// node, with the position of the declaration.
func (a *analysis) collectBindings(node ast.Node) (checkers, tbs []binding) {
	appendField := func(field *ast.Field) {
		for _, name := range field.Names {
			if a.isCheckerType(field.Type) {
				checkers = append(checkers, binding{name: name.Name, pos: name.Pos()})
			}
			if a.isTBType(field.Type) {
				tbs = append(tbs, binding{name: name.Name, pos: name.Pos()})
			}
		}
	}

	appendSignature := func(sig *ast.FuncType) {
		if sig == nil || sig.Params == nil {
			return
		}
		for _, field := range sig.Params.List {
			appendField(field)
		}
	}

	ast.Inspect(node, func(n ast.Node) bool {
		switch typed := n.(type) {
		case *ast.FuncDecl:
			appendSignature(typed.Type)
		case *ast.FuncLit:
			appendSignature(typed.Type)
		case *ast.AssignStmt:
			checkers = append(checkers, a.constructedCheckers(typed)...)
		case *ast.ValueSpec:
			checkers = append(checkers, a.specCheckers(typed)...)
			tbs = append(tbs, a.specTBs(typed)...)
		}
		return true
	})

	return checkers, tbs
}

// constructedCheckers reports the names a `c := qt.New(t)` statement binds.
func (a *analysis) constructedCheckers(assign *ast.AssignStmt) []binding {
	if assign.Tok != token.DEFINE || len(assign.Lhs) != len(assign.Rhs) {
		return nil
	}

	var out []binding
	for i, lhs := range assign.Lhs {
		ident, ok := lhs.(*ast.Ident)
		if !ok || !a.isCheckerConstructor(assign.Rhs[i]) {
			continue
		}
		out = append(out, binding{name: ident.Name, pos: ident.Pos()})
	}

	return out
}

// specCheckers reports the names a var declaration binds to a *qt.C, whether the
// type is written out or inferred from qt.New.
func (a *analysis) specCheckers(spec *ast.ValueSpec) []binding {
	var out []binding
	for i, name := range spec.Names {
		typed := a.isCheckerType(spec.Type)
		constructed := i < len(spec.Values) && a.isCheckerConstructor(spec.Values[i])
		if !typed && !constructed {
			continue
		}
		out = append(out, binding{name: name.Name, pos: name.Pos()})
	}

	return out
}

// specTBs reports the names a var declaration binds to a testing.TB.
func (a *analysis) specTBs(spec *ast.ValueSpec) []binding {
	var out []binding
	for _, name := range spec.Names {
		if !a.isTBType(spec.Type) {
			continue
		}
		out = append(out, binding{name: name.Name, pos: name.Pos()})
	}

	return out
}

// signatureTakesChecker reports whether a signature takes a *qt.C in any
// position.
func (a *analysis) signatureTakesChecker(sig *ast.FuncType) bool {
	return signatureTakes(sig, a.isCheckerType)
}

// signatureTakesTB reports whether a signature takes a testing.TB in any
// position.
func (a *analysis) signatureTakesTB(sig *ast.FuncType) bool {
	return signatureTakes(sig, a.isTBType)
}

// signatureTakes reports whether any parameter of sig satisfies match.
func signatureTakes(sig *ast.FuncType, match func(ast.Expr) bool) bool {
	if sig == nil || sig.Params == nil {
		return false
	}
	for _, field := range sig.Params.List {
		if match(field.Type) {
			return true
		}
	}

	return false
}

// isCheckerType reports whether expr is written *qt.C, with qt resolving to this
// file's quicktest import.
func (a *analysis) isCheckerType(expr ast.Expr) bool {
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}

	return a.isQuicktest(star.X, "C")
}

// isTBType reports whether expr is written *testing.T, *testing.B, *testing.F or
// testing.TB, with testing resolving to this file's import of it.
func (a *analysis) isTBType(expr ast.Expr) bool {
	if a.isTesting(expr, "TB") {
		return true
	}
	star, ok := expr.(*ast.StarExpr)
	if !ok {
		return false
	}
	for _, name := range []string{"T", "B", "F"} {
		if a.isTesting(star.X, name) {
			return true
		}
	}

	return false
}

// isCheckerConstructor reports whether expr is a qt.New(...) call.
func (a *analysis) isCheckerConstructor(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}

	return a.isQuicktest(call.Fun, "New")
}

// isCheckerExpr reports whether expr denotes a *qt.C: a qt.New call, or a name
// bound to one.
func (a *analysis) isCheckerExpr(expr ast.Expr, checkers []binding) bool {
	if a.isCheckerConstructor(expr) {
		return true
	}
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return false
	}
	for _, b := range checkers {
		if b.name == ident.Name {
			return true
		}
	}

	return false
}

// receiverName names the receiver in a message so the reader can find the call,
// falling back to a placeholder when the receiver is not a bare identifier.
func receiverName(expr ast.Expr) string {
	ident, ok := expr.(*ast.Ident)
	if !ok {
		return "<expr>"
	}
	return ident.Name
}

// finding resolves a token position into a reportable line and column.
func (a *analysis) finding(pos token.Pos, rule Rule, message string) Finding {
	position := a.fset.Position(pos)
	return Finding{
		Path:    a.path,
		Line:    position.Line,
		Col:     position.Column,
		Rule:    rule,
		Message: message,
	}
}
