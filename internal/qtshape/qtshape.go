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
	// RuleCheckerSubtest reports a subtest driven by a *qt.C: the callback
	// consumes one, or the receiver is one, or the checker's Run is referenced
	// without being called there and then.
	RuleCheckerSubtest Rule = "R2"
	// RuleBorrowedChecker reports a t.Run, b.Run or f.Fuzz closure whose checker
	// is not the one it was handed: it reaches back out to the enclosing scope's
	// checker or testing.TB, or it builds a checker from a testing.TB that is
	// not its own parameter.
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

	a := newAnalysis(fset, path, src, file)

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
	src   []byte
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
}

// newAnalysis resolves the file's imports, builds its lexical name table and
// marks which of its names denote a checker or a testing.TB.
func newAnalysis(fset *token.FileSet, path string, src []byte, file *ast.File) *analysis {
	a := &analysis{fset: fset, path: path, src: src, file: file, names: newBindings(file)}
	a.quicktest, a.qtName = importedAs(file, QuicktestImportPath)
	a.testing, a.tbName = importedAs(file, testingImportPath)
	a.classifyBindings()

	return a
}

// classifyBindings marks every declaration that binds a *qt.C or a testing.TB.
//
// It is a second pass over the name table rather than part of building it,
// because "is this type written *qt.C" is itself a name resolution: qt means
// quicktest only where this file's import of it is still what that name
// denotes, and answering that needs the table already standing.
func (a *analysis) classifyBindings() {
	a.names.each(func(d *nameDecl) {
		d.checker = a.isCheckerType(d.typ) || a.isCheckerConstructor(d.value)
		d.tb = a.isTBType(d.typ)
	})
	a.propagateAliases()
}

// propagateAliases carries checker and testing.TB identity across a declaration
// that copies another name: `alias := c` binds the same checker, and
// `parent := t` the same testing.TB.
//
// Neither spelling has a written type and neither initializer is a qt.New call,
// so the pass above sees nothing in either and every rule downstream then reads
// the alias as an ordinary value. That is a hole in R2 and R3 at once:
// `alias.Run(name, callback)` is the prohibited subtest, and
// `t.Run(name, func(t *testing.T) { alias.Assert(...) })` is the parent-FailNow
// failure R3 exists to prevent, with nothing in the syntax to distinguish it
// from the spelling that is caught.
//
// The source is resolved at the position the initializer is written, so an
// alias of an unrelated `c` in a nearer scope is that value and not the
// checker. Only a bare identifier is followed: `tb := f.tb` reaches through a
// field this package cannot resolve, and R3's foreign-TB check already covers
// the qt.New spelling of that.
func (a *analysis) propagateAliases() {
	// A chain is resolved by repetition because the table is a map and hands
	// out declarations in no order: `second := first` may be classified before
	// `first := c`. Each pass only ever turns a flag on, so the loop settles
	// after at most one pass per link.
	for {
		changed := false
		a.names.each(func(d *nameDecl) {
			source, ok := a.aliasSource(d)
			if !ok {
				return
			}
			gained := (source.checker && !d.checker) || (source.tb && !d.tb)
			d.checker = d.checker || source.checker
			d.tb = d.tb || source.tb
			changed = changed || gained
		})
		if !changed {
			return
		}
	}
}

// aliasSource resolves the declaration an initializer names when the
// initializer is a bare identifier, at the position that identifier is written.
func (a *analysis) aliasSource(d *nameDecl) (*nameDecl, bool) {
	ident, ok := ast.Unparen(d.value).(*ast.Ident)
	if !ok {
		return nil, false
	}
	source, ok := a.names.lookup(ident.Name, ident.Pos())
	if !ok || source == d {
		return nil, false
	}

	return source, true
}

// source is the exact text an expression was written as, so a message can name
// what the gate saw rather than describing it.
func (a *analysis) source(expr ast.Expr) string {
	start := a.fset.Position(expr.Pos()).Offset
	end := a.fset.Position(expr.End()).Offset
	if start < 0 || end > len(a.src) || start >= end {
		return "<expr>"
	}

	return string(a.src[start:end])
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
	sel, ok := ast.Unparen(expr).(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != name {
		return false
	}
	ident, ok := ast.Unparen(sel.X).(*ast.Ident)
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

// subtestFindings reports R2 and R3.
//
// Nothing here is keyed on a set of names. Every identifier that decides a rule
// is resolved through the file's lexical name table at the position it is
// written, so an inner `c := runner{}` is a runner and not the checker the
// enclosing function happens to have called `c`, and a `c := qt.New(t)` written
// halfway down a closure does not retroactively excuse the assertions above it.
func (a *analysis) subtestFindings() []Finding {
	var findings []Finding

	ast.Inspect(a.file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.CallExpr:
			if f, ok := a.checkerSubtestFinding(typed); ok {
				findings = append(findings, f)
			}
			findings = append(findings, a.borrowedCheckerFindings(typed)...)
		case *ast.SelectorExpr:
			if f, ok := a.checkerRunValueFinding(typed); ok {
				findings = append(findings, f)
			}
		}
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
func (a *analysis) checkerSubtestFinding(call *ast.CallExpr) (Finding, bool) {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Run" || len(call.Args) < 2 {
		return Finding{}, false
	}
	reason, ok := a.checkerSubtestReason(call.Args[1], sel.X)
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
func (a *analysis) checkerSubtestReason(callback, receiver ast.Expr) (string, bool) {
	callback = ast.Unparen(callback)
	if lit, ok := callback.(*ast.FuncLit); ok && a.signatureTakesChecker(lit.Type) {
		return "with a func(*qt.C) closure is forbidden", true
	}
	if ident, ok := callback.(*ast.Ident); ok && a.namesCheckerCallback(ident) {
		return fmt.Sprintf("with the func(*qt.C) callback %s is forbidden", ident.Name), true
	}
	if a.isCheckerExpr(receiver) {
		return "is a (*qt.C).Run subtest and is forbidden", true
	}

	return "", false
}

// checkerRunValueFinding reports a reference to (*qt.C).Run that is not itself
// the call, so the prohibition cannot be laundered through a variable.
//
// R1 has always matched the selector rather than the call, which is why
// `assert := qt.Assert` was never a way around it. R2 matched the call, so
// `run := c.Run` followed by `run(name, callback)` ran exactly the forbidden
// subtest and the gate said nothing. What is prohibited is the method, so the
// reference to it is the finding, whatever is done with the value afterwards.
//
// A called `c.Run(name, callback)` lands on the same position as the call-shaped
// finding above and is dropped by dedupe, which keeps the more specific message.
func (a *analysis) checkerRunValueFinding(sel *ast.SelectorExpr) (Finding, bool) {
	if sel.Sel.Name != "Run" || !a.isCheckerExpr(sel.X) {
		return Finding{}, false
	}

	return a.finding(sel.Pos(), RuleCheckerSubtest, fmt.Sprintf(
		"%s.Run is a (*qt.C).Run subtest and is forbidden; write t.Run(name, func(t *testing.T) { c := qt.New(t); ... }) so the subtest checker is a visible declaration instead of a shadowing parameter",
		receiverName(sel.X),
	)), true
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

// borrowedCheckerFindings reports a subtest closure whose checker is not the one
// it was handed: either it reads a *qt.C or a testing.TB that belongs to the
// scope outside it, or it builds a checker from a testing.TB that is not its own
// parameter.
//
// This is the half a callback-shape rule cannot see. `t.Run(name, func(t
// *testing.T) { c.Assert(...) })` has the required signature and the required
// receiver and is still wrong: `c` is the parent's checker, so the assertion
// fails the parent test from the subtest's goroutine instead of failing the
// subtest. `func(subT *testing.T) { c := qt.New(t) }` is the same defect spelled
// with the parent's TB, and `qt.New(parent)` or `qt.New(fixture.tb)` is the same
// defect again with the parent's TB reached through a name or a field the
// enclosing-scope walk cannot classify.
func (a *analysis) borrowedCheckerFindings(call *ast.CallExpr) []Finding {
	lit, ok := a.subtestClosure(call)
	if !ok {
		return nil
	}

	closure := span{start: lit.Pos(), end: lit.End()}

	// The foreign-checker walk goes first so that where both walks land on the
	// same identifier -- `parent := t` read as the argument of qt.New -- dedupe
	// keeps the message that names the qt.New, which is the one that says what
	// to write instead.
	var findings []Finding
	findings = append(findings, a.foreignCheckerFindings(lit, closure)...)
	findings = append(findings, a.inheritedFindings(lit, closure)...)

	return findings
}

// inheritedFindings reports the first read of each name that resolves to a
// checker or a testing.TB declared outside the closure.
//
// Resolution is what makes the answer positional. A name the closure declares
// itself is its own from the end of that declaration onwards and not before, so
// `func(t *testing.T) { c.Assert(...); c := qt.New(t) }` still reports the first
// line: at that point `c` is unambiguously the parent's. Reducing the closure to
// a set of the names it declares somewhere loses that finding, and reports the
// unrelated `c` an enclosing block declared as an int as a borrowed checker.
func (a *analysis) inheritedFindings(lit *ast.FuncLit, closure span) []Finding {
	var findings []Finding

	reported := map[string]bool{}
	for _, ident := range references(lit.Body) {
		decl, ok := a.names.lookup(ident.Name, ident.Pos())
		if !ok || reported[ident.Name] || decl.declaredWithin(closure) {
			continue
		}
		message, ok := borrowedMessage(ident.Name, decl)
		if !ok {
			continue
		}
		reported[ident.Name] = true
		findings = append(findings, a.finding(ident.Pos(), RuleBorrowedChecker, message))
	}

	return findings
}

// borrowedMessage says which of the two borrowed kinds a declaration is.
func borrowedMessage(name string, decl *nameDecl) (string, bool) {
	if decl.checker {
		return fmt.Sprintf(
			"asserts through %s, a *qt.C declared outside this subtest, so the failure is reported against the parent test; open the closure with its own c := qt.New(t)",
			name,
		), true
	}
	if decl.tb {
		return fmt.Sprintf(
			"uses %s, a testing.TB from the enclosing scope, so anything built from it belongs to the parent test; use the testing.TB this closure was handed",
			name,
		), true
	}

	return "", false
}

// foreignCheckerFindings reports a qt.New inside a subtest closure whose
// argument is not a testing.TB the closure owns.
//
// The rule AGENTS.md states is that the checker must come from the testing.TB
// the closure was handed, and the enclosing-scope walk above only reaches that
// when the parent's TB is a name it could classify. `parent := t` binds a TB
// through an initializer, `fixture.tb` reaches one through a field, and both
// produce the parent-FailNow failure while satisfying every other check. Asking
// instead that the argument resolve to a declaration inside the closure covers
// the whole shape: the closure's own parameter qualifies, a TB a nested helper
// closure was handed qualifies, and nothing from outside does.
func (a *analysis) foreignCheckerFindings(lit *ast.FuncLit, closure span) []Finding {
	var findings []Finding

	ast.Inspect(lit.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok || !a.isCheckerConstructor(call) || len(call.Args) != 1 {
			return true
		}
		if a.ownedTB(call.Args[0], closure) {
			return true
		}
		findings = append(findings, a.finding(call.Args[0].Pos(), RuleBorrowedChecker, fmt.Sprintf(
			"builds its checker from %s, which is not the testing.TB this subtest closure was handed, so the failure is reported against the parent test; write c := qt.New(t) from the closure's own parameter",
			a.source(call.Args[0]),
		)))
		return true
	})

	return findings
}

// ownedTB reports whether expr denotes a testing.TB the closure was handed.
//
// Being declared inside the closure does not prove that. `parent := fixture.tb`
// is a local declaration holding the enclosing test's TB, so `qt.New(parent)`
// produces exactly the parent-FailNow failure this rule exists to prevent while
// satisfying a check that only asks where the name was written. The
// enclosing-scope walk does not reach it either: what it borrows is `fixture`,
// which is neither a checker nor a testing.TB.
//
// What is asked is where the value came from. A parameter is the only thing a
// closure is handed, so the argument must be a parameter of this closure or of
// a function literal written inside it, or a name whose initializer chain leads
// back to one. Everything else -- a field, a call result, a range variable, a
// name copied from outside -- is a testing.TB from somewhere this closure does
// not own.
//
// The walk terminates because every step moves to a declaration written
// strictly earlier: an initializer is written before the point at which the
// name it initializes starts to denote that declaration, and a name resolves
// only to a declaration already in scope where it is read.
func (a *analysis) ownedTB(expr ast.Expr, closure span) bool {
	for {
		ident, ok := ast.Unparen(expr).(*ast.Ident)
		if !ok {
			return false
		}
		decl, ok := a.names.lookup(ident.Name, ident.Pos())
		if !ok || !decl.declaredWithin(closure) {
			return false
		}
		if decl.handed {
			return true
		}
		if decl.value == nil {
			return false
		}
		expr = decl.value
	}
}

// subtestClosure returns the callback of a subtest-shaped call whose parameter
// list makes it a testing.TB consumer.
//
// (*testing.T).Run, (*testing.B).Run and (*testing.F).Fuzz are the three ways
// this repository enters a child test. R2 covers the (*qt.C).Run spelling; this
// is deliberately not a check on the receiver, because a receiver named `sub` or
// reached through a field is the same subtest.
func (a *analysis) subtestClosure(call *ast.CallExpr) (*ast.FuncLit, bool) {
	sel, ok := ast.Unparen(call.Fun).(*ast.SelectorExpr)
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
	expr = ast.Unparen(expr)
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

// references returns, in source order, the identifiers inside a closure body
// that actually read a name.
//
// The selector half of x.c, a name in a field or parameter declaration, a bare
// key in a composite literal, a loop label and the left side of every
// declaration are all the identifier `c` in the syntax tree and none of them
// reads one. A declaration's own left side matters most: `c := qt.New(t)` inside
// a subtest resolves, at the position it is written, to whatever the enclosing
// scope bound, so counting it would report every conforming subtest in the tree
// as borrowing the parent's checker.
func references(body *ast.BlockStmt) []*ast.Ident {
	skip := nonReferences(body)

	var idents []*ast.Ident
	ast.Inspect(body, func(node ast.Node) bool {
		ident, ok := node.(*ast.Ident)
		if !ok || skip[ident.Pos()] {
			return true
		}
		idents = append(idents, ident)
		return true
	})

	sort.SliceStable(idents, func(i, j int) bool { return idents[i].Pos() < idents[j].Pos() })

	return idents
}

// nonReferences collects the positions of identifiers that are spelled like a
// variable but do not read one.
func nonReferences(body *ast.BlockStmt) map[token.Pos]bool {
	spelled := map[token.Pos]bool{}

	mark := func(expr ast.Expr) {
		ident, ok := expr.(*ast.Ident)
		if !ok {
			return
		}
		spelled[ident.Pos()] = true
	}

	markAll := func(exprs ...ast.Expr) {
		for _, expr := range exprs {
			mark(expr)
		}
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
		case *ast.AssignStmt:
			if typed.Tok == token.DEFINE {
				markAll(typed.Lhs...)
			}
		case *ast.ValueSpec:
			for _, name := range typed.Names {
				mark(name)
			}
		case *ast.TypeSpec:
			mark(typed.Name)
		case *ast.RangeStmt:
			if typed.Tok == token.DEFINE {
				markAll(typed.Key, typed.Value)
			}
		}
		return true
	})

	return spelled
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
	star, ok := ast.Unparen(expr).(*ast.StarExpr)
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
	star, ok := ast.Unparen(expr).(*ast.StarExpr)
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
	call, ok := ast.Unparen(expr).(*ast.CallExpr)
	if !ok {
		return false
	}

	return a.isQuicktest(call.Fun, "New")
}

// isCheckerExpr reports whether expr denotes a *qt.C: a qt.New call, the type
// itself as written in a method expression, or a name that resolves to a checker
// at the position it is written.
//
// The resolution is the point. Matching the name against every checker the
// enclosing function declares reports `c := runner{}; c.Run("x", f)` in an inner
// block as a quicktest subtest, because the function also has a `c` that is one.
// A repository-wide gate that rejects that code rejects correct code.
//
// Parentheses are a node in the tree, not whitespace, so `(c).Run` handed this
// function an *ast.ParenExpr and the name branch below saw nothing to resolve.
// Every classification in this package unwraps them for that reason: one pair of
// parentheses may not be the difference between a violation and a clean gate.
func (a *analysis) isCheckerExpr(expr ast.Expr) bool {
	if a.isCheckerConstructor(expr) || a.isCheckerType(expr) {
		return true
	}
	ident, ok := ast.Unparen(expr).(*ast.Ident)
	if !ok {
		return false
	}
	decl, ok := a.names.lookup(ident.Name, ident.Pos())

	return ok && decl.checker
}

// receiverName names the receiver in a message so the reader can find the call,
// falling back to a placeholder when the receiver is not a bare identifier.
func receiverName(expr ast.Expr) string {
	ident, ok := ast.Unparen(expr).(*ast.Ident)
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
