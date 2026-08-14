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

// RequiredImportAlias is the only name quicktest may be imported under. R1 and
// R2 both key on this identifier, so an import under any other name would make
// the gate quiet rather than wrong, which is worse.
const RequiredImportAlias = "qt"

// Rule identifies which prohibition a Finding reports.
type Rule string

const (
	// RuleImportAlias reports quicktest imported under a name other than qt.
	RuleImportAlias Rule = "R0"
	// RulePackageAssert reports a use of the package-level qt.Assert or qt.Check.
	RulePackageAssert Rule = "R1"
	// RuleCheckerSubtest reports a subtest whose closure takes a *qt.C, which is
	// the shape (*qt.C).Run produces.
	RuleCheckerSubtest Rule = "R2"
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

	var findings []Finding
	findings = append(findings, importAliasFindings(fset, path, file)...)
	findings = append(findings, expressionFindings(fset, path, file)...)

	sort.SliceStable(findings, func(i, j int) bool {
		if findings[i].Line != findings[j].Line {
			return findings[i].Line < findings[j].Line
		}
		return findings[i].Col < findings[j].Col
	})

	return findings, nil
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

// importAliasFindings reports quicktest imported under any name but qt.
//
// The package clause of github.com/frankban/quicktest is `package quicktest`, so
// an unaliased import binds `quicktest` and is a violation too. importas in
// .golangci.yml already requires the alias; duplicating it here is deliberate,
// because without it a single renamed import would silently disable R1 and R2
// for a whole file.
func importAliasFindings(fset *token.FileSet, path string, file *ast.File) []Finding {
	var findings []Finding

	for _, spec := range file.Imports {
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
		findings = append(findings, finding(fset, path, spec.Pos(), RuleImportAlias, fmt.Sprintf(
			"import %q with %s; it must be imported as %q, because the assertion and subtest rules key on that identifier",
			QuicktestImportPath, spelled, RequiredImportAlias,
		)))
	}

	return findings
}

// expressionFindings walks the whole file once and reports R1 and R2.
func expressionFindings(fset *token.FileSet, path string, file *ast.File) []Finding {
	var findings []Finding

	ast.Inspect(file, func(node ast.Node) bool {
		switch typed := node.(type) {
		case *ast.SelectorExpr:
			if f, ok := packageAssertFinding(fset, path, typed); ok {
				findings = append(findings, f)
			}
		case *ast.CallExpr:
			if f, ok := checkerSubtestFinding(fset, path, typed); ok {
				findings = append(findings, f)
			}
		}
		return true
	})

	return findings
}

// packageAssertFinding reports qt.Assert and qt.Check.
//
// It matches the selector rather than the call, so `assert := qt.Assert` cannot
// launder the prohibition through a variable. The receiver check is on the
// identifier alone, never on the argument list: qt.Assert(subT, ...) and
// qt.Assert(c.TB, ...) are the same violation as qt.Assert(t, ...), and a rule
// keyed on a first argument named `t` would miss both.
func packageAssertFinding(fset *token.FileSet, path string, sel *ast.SelectorExpr) (Finding, bool) {
	pkg, ok := sel.X.(*ast.Ident)
	if !ok || pkg.Name != RequiredImportAlias {
		return Finding{}, false
	}
	if sel.Sel.Name != "Assert" && sel.Sel.Name != "Check" {
		return Finding{}, false
	}

	return finding(fset, path, sel.Pos(), RulePackageAssert, fmt.Sprintf(
		"qt.%s is forbidden; create a checker from the testing.TB that owns this scope (c := qt.New(t)) and call c.%s(...) without the TB argument",
		sel.Sel.Name, sel.Sel.Name,
	)), true
}

// checkerSubtestFinding reports a subtest whose closure receives a *qt.C, which
// is exactly the shape (*qt.C).Run produces and nothing else does.
//
// The receiver is deliberately not inspected. Keying on a receiver named `c`
// would miss outer.Run(...) and qt.New(t).Run(...), where the receiver is a
// different identifier or not an identifier at all. Keying on the *qt.C
// parameter instead is what leaves the 1328 unrelated .Run( calls in this tree
// -- t.Run, b.Run, m.Run, cmd.Run, testcontour.Run and the rest -- untouched,
// since none of their closures take a *qt.C.
func checkerSubtestFinding(fset *token.FileSet, path string, call *ast.CallExpr) (Finding, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Run" || len(call.Args) < 2 {
		return Finding{}, false
	}
	lit, ok := call.Args[1].(*ast.FuncLit)
	if !ok || !hasCheckerParam(lit) {
		return Finding{}, false
	}

	return finding(fset, path, call.Pos(), RuleCheckerSubtest, fmt.Sprintf(
		"%s.Run with a func(*qt.C) closure is forbidden; write t.Run(name, func(t *testing.T) { c := qt.New(t); ... }) so the subtest checker is a visible declaration instead of a shadowing parameter",
		receiverName(sel.X),
	)), true
}

// hasCheckerParam reports whether the literal takes a *qt.C in any position.
func hasCheckerParam(lit *ast.FuncLit) bool {
	if lit.Type == nil || lit.Type.Params == nil {
		return false
	}
	for _, field := range lit.Type.Params.List {
		star, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		sel, ok := star.X.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "C" {
			continue
		}
		pkg, ok := sel.X.(*ast.Ident)
		if ok && pkg.Name == RequiredImportAlias {
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
func finding(fset *token.FileSet, path string, pos token.Pos, rule Rule, message string) Finding {
	position := fset.Position(pos)
	return Finding{
		Path:    path,
		Line:    position.Line,
		Col:     position.Column,
		Rule:    rule,
		Message: message,
	}
}
