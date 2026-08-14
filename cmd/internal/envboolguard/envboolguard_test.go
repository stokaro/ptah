package envboolguard_test

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	// Importing the owners is what populates the registry the enumeration check
	// reads back. A package left out here contributes no declarations, so its
	// variables would look unclassified rather than boolean -- which is the
	// direction that fails loudly instead of passing quietly.
	_ "go.5x5.cz/ptah/cmd/atlas"
	_ "go.5x5.cz/ptah/cmd/internal/editor"
	_ "go.5x5.cz/ptah/internal/atlasfilter"
	_ "go.5x5.cz/ptah/internal/atlashclrender"
	_ "go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/envbool"
	_ "go.5x5.cz/ptah/internal/migrationintegrity"
	_ "go.5x5.cz/ptah/internal/reservedrole"
	_ "go.5x5.cz/ptah/internal/rolescope"
	_ "go.5x5.cz/ptah/internal/sqlitevirtual"
	_ "go.5x5.cz/ptah/migration/migrator"
)

// nonBooleanPtahVars is the ONLY hand-written list in this file: the `PTAH_*`
// names that carry something other than a boolean.
//
// A new entry here is a deliberate statement, which is the point. A boolean
// variable added without going through the shared package appears in neither
// this list nor the registry and fails TestEveryPtahVariableIsClassified with
// its own name in the message.
//
// Only names the scan can actually see belong here. Test-only probe names do
// not, because the scan skips `_test.go` files: an entry for one would be a
// claim about the tree that nothing checks.
var nonBooleanPtahVars = []string{
	"PTAH_CURRENT_VERSION", // migration version, passed to preflight hooks
	"PTAH_DB_URL",          // database URL
	"PTAH_DIALECT",         // dialect name
	"PTAH_DIR",             // migration directory URL
	"PTAH_FORMAT",          // Go template
	"PTAH_LOG_FORMAT",      // log format name
	"PTAH_MIGRATIONS_DIR",  // native migration directory path
	"PTAH_PLAN",            // plan name, an Atlas capability Ptah refuses
	"PTAH_TARGET_VERSION",  // migration version, passed to preflight hooks
	"PTAH_TO_TAG",          // tag name, an Atlas capability Ptah refuses
	"PTAH_VAR",             // repeatable name=value assignment
}

// ptahVarPattern finds a variable name anywhere inside a string literal, so
// `"PTAH_DB_URL="` and "`PTAH_ATLAS_LINT_ALL_VERSIONS=1`" are both recognized
// as naming their variable. A bare `"PTAH_"` prefix used for name construction
// matches nothing, which is correct: it names no variable.
var ptahVarPattern = regexp.MustCompile(`PTAH_[A-Z0-9]+[A-Z0-9_]*`)

// booleanLiterals are the values a hand-rolled truthiness test compares against.
// `""` is deliberately absent: comparing an environment value with the empty
// string is a presence test, not a boolean parse, and several non-boolean
// variables legitimately do it.
var booleanLiterals = []string{
	"0", "1", "f", "F", "false", "False", "FALSE", "t", "T", "true", "True", "TRUE",
}

// TestNoDirectBooleanEnvironmentParsing is the shape guard.
//
// A function that reads the environment AND parses a boolean is the pattern
// this change removed, whichever way the two are spelled -- nested in one
// expression, or split across a `LookupEnv` and a later `ParseBool` the way the
// three already-strict readers spelled it. Both forms are one function, so one
// rule catches both.
func TestNoDirectBooleanEnvironmentParsing(t *testing.T) {
	c := qt.New(t)
	root := repositoryRoot(c)

	var violations []string
	for _, path := range goSourceFiles(c, root) {
		violations = append(violations, scanFile(c, root, path)...)
	}

	c.Assert(violations, qt.HasLen, 0, qt.Commentf(
		"route the read through go.5x5.cz/ptah/internal/envbool instead:\n%s",
		strings.Join(violations, "\n")))
}

// TestGuardCatchesTheShapesItClaimsTo is the guard's own control.
//
// A scanner that selected nothing would pass TestNoDirectBooleanEnvironmentParsing
// on any tree at all, including the one this change started from. Each fixture
// is a shape the guard has to reject, and the last one is a shape it must NOT
// reject -- a non-boolean variable compared with the empty string, which is how
// several string variables legitimately test for presence.
func TestGuardCatchesTheShapesItClaimsTo(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   int
	}{
		{
			name: "the nested form",
			source: `package p

import (
	"os"
	"strconv"
)

func Allowed() bool {
	v, err := strconv.ParseBool(os.Getenv("PTAH_ENVBOOLGUARD_PROBE"))
	return err == nil && v
}
`,
			want: 1,
		},
		{
			name: "the split form",
			source: `package p

import (
	"os"
	"strconv"
)

func Allowed() (bool, error) {
	value, ok := os.LookupEnv("PTAH_ENVBOOLGUARD_PROBE")
	if !ok {
		return false, nil
	}
	return strconv.ParseBool(value)
}
`,
			want: 1,
		},
		{
			name: "a manual truthiness test",
			source: `package p

import "os"

func Allowed() bool {
	return os.Getenv("PTAH_ENVBOOLGUARD_PROBE") == "1"
}
`,
			want: 1,
		},
		{
			// Two comparisons, two reports: the guard names every offending
			// expression rather than the first one, so a partial fix stays red.
			name: "a manual truthiness test through a local",
			source: `package p

import "os"

func Allowed() bool {
	value := os.Getenv("PTAH_ENVBOOLGUARD_PROBE")
	return value == "true" || value == "TRUE"
}
`,
			want: 2,
		},
		{
			// The shape the function-level rule alone would have missed, and the
			// one cmd/internal/cmdflags actually had: the read and the parse are
			// two functions apart.
			name: "the read and the parse in separate functions",
			source: `package p

import (
	"os"
	"strconv"
)

func value() (string, bool) {
	return os.LookupEnv("PTAH_ENVBOOLGUARD_PROBE")
}

func parse(raw string) (bool, error) {
	return strconv.ParseBool(raw)
}
`,
			want: 1,
		},
		{
			name: "a read of a variable this contract does not govern is left alone",
			source: `package p

import (
	"os"
	"strconv"
)

func Verbose() bool {
	v, err := strconv.ParseBool(os.Getenv("SHOW_DETAILS"))
	return err == nil && v
}
`,
			want: 0,
		},
		{
			name: "a presence test against the empty string is left alone",
			source: `package p

import "os"

func Configured() bool {
	return os.Getenv("PTAH_ENVBOOLGUARD_PROBE") != ""
}
`,
			want: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			path := filepath.Join(dir, "probe.go")
			writeFile(c, path, test.source)

			got := scanFile(c, dir, path)

			c.Assert(got, qt.HasLen, test.want, qt.Commentf("%v", got))
		})
	}
}

// TestEveryPtahVariableIsClassified is the enumeration guard.
//
// It derives the names from the source rather than trusting a list, because a
// list is exactly what goes stale the next time a variable is added. Every name
// the tree mentions has to be either a declared boolean or a written-down
// non-boolean; a name that is neither is a variable nobody decided about.
func TestEveryPtahVariableIsClassified(t *testing.T) {
	c := qt.New(t)

	mentioned := ptahVarNames(c, repositoryRoot(c))

	c.Assert(len(mentioned) > 10, qt.IsTrue, qt.Commentf(
		"found %d names; a scan that finds nothing is also green", len(mentioned)))
	unclassified := namesOutsideBothClassifications(mentioned)
	c.Assert(unclassified, qt.HasLen, 0, qt.Commentf(
		"declare each through go.5x5.cz/ptah/internal/envbool, or record it in"+
			" nonBooleanPtahVars: %v", unclassified))
}

// TestNoVariableIsClassifiedTwice keeps the two classifications from disagreeing.
// A name in both lists would make the enumeration guard pass whichever way the
// variable is actually read.
func TestNoVariableIsClassifiedTwice(t *testing.T) {
	c := qt.New(t)

	both := namesInBothClassifications()

	c.Assert(both, qt.HasLen, 0)
}

// TestEveryDeclaredVariableIsNamedAndDefaultedSafely pins two properties of the
// registry at once.
//
// The prefix is the namespace this contract governs. The default is the more
// interesting half: every boolean toggle in this tree opts IN to the more
// permissive side, so a typo lands on the strict default and fails CLOSED. The
// first variable to default true would invert that, and a typo on it would open
// the gate it guards. Reading the defaults back from the registry is what turns
// "we checked once" into a standing assertion.
func TestEveryDeclaredVariableIsNamedAndDefaultedSafely(t *testing.T) {
	c := qt.New(t)

	registered := envbool.Registered()

	c.Assert(len(registered) > 0, qt.IsTrue, qt.Commentf(
		"the registry is empty, so every assertion over it is vacuous"))
	for _, variable := range registered {
		t.Run(variable.Name(), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(strings.HasPrefix(variable.Name(), envbool.Prefix), qt.IsTrue)
			c.Assert(variable.Default(), qt.IsFalse, qt.Commentf(
				"a boolean PTAH_* variable defaulting to the permissive side turns a typo"+
					" into an open gate; see the reasoning in internal/envbool"))
		})
	}
}

// TestNoVariableIsDeclaredTwice keeps one name from being declared with two
// defaults in two packages, which would make the answer depend on which reader
// ran.
func TestNoVariableIsDeclaredTwice(t *testing.T) {
	c := qt.New(t)

	duplicated := duplicateRegisteredNames()

	c.Assert(duplicated, qt.HasLen, 0)
}

// registeredNames returns the declared variable names, sorted and deduplicated.
func registeredNames() []string {
	var names []string
	for _, variable := range envbool.Registered() {
		names = append(names, variable.Name())
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// duplicateRegisteredNames returns the names declared more than once.
func duplicateRegisteredNames() []string {
	seen := map[string]int{}
	for _, variable := range envbool.Registered() {
		seen[variable.Name()]++
	}
	var duplicated []string
	for name, count := range seen {
		if count > 1 {
			duplicated = append(duplicated, name)
		}
	}
	slices.Sort(duplicated)
	return duplicated
}

// namesOutsideBothClassifications returns the mentioned names that are neither
// declared through the shared package nor recorded as non-boolean.
func namesOutsideBothClassifications(mentioned []string) []string {
	declared := registeredNames()
	var unclassified []string
	for _, name := range mentioned {
		if slices.Contains(declared, name) || slices.Contains(nonBooleanPtahVars, name) {
			continue
		}
		unclassified = append(unclassified, name)
	}
	return unclassified
}

// namesInBothClassifications returns the names claimed by both classifications.
func namesInBothClassifications() []string {
	var both []string
	for _, name := range registeredNames() {
		if slices.Contains(nonBooleanPtahVars, name) {
			both = append(both, name)
		}
	}
	return both
}

// scanFile reports the guard violations in one Go source file, as
// "path:line: reason" strings.
func scanFile(c *qt.C, root, path string) []string {
	c.Helper()
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	c.Assert(err, qt.IsNil)

	relative, err := filepath.Rel(root, path)
	c.Assert(err, qt.IsNil)

	var violations []string
	parsedInAFunction := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		found := scanFunc(fset, relative, fn)
		parsedInAFunction = parsedInAFunction || slices.ContainsFunc(found, func(v string) bool {
			return strings.Contains(v, "parses a boolean itself")
		})
		violations = append(violations, found...)
	}

	// The file-level fallback exists because one shape survived the
	// function-level rule: cmd/internal/cmdflags read the value in `applyEnv`
	// and parsed it in `setEnvValue`, two functions apart. That is the same
	// defect split across a call, and a rule that stopped at the function
	// boundary would have let the next copy of it in.
	if !parsedInAFunction && fileReadsPtahEnv(file) && fileParsesBool(file) {
		violations = append(violations, fmt.Sprintf(
			"%s: reads a PTAH_ environment value and parses a boolean in the same file", relative))
	}
	return violations
}

// fileReadsPtahEnv reports whether anything in the file reads an environment
// variable this contract governs.
func fileReadsPtahEnv(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := selectorName(call.Fun)
		if (name == "os.Getenv" || name == "os.LookupEnv") && readsPtahVariable(call) {
			found = true
		}
		return true
	})
	return found
}

// fileParsesBool reports whether anything in the file calls strconv.ParseBool.
func fileParsesBool(file *ast.File) bool {
	found := false
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok && selectorName(call.Fun) == "strconv.ParseBool" {
			found = true
		}
		return true
	})
	return found
}

// scanFunc applies both shape rules to one function body.
//
// The unit is the function rather than the expression because the split form --
// `LookupEnv` here, `ParseBool` three lines down -- is the same defect as the
// nested one and has to fail the same way.
func scanFunc(fset *token.FileSet, path string, fn *ast.FuncDecl) []string {
	var (
		readsEnv     bool
		parsesBool   bool
		comparisons  []ast.Node
		envPositions []token.Pos
	)
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			name := selectorName(call.Fun)
			if (name == "os.Getenv" || name == "os.LookupEnv") && readsPtahVariable(call) {
				readsEnv = true
				envPositions = append(envPositions, call.Pos())
			}
			if name == "strconv.ParseBool" {
				parsesBool = true
			}
		}
		if binary, ok := node.(*ast.BinaryExpr); ok &&
			(binary.Op == token.EQL || binary.Op == token.NEQ) &&
			(isBooleanLiteral(binary.X) || isBooleanLiteral(binary.Y)) {
			comparisons = append(comparisons, binary)
		}
		return true
	})

	var violations []string
	if readsEnv && parsesBool {
		violations = append(violations, fmt.Sprintf(
			"%s:%d: %s reads the environment and parses a boolean itself",
			path, fset.Position(envPositions[0]).Line, fn.Name.Name))
	}
	if readsEnv {
		for _, comparison := range comparisons {
			violations = append(violations, fmt.Sprintf(
				"%s:%d: %s compares an environment value with a boolean literal",
				path, fset.Position(comparison.Pos()).Line, fn.Name.Name))
		}
	}
	return violations
}

// readsPtahVariable reports whether an environment read is one this contract
// governs.
//
// A read whose argument is a string literal is judged by the name in it, which
// keeps `os.Getenv("VISUAL")` and `os.Getenv("CLICKHOUSE_URL")` out of a rule
// that has nothing to say about them. A read whose argument is anything else --
// a constant, a computed name -- is IN scope, because that is how every reader
// this change touched spells its own variable, and a rule that needed a literal
// would have matched none of them.
func readsPtahVariable(call *ast.CallExpr) bool {
	if len(call.Args) != 1 {
		return true
	}
	literal, ok := call.Args[0].(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return true
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return true
	}
	return strings.HasPrefix(value, envbool.Prefix)
}

// isBooleanLiteral reports whether expression is one of the string spellings a
// hand-rolled truthiness test compares against.
func isBooleanLiteral(expression ast.Expr) bool {
	literal, ok := expression.(*ast.BasicLit)
	if !ok || literal.Kind != token.STRING {
		return false
	}
	value, err := strconv.Unquote(literal.Value)
	if err != nil {
		return false
	}
	return slices.Contains(booleanLiterals, value)
}

// selectorName renders a package-qualified call target, or "" for anything else.
func selectorName(expression ast.Expr) string {
	selector, ok := expression.(*ast.SelectorExpr)
	if !ok {
		return ""
	}
	pkg, ok := selector.X.(*ast.Ident)
	if !ok {
		return ""
	}
	return pkg.Name + "." + selector.Sel.Name
}

// ptahVarNames returns every PTAH_* name mentioned in a string literal in the
// tree's non-test Go source, sorted and deduplicated.
//
// Only string literals count. A name in a comment is prose; a name in a literal
// is the program naming a variable.
func ptahVarNames(c *qt.C, root string) []string {
	c.Helper()
	var names []string
	for _, path := range goSourceFiles(c, root) {
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		c.Assert(err, qt.IsNil)
		ast.Inspect(file, func(node ast.Node) bool {
			literal, ok := node.(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			value, err := strconv.Unquote(literal.Value)
			if err != nil {
				return true
			}
			names = append(names, ptahVarPattern.FindAllString(value, -1)...)
			return true
		})
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// goSourceFiles lists the repository's non-test Go files, excluding the shared
// package itself and this guard's own fixtures.
//
// git is the path source for the reason scripts/check-test-style.sh gives: a
// filesystem walk descends into every linked worktree parked under the
// repository, and judges code that is not in this working tree at all.
func goSourceFiles(c *qt.C, root string) []string {
	c.Helper()
	cmd := exec.Command("git", "-c", "core.quotePath=false",
		"ls-files", "--cached", "--others", "--exclude-standard", "--", "*.go")
	cmd.Dir = root
	var out bytes.Buffer
	cmd.Stdout = &out
	c.Assert(cmd.Run(), qt.IsNil)

	var paths []string
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		if line == "" || strings.HasSuffix(line, "_test.go") {
			continue
		}
		if strings.HasPrefix(line, "internal/envbool/") || strings.HasPrefix(line, "cmd/internal/envboolguard/") {
			continue
		}
		paths = append(paths, filepath.Join(root, line))
	}
	c.Assert(len(paths) > 100, qt.IsTrue, qt.Commentf(
		"selected %d files; a guard that scans nothing is also green", len(paths)))
	return paths
}

// repositoryRoot resolves the checkout this test belongs to.
func repositoryRoot(c *qt.C) string {
	c.Helper()
	root, err := filepath.Abs(filepath.Join("..", "..", ".."))
	c.Assert(err, qt.IsNil)
	return root
}

// writeFile writes a fixture, failing the test rather than the scanner.
func writeFile(c *qt.C, path, content string) {
	c.Helper()
	c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
}
