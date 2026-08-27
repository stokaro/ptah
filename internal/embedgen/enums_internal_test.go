package embedgen

// White-box testing required: the enumeration ratchet reads this package's own
// source to find every declared constant, and compares it against the lists in
// enums.go.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestEnums_ListEveryDeclaredConstant is the ratchet.
//
// A constant that exists and is not in its list is a value a specification file
// cannot name -- so a policy somebody implemented is a policy nobody can select,
// and the compiler is happy about it either way.
//
// The two sides come from different places, which is what makes this a check
// rather than a restatement: the lists are written by hand in enums.go, and the
// constants are read out of this package's syntax tree (stokaro/ptah#2068).
func TestEnums_ListEveryDeclaredConstant(t *testing.T) {
	declared := declaredEnumConstants(t)
	listed := map[string][]string{
		"VersionStrategy":     names(VersionStrategies()),
		"NullPolicy":          names(NullPolicies()),
		"EmptyPolicy":         names(EmptyPolicies()),
		"UnicodeForm":         names(UnicodeForms()),
		"TruncatePolicy":      names(TruncatePolicies()),
		"EndpointClass":       names(EndpointClasses()),
		"VectorNormalization": names(VectorNormalizations()),
		"DistanceMetric":      names(DistanceMetrics()),
	}

	for _, typeName := range sortedTypeNames(declared) {
		t.Run(typeName, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(listed[typeName], qt.DeepEquals, declared[typeName], qt.Commentf(
				"every %s constant this package declares has to appear in its list in enums.go, "+
					"or it is a value no specification file can name", typeName))
		})
	}
}

// TestEnums_TheRatchetFoundSomethingToCheck is the guard on the guard.
//
// A syntax walk that matched nothing would report every list as correct,
// including an empty one. This is the count it has to find, and a new
// enumerated type raises it deliberately.
func TestEnums_TheRatchetFoundSomethingToCheck(t *testing.T) {
	c := qt.New(t)

	declared := declaredEnumConstants(t)

	c.Assert(declared, qt.HasLen, 8)
	c.Assert(len(flatten(declared)) >= 20, qt.IsTrue,
		qt.Commentf("the walk found only %d constants", len(flatten(declared))))
}

// names renders an enumerated list as its string values, sorted.
func names[T ~string](values []T) []string {
	rendered := make([]string, 0, len(values))
	for _, value := range values {
		rendered = append(rendered, string(value))
	}
	sort.Strings(rendered)
	return rendered
}

// declaredEnumConstants reads this package's source for the values each
// enumerated type declares.
func declaredEnumConstants(t *testing.T) map[string][]string {
	t.Helper()
	found := make(map[string][]string)
	for _, file := range packageFiles(t) {
		collectConstants(t, file, found)
	}
	for typeName := range found {
		sort.Strings(found[typeName])
	}
	return found
}

// packageFiles lists this package's non-test Go files.
func packageFiles(t *testing.T) []string {
	t.Helper()
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("read the package directory: %v", err)
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) != ".go" || isTestFile(name) {
			continue
		}
		files = append(files, name)
	}
	return files
}

// isTestFile reports whether a file is a test.
func isTestFile(name string) bool {
	return len(name) > 8 && name[len(name)-8:] == "_test.go"
}

// collectConstants records the string constants of each enumerated type in one
// file.
func collectConstants(t *testing.T, path string, found map[string][]string) {
	t.Helper()
	parsed, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	for _, declaration := range parsed.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			recordValueSpec(spec, found)
		}
	}
}

// recordValueSpec records one const line when it declares an enumerated type.
func recordValueSpec(spec ast.Spec, found map[string][]string) {
	value, ok := spec.(*ast.ValueSpec)
	if !ok || value.Type == nil {
		return
	}
	typeName, ok := value.Type.(*ast.Ident)
	if !ok || !isEnumType(typeName.Name) {
		return
	}
	for _, literal := range value.Values {
		basic, ok := literal.(*ast.BasicLit)
		if !ok || basic.Kind != token.STRING {
			continue
		}
		found[typeName.Name] = append(found[typeName.Name], unquote(basic.Value))
	}
}

// enumTypes are the enumerated types the lists in enums.go cover.
var enumTypes = map[string]bool{
	"VersionStrategy": true, "NullPolicy": true, "EmptyPolicy": true, "UnicodeForm": true,
	"TruncatePolicy": true, "EndpointClass": true, "VectorNormalization": true,
	"DistanceMetric": true,
}

// isEnumType reports whether a type is one of them.
func isEnumType(name string) bool {
	return enumTypes[name]
}

// unquote strips a string literal's quotes.
func unquote(literal string) string {
	if len(literal) < 2 {
		return literal
	}
	return literal[1 : len(literal)-1]
}

// sortedTypeNames orders the types for a stable subtest listing.
func sortedTypeNames(declared map[string][]string) []string {
	names := make([]string, 0, len(declared))
	for name := range declared {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// flatten counts every constant found.
func flatten(declared map[string][]string) []string {
	var all []string
	for _, values := range declared {
		all = append(all, values...)
	}
	return all
}
