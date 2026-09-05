package indexbackingguard_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// consumer is a tree that must reach the shared ownership evidence, and the
// function it must reach. Naming the function rather than only the package is
// what keeps an import that no longer decides anything from satisfying this.
type consumer struct {
	directory string
	function  string
}

// consumers are the two trees that decide constraint-backed index ownership.
//
// Both are named rather than discovered, because the property is about these
// two answering the same question -- a third package importing the shared
// evidence is welcome and is not what this guard is about.
var consumers = []consumer{
	{directory: "internal/convert/dbschematogo", function: "NamedAfterConstraint"},
	{directory: "internal/convert/dbschematogo", function: "Unaddressable"},
	{directory: "migration/schemadiff/internal/compare", function: "ServerBacks"},
	{directory: "migration/schemadiff/internal/compare", function: "Unaddressable"},
}

func repositoryRoot(c *qt.C) string {
	output, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(string(output))
}

// sharedCallsIn returns the indexbacking functions a package's production files
// call. Test files are excluded deliberately: a call from a test proves the
// shared decision compiles, not that the product consults it.
func sharedCallsIn(c *qt.C, directory string) map[string]bool {
	entries, err := os.ReadDir(directory)
	c.Assert(err, qt.IsNil)

	called := make(map[string]bool)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(token.NewFileSet(), filepath.Join(directory, name), nil, 0)
		c.Assert(parseErr, qt.IsNil)
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if pkg.Name == "indexbacking" {
				called[selector.Sel.Name] = true
			}
			return true
		})
	}
	return called
}

// TestBothTreesConsultTheSharedOwnershipEvidence fails when a tree stops asking
// the shared decision, which is how the two independent answers came to exist.
func TestBothTreesConsultTheSharedOwnershipEvidence(t *testing.T) {
	for _, want := range consumers {
		t.Run(want.directory+"/"+want.function, func(t *testing.T) {
			c := qt.New(t)
			directory := filepath.Join(repositoryRoot(c), want.directory)
			c.Assert(sharedCallsIn(c, directory)[want.function], qt.IsTrue,
				qt.Commentf("%s no longer calls indexbacking.%s; see the package doc",
					want.directory, want.function))
		})
	}
}

// TestTheGuardCanFail is the control. Every assertion above is "this call is
// present", and a lookup that answered true for anything would satisfy all four
// while reading nothing. This asks the same reader for a call no package makes.
func TestTheGuardCanFail(t *testing.T) {
	c := qt.New(t)
	directory := filepath.Join(repositoryRoot(c), "internal/convert/dbschematogo")
	c.Assert(sharedCallsIn(c, directory)["NoSuchDecision"], qt.IsFalse)
}

// TestTheGuardReadsProductionFilesOnly pins the exclusion the reader makes. A
// guard that counted a test file's call would report the product as consulting
// a decision the product had stopped consulting.
func TestTheGuardReadsProductionFilesOnly(t *testing.T) {
	c := qt.New(t)
	directory := filepath.Join(repositoryRoot(c), "internal/indexbackingguard")
	c.Assert(sharedCallsIn(c, directory), qt.HasLen, 0)
}
