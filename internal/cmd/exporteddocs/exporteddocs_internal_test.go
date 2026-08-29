package main

// White-box testing required: the rules this gate enforces are unexported
// functions over a Go AST, and the only alternative is mutating the repository
// from outside and reading an exit code -- which is the meta-layer
// stokaro/ptah#2509 exists to replace. Exporting them to test them would widen
// a command's surface for the test's convenience.

import (
	"go/parser"
	"go/token"
	"testing"

	qt "github.com/frankban/quicktest"
)

// findingsIn parses one source and applies the rules to it.
func findingsIn(c *qt.C, source string) []finding {
	c.Helper()
	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, "x.go", source, parser.ParseComments)
	c.Assert(err, qt.IsNil)
	return undocumentedInFile(fileSet, file)
}

// names is what the findings are about, which is all these assertions need.
func names(findings []finding) []string {
	var out []string
	for _, f := range findings {
		out = append(out, f.kind+" "+f.name)
	}
	return out
}

func TestUndocumented_ReportsAnExportedFunctionWithNoComment(t *testing.T) {
	c := qt.New(t)

	found := findingsIn(c, "package p\n\nfunc Exported() {}\n")

	c.Assert(names(found), qt.DeepEquals, []string{"function Exported"})
}

func TestUndocumented_ReportsAnExportedTypeWithNoComment(t *testing.T) {
	c := qt.New(t)

	found := findingsIn(c, "package p\n\ntype Exported struct{}\n")

	c.Assert(names(found), qt.DeepEquals, []string{"type Exported"})
}

// TestUndocumented_KeepsQuietAboutWhatItDoesNotGovern is the control, and it is
// the assertion that matters most: the rule reported 158 declarations before
// its exemptions, and 148 of them were interface implementations.
func TestUndocumented_KeepsQuietAboutWhatItDoesNotGovern(t *testing.T) {
	tests := []struct {
		name   string
		source string
	}{
		{
			name:   "a documented function",
			source: "package p\n\n// Exported does a thing.\nfunc Exported() {}\n",
		},
		{
			name:   "an unexported function",
			source: "package p\n\nfunc unexported() {}\n",
		},
		{
			// A method repeats what the interface it implements already
			// documents. This exemption is why the rule is usable at all.
			name:   "a method, documented or not",
			source: "package p\n\ntype T struct{}\n\nfunc (T) Accept() {}\n\n// Doc is documented.\nfunc (T) Doc() {}\n",
		},
		{
			name:   "a documented type",
			source: "package p\n\n// Exported is a thing.\ntype Exported struct{}\n",
		},
		{
			// A grouped declaration documents its members through its own
			// comment as often as through theirs.
			name:   "a type in a documented group",
			source: "package p\n\n// The shapes this package models.\ntype (\n\tA struct{}\n\tB struct{}\n)\n",
		},
		{
			name:   "an unexported type",
			source: "package p\n\ntype unexported struct{}\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// The method row declares a type, so filter to what this row is
			// about rather than asserting an empty slice.
			found := findingsIn(c, test.source)
			for _, f := range found {
				c.Assert(f.name, qt.Equals, "T", qt.Commentf("unexpected finding: %s %s", f.kind, f.name))
			}
		})
	}
}

// TestUndocumented_ReportsEachMemberOfAnUndocumentedGroup keeps the group
// exemption from swallowing a group nobody documented at all.
func TestUndocumented_ReportsEachMemberOfAnUndocumentedGroup(t *testing.T) {
	c := qt.New(t)

	found := findingsIn(c, "package p\n\ntype (\n\tA struct{}\n\tB struct{}\n)\n")

	c.Assert(names(found), qt.DeepEquals, []string{"type A", "type B"})
}

// TestPlural_PicksTheSpellingOneCountNeeds covers the diagnostic. A gate whose
// failure reads "1 exported declarations" is a gate someone stops reading.
func TestPlural_PicksTheSpellingOneCountNeeds(t *testing.T) {
	c := qt.New(t)

	c.Assert(plural(1, "declaration", "declarations"), qt.Equals, "declaration")
	c.Assert(plural(0, "declaration", "declarations"), qt.Equals, "declarations")
	c.Assert(plural(2, "declaration", "declarations"), qt.Equals, "declarations")
}
