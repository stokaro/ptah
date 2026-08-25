package generate

// White-box testing required: getDependencyInfo is an unexported debug
// formatter, and the render command only exposes its output interleaved with
// the rest of the command's stderr reporting, so the format cannot be pinned
// through the exported command without asserting on unrelated render output.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestGetDependencyInfo_EmptyResult(t *testing.T) {
	c := qt.New(t)

	// Create an empty result to test edge case
	result := &goschema.Database{
		Tables:       make([]goschema.Table, 0),
		Dependencies: make(map[string][]string),
	}

	info := getDependencyInfo(result)

	// Should still contain the headers even with no tables
	c.Assert(info, qt.Contains, "Table Dependencies:")
	c.Assert(info, qt.Contains, "Table Creation Order:")

	// Should not contain any table entries
	c.Assert(info, qt.Not(qt.Contains), ": (no dependencies)")
	c.Assert(info, qt.Not(qt.Contains), ": depends on")
}

func TestGetDependencyInfo(t *testing.T) {
	c := qt.New(t)

	result, err := goschema.ParseDir("../../stubs")
	c.Assert(err, qt.IsNil)

	info := getDependencyInfo(result)

	// Verify the output contains expected sections
	c.Assert(info, qt.Contains, "Table Dependencies:")
	c.Assert(info, qt.Contains, "==================")

	// Verify specific dependency information
	c.Assert(info, qt.Contains, "articles: depends on [users]")
	c.Assert(info, qt.Contains, "products: depends on [categories]")
	c.Assert(info, qt.Contains, "categories: (no dependencies)") // self-reference moved to SelfReferencingForeignKeys

	// Verify tables with no dependencies are marked correctly
	c.Assert(info, qt.Contains, "users: (no dependencies)")

	// Verify the creation order section opens with a numbered list
	_, order, found := strings.Cut(info, "Table Creation Order:\n====================\n")
	c.Assert(found, qt.IsTrue, qt.Commentf("Should find Table Creation Order section"))
	firstLine, _, _ := strings.Cut(order, "\n")
	c.Assert(firstLine, qt.Matches, `\d+\. \w+`)
}
