package fromschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
)

// userTypeOrderDatabase declares one type of each kind naming another kind, in
// both directions, so a whole-schema render that emits kind by kind gets one
// direction wrong whichever kind it puts first.
func userTypeOrderDatabase() goschema.Database {
	return goschema.Database{
		Domains: []goschema.Domain{
			{Name: "d_comp", BaseType: "addr"},
			{Name: "d_range", BaseType: "myrange"},
			{Name: "d_int", BaseType: "integer", Check: "VALUE > 0"},
		},
		CompositeTypes: []goschema.CompositeType{
			{Name: "addr", Fields: []goschema.CompositeTypeField{{Name: "street", Type: "text"}}},
			{Name: "measure", Fields: []goschema.CompositeTypeField{{Name: "qty", Type: "d_int"}}},
		},
		Ranges: []goschema.Range{
			{Name: "myrange", Subtype: "integer"},
			{Name: "posrange", Subtype: "d_int"},
		},
	}
}

// TestFromDatabase_CreatesUserTypesBeforeTheTypesThatNameThem is the ordering
// property for the whole-schema render, the one `ptah generate` writes out.
//
// The migration planner has the same property with its own guard. Both are
// needed: they are separate emitters over the same three kinds, and the planner
// only ever sees the types a diff adds.
func TestFromDatabase_CreatesUserTypesBeforeTheTypesThatNameThem(t *testing.T) {
	c := qt.New(t)

	statements := fromschema.FromDatabase(userTypeOrderDatabase(), "postgres")
	sql, err := renderer.RenderSQL("postgres", statements.Statements...)
	c.Assert(err, qt.IsNil)

	tests := []struct {
		name    string
		earlier string
		later   string
	}{
		{
			name:    "a composite precedes the domain over it",
			earlier: `CREATE TYPE "addr" AS`,
			later:   `CREATE DOMAIN "d_comp" AS`,
		},
		{
			name:    "a range precedes the domain over it",
			earlier: `CREATE TYPE "myrange" AS RANGE`,
			later:   `CREATE DOMAIN "d_range" AS`,
		},
		{
			name:    "a domain precedes the composite whose field uses it",
			earlier: `CREATE DOMAIN "d_int" AS`,
			later:   `CREATE TYPE "measure" AS`,
		},
		{
			name:    "a domain precedes the range whose subtype uses it",
			earlier: `CREATE DOMAIN "d_int" AS`,
			later:   `CREATE TYPE "posrange" AS RANGE`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			assertRenderedBefore(c, sql, test.earlier, test.later)
		})
	}
}

func assertRenderedBefore(c *qt.C, sql, earlier, later string) {
	c.Helper()

	earlierIndex := strings.Index(sql, earlier)
	laterIndex := strings.Index(sql, later)
	c.Assert(earlierIndex, qt.Not(qt.Equals), -1, qt.Commentf("missing %q in:\n%s", earlier, sql))
	c.Assert(laterIndex, qt.Not(qt.Equals), -1, qt.Commentf("missing %q in:\n%s", later, sql))
	c.Assert(earlierIndex < laterIndex, qt.IsTrue, qt.Commentf("expected %q before %q in:\n%s", earlier, later, sql))
}
