package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestCompare_TwoAddedOverloadsAreTwoDifferentStatements is the regression for
// stokaro/ptah#2408.
//
// `pairRoutineOverloads` reported how MANY declarations went unmatched, not
// which. The same name was appended once per unmatched overload, and the
// planner resolved that name back to a declaration by exact match -- finding
// the same one every time. Two declared overloads produced one of them twice,
// the other was never created, and the migration reported success, because a
// second `CREATE OR REPLACE` is a no-op.
//
// The bodies are asserted rather than the count: two statements that are the
// same statement twice are still two statements.
func TestCompare_TwoAddedOverloadsAreTwoDifferentStatements(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Functions: []schemamodel.Function{
			{StructName: "F", Name: "fmt_it", Parameters: "a integer", Returns: "text",
				Language: "sql", Body: "SELECT a::text"},
			{StructName: "F", Name: "fmt_it", Parameters: "a text", Returns: "text",
				Language: "sql", Body: "SELECT upper(a)"},
		},
	}

	diff := schemadiff.CompareWithDialect(desired, &catalog.Database{}, platform.Postgres)
	c.Assert(diff.FunctionsAdded, qt.HasLen, 2)

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, `"fmt_it"(a integer)`)
	c.Assert(statements[0], qt.Contains, "SELECT a::text")
	c.Assert(statements[1], qt.Contains, `"fmt_it"(a text)`)
	c.Assert(statements[1], qt.Contains, "SELECT upper(a)")
}

// TestCompare_AnAddedOverloadBesideAnExistingOneCarriesItsOwnDeclaration
// covers the other branch of the same pairing.
//
// The test above starts from an empty database, which takes the early return
// that reports every declaration. This one has the database holding ONE of the
// two overloads, so the pairing walks its loop and reports the unmatched
// declaration from inside it. Mutating either branch alone leaves the other
// green, which is why both are here.
func TestCompare_AnAddedOverloadBesideAnExistingOneCarriesItsOwnDeclaration(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{
		Functions: []schemamodel.Function{
			{StructName: "F", Name: "fmt_it", Parameters: "a integer", Returns: "text",
				Language: "sql", Body: "SELECT a::text"},
			{StructName: "F", Name: "fmt_it", Parameters: "a text", Returns: "text",
				Language: "sql", Body: "SELECT upper(a)"},
		},
	}
	database := &catalog.Database{
		Functions: []catalog.Function{
			{Name: "fmt_it", Parameters: "a integer", Returns: "text",
				Language: "sql", Body: "SELECT a::text"},
		},
	}

	diff := schemadiff.CompareWithDialect(desired, database, platform.Postgres)

	c.Assert(diff.FunctionsAdded, qt.HasLen, 1)
	c.Assert(diff.FunctionsAdded[0].Parameters, qt.Equals, "a text",
		qt.Commentf("the overload the database lacks, not whichever the schema listed first"))
	c.Assert(diff.FunctionsAdded[0].Body, qt.Equals, "SELECT upper(a)")
}
