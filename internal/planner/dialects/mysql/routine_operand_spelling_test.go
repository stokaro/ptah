package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestCompare_AModifiedFunctionRendersTheDeclarationAsWritten pins which copy of
// a function the change carries.
//
// The comparison folds a local copy of the declaration so that two spellings of
// one function converge: Canonicalize lowercases the return and parameter types
// and fills in a security and a volatility the author may not have written, and
// on MySQL the types are normalized through their aliases on top of that. Both
// folds exist to answer "did this change", and neither is a statement about what
// should be written.
//
// The change carries the declaration from BEFORE those folds
// (stokaro/ptah#2315). Carrying the folded copy is not a subtle difference:
// measured on this fixture it renders
//
//	CREATE FUNCTION `total`(n int) RETURNS int READS SQL DATA SQL SECURITY INVOKER ...
//
// where the author wrote INTEGER twice and no SECURITY clause at all.
func TestCompare_AModifiedFunctionRendersTheDeclarationAsWritten(t *testing.T) {
	c := qt.New(t)

	desired := &schemamodel.Database{Functions: []schemamodel.Function{{
		StructName: "Total", Name: "total",
		Parameters: "n INTEGER", Returns: "INTEGER",
		Language: "sql", Body: "RETURN n * 2;",
	}}}
	database := &catalog.Database{Functions: []catalog.Function{{
		Name: "total", Parameters: "n INTEGER", Returns: "INTEGER",
		Language: "sql", Body: "RETURN n * 3;",
	}}}

	diff := schemadiff.CompareWithDialect(desired, database, platform.MySQL)

	c.Assert(diff.FunctionsModified, qt.HasLen, 1)
	c.Assert(diff.FunctionsModified[0].Desired.Returns, qt.Equals, "INTEGER")
	c.Assert(diff.FunctionsModified[0].Desired.Security, qt.Equals, "")

	sql, err := planner.GenerateSchemaDiffSQL(diff, desired, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "RETURNS INTEGER",
		qt.Commentf("the author wrote INTEGER; the fold's `int` is a comparison detail\n%s", sql))
	c.Assert(sql, qt.Contains, "(n INTEGER)")
	c.Assert(sql, qt.Not(qt.Contains), "SQL SECURITY",
		qt.Commentf("the author wrote no security clause\n%s", sql))
}
