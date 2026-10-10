package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// A SQL Server procedure body runs to the end of its batch, so a plan split as
// one script would make the CREATE TABLE after a procedure part of the
// procedure. Each planned node ends its own statements, through both entry
// points that return statements.
func TestGenerateSchemaDiffSQLStatements_EndsAProcedureWithItsNode(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}},
		Functions: []schemamodel.Function{{
			StructName: "F", Name: "do_it", Language: "sql", Body: "SELECT 1;", Kind: schemamodel.FunctionKindProcedure,
		}},
	}
	runtime := must.Must(builtin.New())
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, &catalog.Database{}, platform.SQLServer, runtime))

	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, diff, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Equals, "CREATE OR ALTER PROCEDURE [do_it] \nAS\nSELECT 1;;")
	c.Assert(statements[1], qt.Matches, `(?s).*CREATE TABLE \[t\].*`)
	withOptions := must.Must(planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.SQLServer, planner.Options{}))
	c.Assert(withOptions, qt.DeepEquals, statements)
}
