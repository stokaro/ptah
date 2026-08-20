package mssql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	mssqlplanner "go.5x5.cz/ptah/internal/planner/dialects/mssql"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// A modified procedure is planned as a drop and a create. The drop half was
// built without the kind, so it rendered DROP FUNCTION for a procedure, and
// SQL Server answers that with "does not exist" about an object that is right
// there: the apply fails and the old body stays (stokaro/ptah#1784).

// planProcedureReplacement plans the modification of one routine and renders
// the statements, so the assertion is on what the server would be given.
func planProcedureReplacement(c *qt.C, kind string) string {
	c.Helper()
	desired := &goschema.Database{
		Functions: []goschema.Function{{
			StructName: "Report", Name: "dbo.p_report", Kind: kind,
			Parameters: "@id int", Language: "sql",
			Body: "BEGIN SELECT @id; END",
		}},
	}
	diff := &difftypes.SchemaDiff{
		FunctionsModified: []difftypes.FunctionDiff{{
			FunctionName: "dbo.p_report",
			Changes:      map[string]string{"body": "old -> new"},
		}},
	}

	nodes, err := mssqlplanner.New().GenerateMigrationASTChecked(diff, desired)
	c.Assert(err, qt.IsNil)

	var rendered strings.Builder
	for _, node := range nodes {
		sql, renderErr := renderer.RenderSQL(platform.SQLServer, node)
		c.Assert(renderErr, qt.IsNil)
		rendered.WriteString(sql)
		rendered.WriteString("\n")
	}
	return rendered.String()
}

func TestPlanner_ProcedureReplacementDropsWithTheProcedureVerb(t *testing.T) {
	c := qt.New(t)

	rendered := planProcedureReplacement(c, goschema.FunctionKindProcedure)

	c.Assert(rendered, qt.Contains, "DROP PROCEDURE IF EXISTS")
	c.Assert(rendered, qt.Not(qt.Contains), "DROP FUNCTION")
	c.Assert(rendered, qt.Contains, "CREATE OR ALTER PROCEDURE")
}

// TestPlanner_FunctionReplacementKeepsTheFunctionVerb is the control: carrying
// the kind must not change what a function's replacement plans as.
func TestPlanner_FunctionReplacementKeepsTheFunctionVerb(t *testing.T) {
	c := qt.New(t)

	rendered := planProcedureReplacement(c, "")

	c.Assert(rendered, qt.Contains, "DROP FUNCTION IF EXISTS")
	c.Assert(rendered, qt.Not(qt.Contains), "DROP PROCEDURE")
}

// TestPlanner_ReplacementCommentNamesTheObject keeps the plan readable: a
// comment on a procedure that calls it a function sends a reader looking for
// the wrong object.
func TestPlanner_ReplacementCommentNamesTheObject(t *testing.T) {
	c := qt.New(t)

	procedure := planProcedureReplacement(c, goschema.FunctionKindProcedure)
	function := planProcedureReplacement(c, "")

	c.Assert(procedure, qt.Contains, "Replace procedure dbo.p_report")
	c.Assert(function, qt.Contains, "Replace function dbo.p_report")
}
