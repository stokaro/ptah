package mysql_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// A restored Oracle grant option copies a catalog row, quoting its schema,
// routine and grantee. Quotes inside those identifiers stay inside them.
func TestPlan_RestoresAnOracleGrantOptionWithQuotedNames(t *testing.T) {
	c := qt.New(t)
	const routine = `"Odd Schema"."Odd""Routine"`
	diff := &difftypes.SchemaDiff{
		FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
			Name: routine, Kind: "procedure", Language: "plsql", Body: "BEGIN NULL; END;",
		}}},
		CurrentGrants: []difftypes.GrantRef{{
			Role: `odd"user`, Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: routine, WithOption: true,
		}},
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), must.Must(builtin.New()), diff, platform.Oracle)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains,
		`GRANT EXECUTE ON "Odd Schema"."Odd""Routine" TO "odd""user" WITH GRANT OPTION`)
}

// A snapshot's privilege names still cross a SQL boundary. Invalid tokens
// are refused instead of being written into a raw restoration statement.
func TestPlan_RefusesAnInvalidOracleRestoredPrivilege(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		FunctionsAdded: difftypes.FunctionChanges{{Function: schemamodel.Function{
			Name: "RT", Kind: "procedure", Language: "plsql", Body: "BEGIN NULL; END;",
		}}},
		CurrentGrants: []difftypes.GrantRef{{
			Role: "AUDITOR", Privilege: "EXECUTE; DROP TABLE USERS", ObjectType: "TABLE", ObjectName: "RT", WithOption: true,
		}},
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), must.Must(builtin.New()), diff, platform.Oracle)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*cannot restore the observed grant option on "RT" to "AUDITOR": invalid object or privilege name`)
	c.Assert(statements, qt.IsNil)
}
