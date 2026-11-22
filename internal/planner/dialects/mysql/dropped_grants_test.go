package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// droppedWithGrants is a diff that drops the table t1 and the procedure rt
// while reader holds privileges on both, and on the table t3 the schema keeps.
func droppedWithGrants() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesRemoved: difftypes.TableRemovals{{Name: "t1"}},
		ProceduresRemoved: difftypes.FunctionChanges{{Function: schemamodel.Function{
			Name: "rt", Kind: schemamodel.FunctionKindProcedure, Language: "plsql", Body: "BEGIN NULL; END;",
		}}},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
			{Role: "reader", Privilege: "DEBUG", ObjectType: "TABLE", ObjectName: "rt"},
			{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t3"},
		},
	}
}

// replacedWithGrant is a diff that changes the body of the routine rt, written
// in language, while reader loses EXECUTE on it.
func replacedWithGrant(language string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		FunctionsModified: []difftypes.FunctionDiff{{
			FunctionName: "rt",
			Changes:      map[string]string{"body": "BEGIN NULL; END; -> BEGIN RETURN; END;"},
			Desired: schemamodel.Function{
				Name: "rt", Kind: schemamodel.FunctionKindProcedure, Language: language, Body: "BEGIN RETURN; END;",
			},
		}},
		GrantsRemoved: []difftypes.GrantRef{{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt"}},
	}
}

// On Oracle the revokes run after the drops, and a DROP takes every privilege
// on its object, so a REVOKE naming a dropped table or routine fails with
// ORA-00942, measured on Oracle 23.26.3. The plan revokes only what the role
// holds on an object that stays.
func TestPlan_OracleRevokesNothingOnWhatItDrops(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()), droppedWithGrants(), "oracle",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"-- WARNING: Ensure no other objects depend on this procedure\nDROP PROCEDURE IF EXISTS rt",
		"-- WARNING: This will delete all data!\nDROP TABLE IF EXISTS t1 PURGE",
		"REVOKE SELECT ON t3 FROM reader",
	})
}

// A changed Oracle routine is replaced by DROP and CREATE, which takes its
// privileges too. A routine in a language Oracle does not run is left alone,
// so the privilege on it is still revoked.
func TestPlan_OracleRevokesNothingOnARoutineItReplaces(t *testing.T) {
	tests := []struct {
		name     string
		language string
		want     []string
	}{
		{
			name:     "a PL/SQL procedure is replaced",
			language: "plsql",
			want: []string{
				"-- Replace procedure rt: body\nDROP PROCEDURE IF EXISTS rt",
				"-- Modify procedure rt: body\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN RETURN; END;",
			},
		},
		{
			name:     "a procedure in another language is left alone",
			language: "plpgsql",
			want: []string{
				"-- ORACLE: CREATE FUNCTION rt declares language plpgsql, which this target does not run; skipped.\n" +
					"--   If this body is PL/SQL, declare language=\"plsql\" or no language.\n" +
					"REVOKE EXECUTE ON rt FROM reader",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), replacedWithGrant(test.language), "oracle",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// MySQL keeps a table privilege after DROP TABLE, and MySQL and SQL Server
// revoke before they drop anything, so a REVOKE on an object the plan drops
// still names one that is there. Their plans keep it.
func TestPlan_RevokesFirstStillRevokeOnWhatTheyDrop(t *testing.T) {
	tests := []struct {
		dialect string
		want    []string
	}{
		{
			dialect: "mysql",
			want: []string{
				"REVOKE SELECT ON `t1` FROM `reader`",
				"REVOKE DEBUG ON `rt` FROM `reader`",
				"REVOKE EXECUTE ON `rt` FROM `reader`",
				"REVOKE SELECT ON `t3` FROM `reader`",
				"-- WARNING: Ensure no other objects depend on this procedure\nDROP PROCEDURE IF EXISTS `rt`",
				"-- WARNING: This will delete all data!\nDROP TABLE IF EXISTS `t1`",
			},
		},
		{
			dialect: "sqlserver",
			want: []string{
				"REVOKE SELECT ON [t1] FROM [reader]",
				"REVOKE DEBUG ON [rt] FROM [reader]",
				"REVOKE EXECUTE ON [rt] FROM [reader]",
				"REVOKE SELECT ON [t3] FROM [reader]",
				"-- WARNING: This will delete all data!\nDROP TABLE IF EXISTS [t1]",
				"-- WARNING: Ensure no other objects depend on this procedure\nDROP PROCEDURE IF EXISTS [rt]",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), droppedWithGrants(), test.dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
