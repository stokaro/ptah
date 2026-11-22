package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
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

// heldOnAReplacedRoutine is a diff that changes the body of the procedure rt,
// written in language, while the read reports what roles hold on it: reader's
// EXECUTE, which the declaration keeps, reader's removed privilege, which the
// plan revokes, and auditor's EXECUTE, held with the grant option where
// auditorOption is set, for a role the schema does not manage. Reader also
// holds SELECT on the table t3, which the plan keeps, and on a schema that
// shares the routine's name.
func heldOnAReplacedRoutine(language, removed string, auditorOption bool) *difftypes.SchemaDiff {
	diff := replacedWithGrant(language)
	revoked := difftypes.GrantRef{Role: "reader", Privilege: removed, ObjectType: "TABLE", ObjectName: "rt"}
	diff.GrantsRemoved = []difftypes.GrantRef{revoked}
	diff.CurrentGrants = []difftypes.GrantRef{
		{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt"},
		revoked,
		{Role: "auditor", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt", WithOption: auditorOption},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t3"},
		{Role: "reader", Privilege: "SELECT", ObjectType: "SCHEMA", ObjectName: "rt"},
	}
	return diff
}

// A routine the plan replaces with DROP and CREATE holds nothing once both
// ran, measured on Oracle 23.26.3 and SQL Server 2025. The plan grants again,
// after the CREATE, every privilege the read reported on it except the one it
// revokes, a privilege of a role the schema does not manage included. MySQL
// reads report no privilege on a routine, and a table privilege of the same
// name is the table's, so the MySQL plan gives back nothing.
func TestPlan_GivesBackWhatARoleHeldOnAReplacedRoutine(t *testing.T) {
	tests := []struct {
		dialect       string
		language      string
		removed       string
		auditorOption bool
		want          []string
	}{
		{
			dialect:  "oracle",
			language: "plsql",
			removed:  "DEBUG",
			want: []string{
				"-- Replace procedure rt: body\nDROP PROCEDURE IF EXISTS rt",
				"-- Modify procedure rt: body\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN RETURN; END;",
				"GRANT EXECUTE ON rt TO reader",
				"GRANT EXECUTE ON rt TO auditor",
			},
		},
		{
			dialect:       "sqlserver",
			language:      "sql",
			removed:       "CONTROL",
			auditorOption: true,
			want: []string{
				"REVOKE CONTROL ON [rt] FROM [reader]",
				"-- Replace procedure rt: body\nDROP PROCEDURE IF EXISTS [rt]",
				"-- Modify procedure rt: body\nCREATE OR ALTER PROCEDURE [rt] \nAS\nBEGIN RETURN; END",
				"GRANT EXECUTE ON [rt] TO [reader]",
				"GRANT EXECUTE ON [rt] TO [auditor] WITH GRANT OPTION",
			},
		},
		{
			dialect:  "mysql",
			language: "sql",
			removed:  "SELECT",
			want: []string{
				"REVOKE SELECT ON `rt` FROM `reader`",
				"-- Replace procedure rt: body\nDROP PROCEDURE IF EXISTS `rt`",
				"-- Modify procedure rt: body\nCREATE PROCEDURE `rt`() READS SQL DATA BEGIN RETURN; END",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
				heldOnAReplacedRoutine(test.language, test.removed, test.auditorOption), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

// A grant option the plan takes away does not come back with the privilege it
// was held on.
func TestPlan_GivesBackAPrivilegeWithoutTheGrantOptionItRevokes(t *testing.T) {
	c := qt.New(t)
	diff := heldOnAReplacedRoutine("sql", "CONTROL", true)
	diff.GrantOptionsRevoked = []difftypes.GrantRef{{Role: "auditor", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt", WithOption: true}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(statements[len(statements)-2:], qt.DeepEquals, []string{
		"GRANT EXECUTE ON [rt] TO [reader]",
		"GRANT EXECUTE ON [rt] TO [auditor]",
	})
}

// Ptah writes no grant option on Oracle, and a grantee that holds one on a
// routine the plan replaces would lose the option to the DROP. The plan is
// refused rather than giving back the privilege alone, and the refusal names
// the grantee, the privilege and the routine.
func TestPlan_OracleRefusesToReplaceARoutineAGrantOptionIsHeldOn(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
		heldOnAReplacedRoutine("plsql", "DEBUG", true), "oracle")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*oracle: cannot replace procedure rt: grantee "auditor" holds EXECUTE on it WITH GRANT OPTION, `+
		`and the DROP that replaces the routine takes the option, which Ptah cannot grant back; revoke the option, `+
		`or change the routine by hand with CREATE OR REPLACE, which keeps it`)
	c.Assert(statements, qt.IsNil)
}

// A grant option the plan revokes is not one the DROP loses, so Oracle
// replaces the routine and gives back the privilege without it.
func TestPlan_OracleReplacesARoutineWhoseGrantOptionThePlanRevokes(t *testing.T) {
	c := qt.New(t)
	diff := heldOnAReplacedRoutine("plsql", "DEBUG", true)
	diff.GrantOptionsRevoked = []difftypes.GrantRef{{Role: "auditor", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt", WithOption: true}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "oracle")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"-- Replace procedure rt: body\nDROP PROCEDURE IF EXISTS rt",
		"-- Modify procedure rt: body\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN RETURN; END;",
		"GRANT EXECUTE ON rt TO reader",
		"GRANT EXECUTE ON rt TO auditor",
	})
}

// A target that hosts no routines replaces none, so the plan neither drops
// the routine nor grants anything back on it.
func TestPlan_GivesBackNothingWhereTheTargetHostsNoRoutines(t *testing.T) {
	c := qt.New(t)
	caps := capability.ForDialect(platform.SQLServer).
		With(capability.Functions, false).
		With(capability.RoutineReplacementResetsDefiner, false)

	nodes, err := mysql.NewForDialect(platform.SQLServer, caps).GenerateMigrationAST(
		context.Background(), must.Must(builtin.New()), heldOnAReplacedRoutine("sql", "CONTROL", true),
	)

	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.SQLServer, caps, nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Equals, "REVOKE CONTROL ON [rt] FROM [reader];\n")
}
