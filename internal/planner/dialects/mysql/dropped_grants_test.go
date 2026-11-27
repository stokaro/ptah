package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/planext"
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

// An Oracle routine replacement keeps its grants. An explicit revoke must
// therefore run whether the body is replaced or its language is skipped.
func TestPlan_OracleRevokesADeclaredPrivilegeOnARoutineItReplaces(t *testing.T) {
	tests := []struct {
		name     string
		language string
		want     []string
	}{
		{
			name:     "a PL/SQL procedure is replaced",
			language: "plsql",
			want: []string{
				"-- Modify procedure rt: body\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN RETURN; END;",
				"REVOKE EXECUTE ON rt FROM reader",
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
// ran, measured on SQL Server 2025. The plan grants again,
// after the CREATE, every privilege the read reported on it except the one it
// revokes, a privilege of a role the schema does not manage included. MySQL
// reads report no privilege on a routine, and a table privilege of the same
// name is the table's, so the MySQL plan gives back nothing. Oracle replaces
// the routine without a DROP and needs only the explicitly requested REVOKE.
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
				"-- Modify procedure rt: body\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN RETURN; END;",
				"REVOKE DEBUG ON rt FROM reader",
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

// CREATE OR REPLACE keeps a user's observed grant option without rendering
// a grant to a grantee whose role or user classification is not known.
func TestPlan_OracleReplacesARoutineWhileKeepingGrantOptions(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
		heldOnAReplacedRoutine("plsql", "DEBUG", true), "oracle")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"-- Modify procedure rt: body\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN RETURN; END;",
		"REVOKE DEBUG ON rt FROM reader",
	})
}

// A body change cannot stand in for an unsupported revoke of only a grant
// option. Oracle keeps the option when it replaces the routine in place.
func TestPlan_OracleRefusesToRevokeOnlyAGrantOptionWhileReplacing(t *testing.T) {
	c := qt.New(t)
	diff := heldOnAReplacedRoutine("plsql", "DEBUG", true)
	diff.GrantOptionsRevoked = []difftypes.GrantRef{{Role: "auditor", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt", WithOption: true}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "oracle")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*oracle: cannot take the grant option on EXECUTE ON rt from "auditor" and leave the privilege: .*`)
	c.Assert(statements, qt.IsNil)
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

// deniedOnAReplacedRoutine is [heldOnAReplacedRoutine] with the privileges
// the read reported denied: on the replaced routine to a role and to a user,
// on a column of the routine's name, which is a column REVOKE row rather than
// a DENY, on the table t3 the schema keeps, and on a schema of the routine's
// name.
func deniedOnAReplacedRoutine(language string) *difftypes.SchemaDiff {
	diff := heldOnAReplacedRoutine(language, "CONTROL", false)
	diff.CurrentPartialRevokes = []difftypes.GrantRef{
		{Role: "r]d", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "rt"},
		{Role: "app_user", Privilege: "VIEW DEFINITION", ObjectType: "TABLE", ObjectName: "rt"},
		{Role: "r]d", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "rt", Column: "one"},
		{Role: "r]d", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t3"},
		{Role: "r]d", Privilege: "EXECUTE", ObjectType: "SCHEMA", ObjectName: "rt"},
	}
	return diff
}

// The DROP of a replaced SQL Server routine takes every DENY on it, measured
// on SQL Server 2025, and the comparison plans none. The plan denies again,
// after the grants it gives back, what the read reported denied on the
// routine, to a role or a user alike, and nothing it reported denied on
// another object.
func TestPlan_SQLServerDeniesAgainWhatWasDeniedOnAReplacedRoutine(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
		deniedOnAReplacedRoutine("sql"), "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(statements[len(statements)-4:], qt.DeepEquals, []string{
		"GRANT EXECUTE ON [rt] TO [reader]",
		"GRANT EXECUTE ON [rt] TO [auditor]",
		"DENY EXECUTE ON [rt] TO [r]]d]",
		"DENY VIEW DEFINITION ON [rt] TO [app_user]",
	})
}

// A routine named with its schema is denied on under both names, each
// bracketed the way the GRANT beside it is.
func TestPlan_SQLServerDeniesAgainOnAQualifiedRoutine(t *testing.T) {
	c := qt.New(t)
	diff := deniedOnAReplacedRoutine("sql")
	diff.FunctionsModified[0].Desired.Name = "app.rt"
	diff.CurrentGrants = []difftypes.GrantRef{{Role: "reader", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "app.rt"}}
	diff.CurrentPartialRevokes = []difftypes.GrantRef{{Role: "r]d", Privilege: "EXECUTE", ObjectType: "TABLE", ObjectName: "app.rt"}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(statements[len(statements)-2:], qt.DeepEquals, []string{
		"GRANT EXECUTE ON [app].[rt] TO [reader]",
		"DENY EXECUTE ON [app].[rt] TO [r]]d]",
	})
}

// No other target of the planner reads a partial revoke, and Oracle has no
// DENY, so an Oracle plan writes none even for a diff that carries one.
func TestPlan_OracleDeniesNothingOnAReplacedRoutine(t *testing.T) {
	c := qt.New(t)
	diff := deniedOnAReplacedRoutine("plsql")
	diff.GrantsRemoved = nil

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
		diff, "oracle")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"-- Modify procedure rt: body\nCREATE OR REPLACE PROCEDURE rt IS\nBEGIN RETURN; END;",
	})
}

// A permission name the plan would write into the DENY is the catalog's, and
// one that is not a run of upper-case words is refused rather than written.
func TestPlan_SQLServerDeniesAgainOnAReplacedRoutine_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := deniedOnAReplacedRoutine("sql")
	diff.CurrentPartialRevokes = []difftypes.GrantRef{{Role: "r_d", Privilege: "EXECUTE ON x TO y; DROP", ObjectType: "TABLE", ObjectName: "rt"}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, "sqlserver")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*sqlserver: cannot deny "EXECUTE ON x TO y; DROP" on rt to "r_d" again after the routine is replaced: `+
		`the catalog names a permission Ptah does not write.*`)
	c.Assert(statements, qt.IsNil)
}

// A DENY removes no object, so a plan in the exact style still drops a table
// it removes after the DENY without IF EXISTS, as it would with no DENY in
// front of it.
func TestPlan_SQLServerDenyLeavesALaterDropUnguarded(t *testing.T) {
	c := qt.New(t)
	diff := deniedOnAReplacedRoutine("sql")
	diff.TablesRemoved = difftypes.TableRemovals{{Name: "t1"}}
	caps := capability.ForDialect(platform.SQLServer)

	nodes, err := mysql.NewForDialect(platform.SQLServer, caps).WithStatementStyle(planext.StatementStyleExact).
		GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.SQLServer, caps, nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "DENY VIEW DEFINITION ON [rt] TO [app_user];\nDROP TABLE [t1];\n")
}
