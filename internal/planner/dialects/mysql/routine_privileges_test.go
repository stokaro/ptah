package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
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

// replacedProcedure is a diff that changes the body of the procedure name,
// while the read found privileges on routines.
func replacedProcedure(name string, privileges ...catalog.RoutineGrant) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		FunctionsModified: []difftypes.FunctionDiff{{
			FunctionName: name,
			Changes:      map[string]string{"body": "BEGIN SELECT 1; END -> BEGIN SELECT 2; END"},
			Desired: schemamodel.Function{
				Name: name, Kind: schemamodel.FunctionKindProcedure, Language: "sql", Body: "BEGIN SELECT 2; END",
			},
		}},
		CurrentRoutinePrivileges: catalog.RoutinePrivileges{Grants: privileges},
	}
}

// heldOnShopRt is what the read found granted on routines of the database
// shop: on the procedure rt to a role, a user and a MariaDB role, and on
// routines the plan does not replace.
func heldOnShopRt() []catalog.RoutineGrant {
	return []catalog.RoutineGrant{
		{User: "app_r", Host: "%", Schema: "shop", Name: "RT", Type: "PROCEDURE", Privileges: []string{"EXECUTE", "ALTER ROUTINE"}, WithOption: true},
		{User: "svc", Host: "10.1.%", Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}},
		{User: "m_r", Role: true, Schema: "shop", Name: "rt", Type: "PROCEDURE", WithOption: true},
		{User: "svc", Host: "%", Schema: "shop", Name: "rt", Type: "FUNCTION", Privileges: []string{"EXECUTE"}},
		{User: "svc", Host: "%", Schema: "shop", Name: "rt2", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}},
	}
}

// The DROP that replaces a MySQL or MariaDB routine deletes its rows of
// mysql.procs_priv. The plan grants each one again after the CREATE, to a
// user with its host and to a MariaDB role by its name, with the grant option
// where the row held it, and leaves alone a function of the same name and
// another procedure.
func TestPlan_GivesBackThePrivilegesOnAReplacedMySQLRoutine(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
				replacedProcedure("rt", heldOnShopRt()...), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"-- Replace procedure rt: body\nDROP PROCEDURE IF EXISTS `rt`",
				"-- Modify procedure rt: body\nCREATE PROCEDURE `rt`() READS SQL DATA BEGIN SELECT 2; END",
				"GRANT EXECUTE, ALTER ROUTINE ON PROCEDURE `shop`.`rt` TO `app_r`@`%` WITH GRANT OPTION",
				"GRANT EXECUTE ON PROCEDURE `shop`.`rt` TO `svc`@`10.1.%`",
				"GRANT USAGE ON PROCEDURE `shop`.`rt` TO `m_r` WITH GRANT OPTION",
			})
		})
	}
}

// A routine named with its database takes back only the rows of that
// database.
func TestPlan_GivesBackThePrivilegesOnAQualifiedMySQLRoutine(t *testing.T) {
	c := qt.New(t)
	diff := replacedProcedure("shop.rt",
		catalog.RoutineGrant{User: "svc", Host: "%", Schema: "elsewhere", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}},
		catalog.RoutineGrant{User: "svc", Host: "%", Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE"}},
	)

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(statements[2:], qt.DeepEquals, []string{"GRANT EXECUTE ON PROCEDURE `shop`.`rt` TO `svc`@`%`"})
}

// The other targets of the planner read no mysql.procs_priv, and an Oracle or
// SQL Server plan writes no MySQL GRANT even for a diff that carries the rows.
func TestPlan_GivesBackNoMySQLRoutinePrivilegeOnAnotherTarget(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()),
		replacedProcedure("rt", heldOnShopRt()...), platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
}

// A GRANT removes no object, so a plan in the exact style still drops a table
// it removes after the GRANT without IF EXISTS.
func TestPlan_MySQLRoutineGrantLeavesALaterDropUnguarded(t *testing.T) {
	c := qt.New(t)
	diff := replacedProcedure("rt", heldOnShopRt()...)
	diff.TablesRemoved = difftypes.TableRemovals{{Name: "t1"}}
	caps := capability.ForDialect(platform.MySQL)

	nodes, err := mysql.NewForDialect(platform.MySQL, caps).WithStatementStyle(planext.StatementStyleExact).
		GenerateMigrationAST(context.Background(), must.Must(builtin.New()), diff)

	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.MySQL, caps, nodes...)
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "TO `m_r` WITH GRANT OPTION;\nDROP TABLE `t1`;\n")
}

// A read refused mysql.procs_priv leaves the privileges on a routine unknown,
// and the DROP that replaces it would take them without a word, so the plan
// is refused and the refusal names the routine and the privilege the account
// lacks.
func TestPlan_RefusesToReplaceAMySQLRoutineWhosePrivilegesWereNotRead(t *testing.T) {
	c := qt.New(t)
	diff := replacedProcedure("rt")
	diff.CurrentRoutinePrivileges = catalog.RoutinePrivileges{Refused: true}

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, platform.MariaDB)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*mariadb: cannot replace procedure rt: the account may not read mysql.procs_priv, `+
		`so nothing says who holds privileges on the routine, and the DROP that replaces it takes them all; `+
		`grant the account SELECT on mysql.procs_priv`)
	c.Assert(statements, qt.IsNil)
}

// A refused read does not stand in the way of a plan that replaces no
// routine.
func TestPlan_ChangesATableWhereThePrivilegesOnRoutinesWereNotRead(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		TablesRemoved:            difftypes.TableRemovals{{Name: "t1"}},
		CurrentRoutinePrivileges: catalog.RoutinePrivileges{Refused: true},
	}

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, platform.MySQL)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{"-- WARNING: This will delete all data!\nDROP TABLE IF EXISTS `t1`"})
}

// A privilege name the plan would write into the GRANT is the server's, and
// one that is not a run of upper-case words is refused rather than written.
func TestPlan_GivesBackThePrivilegesOnAReplacedMySQLRoutine_FailurePath(t *testing.T) {
	c := qt.New(t)
	diff := replacedProcedure("rt", catalog.RoutineGrant{
		User: "svc", Host: "%", Schema: "shop", Name: "rt", Type: "PROCEDURE", Privileges: []string{"EXECUTE ON *.* TO x; DROP"},
	})

	statements, err := planner.GenerateSchemaDiffSQLStatements(context.Background(), must.Must(builtin.New()), diff, platform.MySQL)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*mysql: cannot grant "EXECUTE ON \*\.\* TO x; DROP" on procedure rt to "svc" again `+
		`after the routine is replaced: the catalog names a privilege Ptah does not write.*`)
	c.Assert(statements, qt.IsNil)
}
