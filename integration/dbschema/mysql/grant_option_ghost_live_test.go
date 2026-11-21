//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// grantOptionGhostRun is a database holding orders, a role holding SELECT on
// it with the grant option, and a connection to the database.
type grantOptionGhostRun struct {
	adminDB      *sql.DB
	conn         *dbschema.DatabaseConnection
	databaseName string
	role         string
	table        string
	dialect      string
	declared     *schemamodel.Database
}

// newGrantOptionGhostRun creates the database, the table and the role, and
// gives the role SELECT on the table with the grant option. The declaration
// names the table and the role but grants nothing: the role is meant to hold
// no privilege on it at all.
func newGrantOptionGhostRun(c *qt.C, dialect string, engine dbtarget.Engine) grantOptionGhostRun {
	c.Helper()
	adminDSN := dbtarget.DriverDSN(c, engine)
	adminDB := openMySQLWriterLiveDatabase(c, engine)
	c.Cleanup(func() { c.Check(adminDB.Close(), qt.IsNil) })

	suffix := time.Now().UnixNano()
	run := grantOptionGhostRun{
		adminDB:      adminDB,
		databaseName: fmt.Sprintf("ptah_ghost_%d", suffix),
		// A MySQL account name holds at most 32 characters.
		role:    fmt.Sprintf("ptah_gh_%d", suffix%1_000_000_000),
		dialect: dialect,
	}
	createMySQLWriterLiveDatabase(c, adminDB, run.databaseName)
	c.Cleanup(func() { dropMySQLWriterLiveDatabase(c, adminDB, run.databaseName) })
	execAdmin(c, adminDB, "CREATE ROLE "+sqlident.Quote(dialect, run.role))
	c.Cleanup(func() { dropRole(c, adminDB, dialect, run.role) })
	run.table = sqlident.Quote(dialect, run.databaseName) + ".`orders`"
	execAdmin(c, adminDB, "CREATE TABLE "+run.table+" (`id` INT PRIMARY KEY)")
	execAdmin(c, adminDB, "GRANT SELECT ON "+run.table+" TO "+sqlident.Quote(dialect, run.role)+" WITH GRANT OPTION")

	conn, err := dbschema.ConnectToDatabase(c.Context(), mysqlFamilyDatabaseURL(c, adminDSN, run.databaseName, dialect))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	run.conn = conn
	run.declared = &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INT", Primary: true}},
		Roles:  []schemamodel.Role{{StructName: "Reader", Name: run.role, Inherit: true}},
	}
	return run
}

// compare reads the database and compares it with the declaration.
func (r grantOptionGhostRun) compare(c *qt.C) *difftypes.SchemaDiff {
	c.Helper()
	live, err := r.conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(c.Context(), r.conn, r.declared, live, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return diff
}

// TestGrantOptionGhost_LiveConverges takes a role's last privilege on a table
// away entirely, through the plan Ptah writes, and reads what is left.
//
// Revoking SELECT from a role that held it WITH GRANT OPTION leaves a row
// behind: information_schema.TABLE_PRIVILEGES and SHOW GRANTS both report
// USAGE, grantable, because the grant option survives the privilege it came
// with. The plan revokes the grant option beside the privilege, so one apply
// leaves the role nothing on the table, and the next comparison has nothing to
// do.
//
// The admin account runs it because revoking needs the GRANT OPTION privilege,
// which the ordinary test account does not hold.
func TestGrantOptionGhost_LiveConverges(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		engine  dbtarget.Engine
	}{
		{name: "mysql", dialect: platform.MySQL, engine: dbtarget.MySQLAdmin},
		{name: "mariadb", dialect: platform.MariaDB, engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newGrantOptionGhostRun(c, test.dialect, test.engine)

			diff := run.compare(c)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
			c.Assert(diff.GrantsRemoved[0].Privilege, qt.Equals, "SELECT")

			applyPlan(c, run.conn, diff, test.dialect)
			c.Assert(tablePrivileges(c, run.adminDB, run.databaseName, run.role), qt.HasLen, 0)

			again := run.compare(c)
			c.Assert(again.GrantsAdded, qt.HasLen, 0)
			c.Assert(again.GrantsRemoved, qt.HasLen, 0)
			c.Assert(again.GrantOptionsRevoked, qt.HasLen, 0)
		})
	}
}

// TestGrantOptionGhost_LiveClearsARowLeftBehind reads the row a revoke made
// outside Ptah leaves behind: SELECT is gone, but the row survives as USAGE,
// grantable. A comparison reads USAGE as an undeclared grant to remove, and
// REVOKE USAGE changes nothing, so the naive plan reports the same removal
// forever. The planner reroutes it to REVOKE GRANT OPTION, which clears the
// row, and the next comparison has nothing to do.
func TestGrantOptionGhost_LiveClearsARowLeftBehind(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		engine  dbtarget.Engine
	}{
		{name: "mysql", dialect: platform.MySQL, engine: dbtarget.MySQLAdmin},
		{name: "mariadb", dialect: platform.MariaDB, engine: dbtarget.MariaDBAdmin},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := newGrantOptionGhostRun(c, test.dialect, test.engine)
			execAdmin(c, run.adminDB, "REVOKE SELECT ON "+run.table+" FROM "+sqlident.Quote(test.dialect, run.role))
			c.Assert(tablePrivileges(c, run.adminDB, run.databaseName, run.role), qt.DeepEquals, []string{"USAGE YES"})

			ghostDiff := run.compare(c)
			c.Assert(ghostDiff.GrantsRemoved, qt.HasLen, 1)
			c.Assert(ghostDiff.GrantsRemoved[0].Privilege, qt.Equals, "USAGE")
			c.Assert(ghostDiff.GrantsRemoved[0].WithOption, qt.IsTrue)

			applyPlan(c, run.conn, ghostDiff, test.dialect)
			c.Assert(tablePrivileges(c, run.adminDB, run.databaseName, run.role), qt.HasLen, 0)

			again := run.compare(c)
			c.Assert(again.GrantsAdded, qt.HasLen, 0)
			c.Assert(again.GrantsRemoved, qt.HasLen, 0)
			c.Assert(again.GrantOptionsRevoked, qt.HasLen, 0)
		})
	}
}

// applyPlan generates the statements a diff plans and runs every one of them.
func applyPlan(c *qt.C, conn *dbschema.DatabaseConnection, diff *difftypes.SchemaDiff, dialect string) {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		diff, dialect,
	)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}
