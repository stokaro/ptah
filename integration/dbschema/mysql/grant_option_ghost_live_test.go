//go:build integration

package mysql_test

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestGrantOptionGhost_LiveConverges takes a role's last privilege on a table
// away entirely, through the plan Ptah writes, and reads what is left twice.
//
// Revoking SELECT from a role that held it WITH GRANT OPTION leaves a row
// behind: information_schema.TABLE_PRIVILEGES and SHOW GRANTS both report
// USAGE, grantable, because the grant option survives the privilege it came
// with. A comparison against that state reads USAGE as an undeclared grant to
// remove, and REVOKE USAGE changes nothing, so the naive plan reports the same
// removal forever. The planner reroutes it to REVOKE GRANT OPTION, which
// clears the row, and a third comparison has nothing left to do.
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
			adminDSN := dbtarget.DriverDSN(c, test.engine)
			adminDB := openMySQLWriterLiveDatabase(c, test.engine)
			c.Cleanup(func() { c.Check(adminDB.Close(), qt.IsNil) })

			suffix := time.Now().UnixNano()
			databaseName := fmt.Sprintf("ptah_ghost_%d", suffix)
			// A MySQL account name holds at most 32 characters.
			role := fmt.Sprintf("ptah_gh_%d", suffix%1_000_000_000)
			createMySQLWriterLiveDatabase(c, adminDB, databaseName)
			c.Cleanup(func() { dropMySQLWriterLiveDatabase(c, adminDB, databaseName) })
			execAdmin(c, adminDB, "CREATE ROLE "+sqlident.Quote(test.dialect, role))
			c.Cleanup(func() { dropRole(c, adminDB, test.dialect, role) })
			table := sqlident.Quote(test.dialect, databaseName) + ".`orders`"
			execAdmin(c, adminDB, "CREATE TABLE "+table+" (`id` INT PRIMARY KEY)")
			execAdmin(c, adminDB, "GRANT SELECT ON "+table+" TO "+sqlident.Quote(test.dialect, role)+" WITH GRANT OPTION")

			conn, err := dbschema.ConnectToDatabase(c.Context(), mysqlFamilyDatabaseURL(c, adminDSN, databaseName, test.dialect))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			// The declaration names the table and the role but grants nothing:
			// the role is meant to hold no privilege on it at all.
			declared := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
				Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INT", Primary: true}},
				Roles:  []schemamodel.Role{{StructName: "Reader", Name: role}},
			}

			live, err := conn.Reader().ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 1)
			c.Assert(diff.GrantsRemoved[0].Privilege, qt.Equals, "SELECT")

			applyPlan(c, conn, diff, test.dialect)
			c.Assert(tablePrivileges(c, adminDB, databaseName, role), qt.DeepEquals, []string{"USAGE YES"})

			// The ghost: SELECT is gone, but the row survives as USAGE,
			// grantable, and a plain REVOKE USAGE would leave it exactly as
			// it is.
			ghostRead, err := conn.Reader().ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)
			ghostDiff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, ghostRead, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(ghostDiff.GrantsRemoved, qt.HasLen, 1)
			c.Assert(ghostDiff.GrantsRemoved[0].Privilege, qt.Equals, "USAGE")
			c.Assert(ghostDiff.GrantsRemoved[0].WithOption, qt.IsTrue)

			applyPlan(c, conn, ghostDiff, test.dialect)
			c.Assert(tablePrivileges(c, adminDB, databaseName, role), qt.HasLen, 0)

			settled, err := conn.Reader().ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)
			again, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, settled, nil)
			c.Assert(err, qt.IsNil)
			c.Assert(again.GrantsAdded, qt.HasLen, 0)
			c.Assert(again.GrantsRemoved, qt.HasLen, 0)
			c.Assert(again.GrantOptionsRevoked, qt.HasLen, 0)
		})
	}
}

// applyPlan generates the statements a diff plans and runs every one of them.
func applyPlan(c *qt.C, conn *dbschema.DatabaseConnection, diff *difftypes.SchemaDiff, dialect string) {
	c.Helper()
	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, dialect)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}
