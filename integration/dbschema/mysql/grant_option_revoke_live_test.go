//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestGrantOptionRevoke_LiveKeepsThePrivileges takes the grant option away from
// a role that holds SELECT and INSERT on a table WITH GRANT OPTION, through the
// plan Ptah writes, and reads back what the role holds.
//
// The role keeps both privileges and neither is grantable, and a second
// comparison has nothing left to do. Without the GRANT OPTION spelling the plan
// revokes SELECT and INSERT themselves, and the next comparison plans to grant
// them again. Without one statement per table, MySQL refuses the second
// `REVOKE GRANT OPTION` for the same table with error 1147 and the plan stops.
//
// The admin account runs it because revoking needs the GRANT OPTION privilege,
// which the ordinary test account does not hold.
func TestGrantOptionRevoke_LiveKeepsThePrivileges(t *testing.T) {
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
			databaseName := fmt.Sprintf("ptah_grantopt_%d", suffix)
			// A MySQL account name holds at most 32 characters.
			role := fmt.Sprintf("ptah_go_%d", suffix%1_000_000_000)
			createMySQLWriterLiveDatabase(c, adminDB, databaseName)
			c.Cleanup(func() { dropMySQLWriterLiveDatabase(c, adminDB, databaseName) })
			execAdmin(c, adminDB, "CREATE ROLE "+sqlident.Quote(test.dialect, role))
			c.Cleanup(func() { dropRole(c, adminDB, test.dialect, role) })
			table := sqlident.Quote(test.dialect, databaseName) + ".`orders`"
			execAdmin(c, adminDB, "CREATE TABLE "+table+" (`id` INT PRIMARY KEY)")
			execAdmin(c, adminDB, "GRANT SELECT, INSERT ON "+table+" TO "+sqlident.Quote(test.dialect, role)+" WITH GRANT OPTION")

			conn, err := dbschema.ConnectToDatabase(c.Context(), mysqlFamilyDatabaseURL(c, adminDSN, databaseName, test.dialect))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			declared := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
				Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INT", Primary: true}},
				Roles:  []schemamodel.Role{{StructName: "Reader", Name: role, Inherit: true}},
				Grants: []schemamodel.Grant{{StructName: "Reader", Role: role, Privileges: []string{"SELECT", "INSERT"}, OnTable: "orders"}},
			}

			live, err := conn.Reader().ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, live, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(diff.GrantOptionsRevoked, qt.HasLen, 2)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()),
				diff, test.dialect,
			)
			c.Assert(err, qt.IsNil)
			for _, statement := range statements {
				c.Assert(conn.Writer().ExecuteSQL(c.Context(), statement), qt.IsNil, qt.Commentf("statement:\n%s", statement))
			}

			c.Assert(tablePrivileges(c, adminDB, databaseName, role), qt.DeepEquals, []string{"INSERT NO", "SELECT NO"})
			settled, err := conn.Reader().ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)
			again, err := schemadiff.CompareWithDatabase(c.Context(), conn, declared, settled, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			c.Assert(again.GrantOptionsRevoked, qt.HasLen, 0)
			c.Assert(again.GrantsAdded, qt.HasLen, 0)
			c.Assert(again.GrantsRemoved, qt.HasLen, 0)
		})
	}
}

// execAdmin runs one statement through the admin connection.
func execAdmin(c *qt.C, db *sql.DB, statement string) {
	c.Helper()
	_, err := db.ExecContext(c.Context(), statement)
	c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
}

// dropRole drops the role on a context of its own, because the test's context
// is already canceled when cleanup runs.
func dropRole(c *qt.C, db *sql.DB, dialect, role string) {
	c.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	_, err := db.ExecContext(cleanupCtx, "DROP ROLE IF EXISTS "+sqlident.Quote(dialect, role))
	c.Check(err, qt.IsNil)
}

// tablePrivileges lists what a role holds on the tables of one database, as
// `PRIVILEGE IS_GRANTABLE`, in privilege order.
func tablePrivileges(c *qt.C, db *sql.DB, databaseName, role string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(),
		"SELECT PRIVILEGE_TYPE, IS_GRANTABLE FROM information_schema.TABLE_PRIVILEGES "+
			"WHERE TABLE_SCHEMA = ? AND GRANTEE LIKE ? ORDER BY PRIVILEGE_TYPE",
		databaseName, "'"+role+"'%")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var held []string
	for rows.Next() {
		var privilege, grantable string
		c.Assert(rows.Scan(&privilege, &grantable), qt.IsNil)
		held = append(held, strings.Join([]string{privilege, grantable}, " "))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return held
}
