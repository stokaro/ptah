//go:build integration

package mysql_test

import (
	"fmt"
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
	"ptah.run/migration/schemadiff"
)

// TestGrantOptionConflict_LiveRefusesBeforeGranting takes two declarations for
// one role and table that disagree on WITH GRANT OPTION through
// [schemadiff.CompareWithDatabase], the live entry point every caller with a
// connection reaches, and checks the refusal happens before either statement
// is even planned: MySQL and MariaDB keep the grant option at the whole
// object, so the pair can never both hold and a plan built from them would
// alternate between granting the option back and revoking it forever
// (stokaro/ptah-operator#481).
func TestGrantOptionConflict_LiveRefusesBeforeGranting(t *testing.T) {
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
			databaseName := fmt.Sprintf("ptah_optconf_%d", suffix)
			role := fmt.Sprintf("ptah_oc_%d", suffix%1_000_000_000)
			createMySQLWriterLiveDatabase(c, adminDB, databaseName)
			c.Cleanup(func() { dropMySQLWriterLiveDatabase(c, adminDB, databaseName) })
			execAdmin(c, adminDB, "CREATE ROLE "+sqlident.Quote(test.dialect, role))
			c.Cleanup(func() { dropRole(c, adminDB, test.dialect, role) })
			execAdmin(c, adminDB, "CREATE TABLE "+sqlident.Quote(test.dialect, databaseName)+".`orders` (`id` INT PRIMARY KEY)")

			conn, err := dbschema.ConnectToDatabase(c.Context(), mysqlFamilyDatabaseURL(c, adminDSN, databaseName, test.dialect))
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
			// Two declarations for the same role and table that disagree on
			// the grant option.
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
				Fields: []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INT", Primary: true}},
				Roles:  []schemamodel.Role{{StructName: "Reader", Name: role, Inherit: true}},
				Grants: []schemamodel.Grant{
					{StructName: "Reader", Role: role, Privileges: []string{"SELECT"}, OnTable: "orders"},
					{StructName: "Reader", Role: role, Privileges: []string{"INSERT"}, OnTable: "orders", WithOption: true},
				},
			}

			live, err := conn.Reader().ReadSchemaContext(c.Context())
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabase(c.Context(), conn, desired, live, nil, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorMatches, `.*is granted INSERT on TABLE orders WITH GRANT OPTION and SELECT on the same object without it.*`)
			c.Assert(diff, qt.IsNil)
			// The refusal happened before anything was planned or run: the
			// role holds nothing on the table.
			c.Assert(tablePrivileges(c, adminDB, databaseName, role), qt.HasLen, 0)
		})
	}
}
