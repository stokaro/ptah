package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// grantOptionsRevoked is a diff that takes the grant option away from SELECT
// and INSERT on one table and from SELECT on a second, and revokes nothing
// else.
func grantOptionsRevoked() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{GrantOptionsRevoked: []difftypes.GrantRef{
		{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "customers", WithOption: true},
	}}
}

// On MySQL and MariaDB the grant option belongs to the grantee at one object,
// so a diff that takes it away from several privileges on one object plans one
// statement for that object. Measured on MySQL 8.4.11 and 26.7.0, a second
// `REVOKE GRANT OPTION ON db.t FROM r` for the same object is error 1147, there
// is no such grant defined, and the plan stops there. No statement revokes a
// privilege: the diff asks to keep every one of them.
func TestPlan_MySQLFamilyRevokesEachGrantOptionOncePerObject(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(grantOptionsRevoked(), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE GRANT OPTION ON `orders` FROM `reader`",
				"REVOKE GRANT OPTION ON `customers` FROM `reader`",
			})
		})
	}
}

// SQL Server shares this planner and names the privilege in its own form,
// `REVOKE GRANT OPTION FOR SELECT ...`, so it keeps one statement per privilege.
func TestPlan_SQLServerRevokesTheGrantOptionPerPrivilege(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(grantOptionsRevoked(), "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		"REVOKE GRANT OPTION FOR INSERT ON [orders] FROM [reader] CASCADE",
		"REVOKE GRANT OPTION FOR SELECT ON [orders] FROM [reader] CASCADE",
		"REVOKE GRANT OPTION FOR SELECT ON [customers] FROM [reader] CASCADE",
	})
}
