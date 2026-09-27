package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// A revoke of a role's last real privilege on an object leaves a row behind on
// MySQL and MariaDB when that privilege carried WITH GRANT OPTION:
// information_schema and SHOW GRANTS both report it as USAGE, grantable,
// because the grant option outlives the privilege that came with it. A
// comparison reads that row back as a removed USAGE grant, and REVOKE USAGE
// changes nothing -- USAGE names no privilege bit -- so a plan built from the
// plain revoke never converges. The planner reroutes it to the statement that
// actually clears the row, REVOKE GRANT OPTION, exactly as it already does for
// a kept privilege losing its option (stokaro/ptah-operator#481).
func grantOptionGhostRemoved() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{
		{Role: "reader", Privilege: "USAGE", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
	}}
}

func TestPlan_MySQLFamilyClearsAnOrphanedGrantOption(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(grantOptionGhostRemoved(), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE GRANT OPTION ON `orders` FROM `reader`",
			})
		})
	}
}

// A removed privilege that is not the USAGE ghost revokes normally: MySQL and
// MariaDB report a real privilege by its own name whether or not it carried
// the grant option, so nothing here needs the GRANT OPTION spelling.
func TestPlan_MySQLFamilyRevokesARealPrivilegeNormally(t *testing.T) {
	diff := &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{
		{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
	}}
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE SELECT ON `orders` FROM `reader`",
			})
		})
	}
}

// A USAGE row without the grant option is not the ghost this rule catches --
// nothing survives revoking it either way -- so it keeps the plain spelling.
func TestPlan_MySQLFamilyLeavesAPlainUsageRevokeAlone(t *testing.T) {
	diff := &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{
		{Role: "reader", Privilege: "USAGE", ObjectType: "TABLE", ObjectName: "orders", WithOption: false},
	}}
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE USAGE ON `orders` FROM `reader`",
			})
		})
	}
}

// A ghost on one object and a real grant-option revoke on another still plan
// as two statements; on the SAME object, they merge into one -- MySQL refuses
// a second REVOKE GRANT OPTION for one object with error 1147, measured on
// 8.4.11 and 26.7.0.
func TestPlan_MySQLFamilyMergesAGhostWithAKeptPrivilegesOptionOnTheSameObject(t *testing.T) {
	diff := &difftypes.SchemaDiff{
		GrantOptionsRevoked: []difftypes.GrantRef{
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "USAGE", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(diff, dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE GRANT OPTION ON `orders` FROM `reader`",
			})
		})
	}
}

// SQL Server and Oracle share this planner's type but not the MySQL-family
// grant-option-per-object quirk, and neither reports a bare USAGE for an
// orphaned grant option, so a USAGE removal there is left for the renderer's
// own T-SQL refusal rather than rerouted as the ghost.
func TestPlan_SQLServerDoesNotTreatUsageAsAGrantOptionGhost(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{GrantsRemoved: []difftypes.GrantRef{
		{Role: "reader", Privilege: "USAGE", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
	}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(diff, "sqlserver")

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`-- SQLSERVER: revoke from "reader" names USAGE, which T-SQL has no privilege for; nothing was granted under that name either.`,
	})
}
