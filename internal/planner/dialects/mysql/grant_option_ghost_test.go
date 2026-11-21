package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
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

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()),
				grantOptionGhostRemoved(), dialect,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{
				"REVOKE GRANT OPTION ON `orders` FROM `reader`",
			})
		})
	}
}

// A removed privilege that is not the USAGE ghost revokes by its own name.
// Where it carried the grant option and the role keeps no privilege with the
// option on the object, the option goes first: a privilege revoke leaves it,
// and the role is left with the USAGE ghost the next plan has to clear. Where
// the role keeps a privilege with the option there, the option stays.
func TestPlan_MySQLFamilyRevokesARealPrivilegeNormally(t *testing.T) {
	tests := []struct {
		name    string
		current []difftypes.GrantRef
		want    []string
	}{
		{
			name: "the last privilege with the option",
			want: []string{
				"REVOKE GRANT OPTION ON `orders` FROM `reader`",
				"REVOKE SELECT ON `orders` FROM `reader`",
			},
		},
		{
			name: "a kept privilege holds the option",
			current: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
				{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
			},
			want: []string{"REVOKE SELECT ON `orders` FROM `reader`"},
		},
		{
			name: "another role holds the option",
			current: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
				{Role: "writer", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
			},
			want: []string{
				"REVOKE GRANT OPTION ON `orders` FROM `reader`",
				"REVOKE SELECT ON `orders` FROM `reader`",
			},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				diff := &difftypes.SchemaDiff{
					GrantsRemoved: []difftypes.GrantRef{
						{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "orders", WithOption: true},
					},
					CurrentGrants: test.current,
				}

				statements, err := planner.GenerateSchemaDiffSQLStatements(
					context.Background(), must.Must(builtin.New()),
					diff, dialect,
				)

				c.Assert(err, qt.IsNil)
				c.Assert(statements, qt.DeepEquals, test.want)
			})
		}
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

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()),
				diff, dialect,
			)

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

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()),
				diff, dialect,
			)

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

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		context.Background(), must.Must(builtin.New()),
		diff, "sqlserver",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.DeepEquals, []string{
		`-- SQLSERVER: revoke from "reader" names USAGE, which T-SQL has no privilege for; nothing was granted under that name either.`,
	})
}
