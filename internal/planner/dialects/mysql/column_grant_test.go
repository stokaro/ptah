package mysql_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// columnGrantsChanged is a diff that grants SELECT on one column, revokes
// INSERT on another, adds the grant option to UPDATE on a third, and takes the
// grant option away from two column privileges of a second table.
func columnGrantsChanged() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name"},
		},
		GrantOptionsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "secret"},
		},
		GrantOptionsRevoked: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "a", WithOption: true},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "b", WithOption: true},
		},
	}
}

// tableGrantNarrowed is a diff that replaces SELECT on the whole of t1 with
// SELECT on its name column, while the role keeps SELECT and INSERT on the id
// column, INSERT with the grant option, and SELECT on a column of t2. The
// grant option comes off t1 and stays on t2.
func tableGrantNarrowed() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "name"},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1"},
		},
		GrantOptionsRevoked: []difftypes.GrantRef{
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
			{Role: "reader", Privilege: "INSERT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t2", Column: "a", WithOption: true},
			{Role: "other", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id"},
		},
	}
}

// columnGrantWidened is a diff that replaces SELECT on the name column of t1,
// held with the grant option, with SELECT on the whole of t1 without it.
func columnGrantWidened() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsAdded: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
		},
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name", WithOption: true},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "app.t1", Column: "name", WithOption: true},
		},
	}
}

// tableGrantRevokedAll is a diff that revokes ALL on t1 from a role that
// keeps UPDATE on a column of t1 with the grant option.
func tableGrantRevokedAll() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		GrantsRemoved: []difftypes.GrantRef{
			{Role: "reader", Privilege: "ALL", ObjectType: "TABLE", ObjectName: "app.t1"},
		},
		CurrentGrants: []difftypes.GrantRef{
			{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "app.t1", Column: "id", WithOption: true},
		},
	}
}

// A grant limited to a column is planned on that column and no wider, and the
// revokes go first: on MySQL and MariaDB a revoke on a table takes the column
// privileges of the same kind, and a revoke of the grant option takes it from
// every privilege on the table. The grant option of a column privilege is the
// grantee's at the table, so the two columns of t2 losing it plan one
// statement for t2, and a revoked privilege that carried it takes it along
// where the role keeps no privilege with it. A column privilege the role keeps comes back after the
// table revoke that takes it, with its grant option unless the diff revokes
// the option on that table; a privilege the table revoke does not take, or
// another role holds, is left alone.
func TestPlan_MySQLFamilyColumnGrants_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff func() *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "column grants changed",
			diff: columnGrantsChanged,
			want: []string{
				"REVOKE GRANT OPTION ON `app`.`t2` FROM `reader`",
				"REVOKE INSERT (`secret`) ON `app`.`t1` FROM `reader`",
				"GRANT SELECT (`name`) ON `app`.`t1` TO `reader`",
				"GRANT UPDATE (`id`) ON `app`.`t1` TO `reader` WITH GRANT OPTION",
			},
		},
		{
			name: "a table grant narrowed to a column",
			diff: tableGrantNarrowed,
			want: []string{
				"REVOKE GRANT OPTION ON `app`.`t1` FROM `reader`",
				"REVOKE SELECT ON `app`.`t1` FROM `reader`",
				"REVOKE INSERT ON `app`.`t1` FROM `reader`",
				"GRANT SELECT (`name`) ON `t1` TO `reader`",
				"GRANT SELECT (`id`) ON `app`.`t1` TO `reader`",
				"GRANT INSERT (`id`) ON `app`.`t1` TO `reader`",
			},
		},
		{
			name: "a column grant with the option widened to the table without it",
			diff: columnGrantWidened,
			want: []string{
				"REVOKE GRANT OPTION ON `app`.`t1` FROM `reader`",
				"REVOKE SELECT (`name`) ON `app`.`t1` FROM `reader`",
				"GRANT SELECT ON `t1` TO `reader`",
			},
		},
		{
			name: "ALL revoked from a table",
			diff: tableGrantRevokedAll,
			want: []string{
				"REVOKE ALL ON `app`.`t1` FROM `reader`",
				"GRANT UPDATE (`id`) ON `app`.`t1` TO `reader` WITH GRANT OPTION",
			},
		},
	}
	for _, dialect := range []string{"mysql", "mariadb"} {
		for _, test := range tests {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := planner.GenerateSchemaDiffSQLStatements(
					context.Background(), must.Must(builtin.New()), test.diff(), dialect,
				)

				c.Assert(err, qt.IsNil)
				c.Assert(statements, qt.DeepEquals, test.want)
			})
		}
	}
}

// SQL Server and Oracle share this planner and have no column grant spelling
// here, so a plan holding one is refused by name rather than granted on the
// whole table.
func TestPlan_MySQLFamilyColumnGrants_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		wantErr string
	}{
		{dialect: "sqlserver", wantErr: `(?s).*column privileges are modeled for PostgreSQL, MySQL and MariaDB only, not sqlserver.*`},
		{dialect: "oracle", wantErr: `(?s).*column privileges are modeled for PostgreSQL, MySQL and MariaDB only, not oracle.*`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{
				{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Column: "name"},
			}}

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), diff, test.dialect,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
