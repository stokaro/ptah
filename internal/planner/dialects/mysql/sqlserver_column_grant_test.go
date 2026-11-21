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

// held is a diff that takes the privileges current away from the role reader
// and gives it added, while the database holds current.
func held(current, removed, added []difftypes.GrantRef) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{GrantsRemoved: removed, GrantsAdded: added, CurrentGrants: current}
}

// TestPlan_SQLServerColumnPermissions_HappyPath pins the order and the
// statements of a SQL Server plan that changes column permissions.
//
// A permission on the whole table takes the place of the column permissions of
// the same name there, and a column revoke under one withholds the column, so
// the revokes run first: granting SELECT on t1 before revoking SELECT (id)
// would leave a REVOKE row that keeps the role from reading id. A grantable
// permission loses its option before the permission, because SQL Server
// refuses a revoke of one without CASCADE.
func TestPlan_SQLServerColumnPermissions_HappyPath(t *testing.T) {
	columnID := difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t1", Column: "id"}
	table := difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t1"}
	grantableTable := table
	grantableTable.WithOption = true
	grantableColumn := difftypes.GrantRef{Role: "reader", Privilege: "UPDATE", ObjectType: "TABLE", ObjectName: "dbo.t1", Column: "name", WithOption: true}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "a column permission widened to the table",
			diff: held([]difftypes.GrantRef{columnID}, []difftypes.GrantRef{columnID}, []difftypes.GrantRef{table}),
			want: []string{
				"REVOKE SELECT ([id]) ON [dbo].[t1] FROM [reader]",
				"GRANT SELECT ON [dbo].[t1] TO [reader]",
			},
		},
		{
			name: "a table permission narrowed to a column",
			diff: held([]difftypes.GrantRef{table}, []difftypes.GrantRef{table}, []difftypes.GrantRef{columnID}),
			want: []string{
				"REVOKE SELECT ON [dbo].[t1] FROM [reader]",
				"GRANT SELECT ([id]) ON [dbo].[t1] TO [reader]",
			},
		},
		{
			name: "a grantable table permission revoked",
			diff: held([]difftypes.GrantRef{grantableTable}, []difftypes.GrantRef{grantableTable}, nil),
			want: []string{
				"REVOKE GRANT OPTION FOR SELECT ON [dbo].[t1] FROM [reader] CASCADE",
				"REVOKE SELECT ON [dbo].[t1] FROM [reader]",
			},
		},
		{
			name: "a grantable column permission revoked",
			diff: held([]difftypes.GrantRef{grantableColumn}, []difftypes.GrantRef{grantableColumn}, nil),
			want: []string{
				"REVOKE GRANT OPTION FOR UPDATE ([name]) ON [dbo].[t1] FROM [reader] CASCADE",
				"REVOKE UPDATE ([name]) ON [dbo].[t1] FROM [reader]",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), test.diff, "sqlserver",
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
