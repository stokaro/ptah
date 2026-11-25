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

// onT1 is privilege on dbo.t1 for reader, on column where one is named.
func onT1(privilege, column string) difftypes.GrantRef {
	return difftypes.GrantRef{Role: "reader", Privilege: privilege, ObjectType: "TABLE", ObjectName: "dbo.t1", Column: column}
}

// grantable is ref WITH GRANT OPTION.
func grantable(ref difftypes.GrantRef) difftypes.GrantRef {
	ref.WithOption = true
	return ref
}

// deniedRow is ref as a DENY row the read reports among its partial revokes.
func deniedRow(ref difftypes.GrantRef) difftypes.GrantRef {
	ref.Denied = true
	return ref
}

// onT1To is onT1 for another grantee.
func onT1To(role, privilege, column string) difftypes.GrantRef {
	ref := onT1(privilege, column)
	ref.Role = role
	return ref
}

// TestPlan_SQLServerDenies_HappyPath pins the statements and the order of a
// SQL Server plan that denies a permission or takes a DENY away.
//
// The server keeps one row for a permission of one grantee on one securable,
// and a statement on the whole table reaches the rows of its columns, measured
// on SQL Server 2025. So a DENY on the table runs before the grants on its
// columns and a DENY on a column after the grant on its table, and a column
// grant a DENY on its table takes is granted again. A DENY of a permission the
// grantee holds WITH GRANT OPTION, on the securable or on a column a DENY on
// the table reaches, needs CASCADE, and one on another column does not.
//
// A GRANT or a REVOKE on the table takes the column DENY rows of its
// permission, so the plan denies again the ones the database holds and the
// diff keeps, and a column DENY under a table grant the diff keeps is taken
// with a GRANT on the column, which leaves the table's row alone where a
// REVOKE leaves a row that withholds the column.
func TestPlan_SQLServerDenies_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "a deny on a table",
			diff: &difftypes.SchemaDiff{DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "")}},
			want: []string{"DENY SELECT ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a column",
			diff: &difftypes.SchemaDiff{DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "secret")}},
			want: []string{"DENY SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a table and a grant on its column",
			diff: &difftypes.SchemaDiff{DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "")}, GrantsAdded: []difftypes.GrantRef{onT1("SELECT", "name")}},
			want: []string{"DENY SELECT ON [dbo].[t1] TO [reader]", "GRANT SELECT ([name]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a grant on a table and a deny on its column",
			diff: &difftypes.SchemaDiff{GrantsAdded: []difftypes.GrantRef{onT1("SELECT", "")}, DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "secret")}},
			want: []string{"GRANT SELECT ON [dbo].[t1] TO [reader]", "DENY SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a table over a column grant the schema keeps",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentGrants: []difftypes.GrantRef{onT1("SELECT", "name")},
				GrantsDeniesTake: []difftypes.GrantRef{onT1("SELECT", "name")},
			},
			want: []string{"DENY SELECT ON [dbo].[t1] TO [reader]", "GRANT SELECT ([name]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a table over a column grant the schema does not declare",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentGrants: []difftypes.GrantRef{onT1("SELECT", "name"), onT1("SELECT", "secret")},
			},
			want: []string{"DENY SELECT ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a table over a grantable column grant",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", "name"))},
				GrantsDeniesTake: []difftypes.GrantRef{grantable(onT1("SELECT", "name"))},
			},
			want: []string{"DENY SELECT ON [dbo].[t1] TO [reader] CASCADE", "GRANT SELECT ([name]) ON [dbo].[t1] TO [reader] WITH GRANT OPTION"},
		},
		{
			name: "a deny on a column under a grantable table grant",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("REFERENCES", "secret")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("REFERENCES", ""))},
			},
			want: []string{"DENY REFERENCES ([secret]) ON [dbo].[t1] TO [reader] CASCADE"},
		},
		{
			name: "a deny on a column beside a grantable grant on another column",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("UPDATE", "secret")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("UPDATE", "name"))},
			},
			want: []string{"DENY UPDATE ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny of a grantable permission the plan revokes",
			diff: &difftypes.SchemaDiff{
				DeniesAdded:   []difftypes.GrantRef{onT1("SELECT", "")},
				CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", "name"))}, GrantsRemoved: []difftypes.GrantRef{grantable(onT1("SELECT", "name"))},
			},
			want: []string{
				"REVOKE GRANT OPTION FOR SELECT ([name]) ON [dbo].[t1] FROM [reader] CASCADE",
				"REVOKE SELECT ([name]) ON [dbo].[t1] FROM [reader]",
				"DENY SELECT ON [dbo].[t1] TO [reader]",
			},
		},
		{
			name: "a deny on a table taken away",
			diff: &difftypes.SchemaDiff{DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "")}},
			want: []string{"REVOKE SELECT ON [dbo].[t1] FROM [reader]"},
		},
		{
			name: "a deny on a table taken away under a column grant the schema keeps",
			diff: &difftypes.SchemaDiff{DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentGrants: []difftypes.GrantRef{onT1("SELECT", "name")}},
			want: []string{"REVOKE SELECT ON [dbo].[t1] FROM [reader]", "GRANT SELECT ([name]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a column taken away",
			diff: &difftypes.SchemaDiff{DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "secret")}},
			want: []string{"REVOKE SELECT ([secret]) ON [dbo].[t1] FROM [reader]"},
		},
		{
			name: "a deny on a column taken away under a table grant the diff keeps",
			diff: &difftypes.SchemaDiff{DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "secret")}, CurrentGrants: []difftypes.GrantRef{onT1("SELECT", "")}},
			want: []string{"GRANT SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a column taken away under a grantable table grant",
			diff: &difftypes.SchemaDiff{DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "secret")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", ""))}},
			want: []string{"GRANT SELECT ([secret]) ON [dbo].[t1] TO [reader] WITH GRANT OPTION"},
		},
		{
			name: "a deny on a column taken away under a table grant that loses the option",
			diff: &difftypes.SchemaDiff{
				DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "secret")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", ""))},
				GrantOptionsRevoked: []difftypes.GrantRef{grantable(onT1("SELECT", ""))},
			},
			want: []string{"REVOKE GRANT OPTION FOR SELECT ON [dbo].[t1] FROM [reader] CASCADE", "GRANT SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a deny on a column taken away under a table grant the plan revokes",
			diff: &difftypes.SchemaDiff{
				DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "secret")}, CurrentGrants: []difftypes.GrantRef{onT1("SELECT", "")},
				GrantsRemoved: []difftypes.GrantRef{onT1("SELECT", "")},
			},
			want: []string{"REVOKE SELECT ON [dbo].[t1] FROM [reader]", "REVOKE SELECT ([secret]) ON [dbo].[t1] FROM [reader]"},
		},
		{
			name: "a grant on a table over a column deny the database holds",
			diff: &difftypes.SchemaDiff{
				GrantsAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1("SELECT", "secret"))},
			},
			want: []string{"GRANT SELECT ON [dbo].[t1] TO [reader]", "DENY SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a grantable grant on a table over a column deny the database holds",
			diff: &difftypes.SchemaDiff{
				GrantsAdded: []difftypes.GrantRef{grantable(onT1("SELECT", ""))}, CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1("SELECT", "secret"))},
			},
			want: []string{"GRANT SELECT ON [dbo].[t1] TO [reader] WITH GRANT OPTION", "DENY SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a grant option added on a table over a column deny the database holds",
			diff: &difftypes.SchemaDiff{
				GrantOptionsAdded: []difftypes.GrantRef{grantable(onT1("SELECT", ""))}, CurrentGrants: []difftypes.GrantRef{onT1("SELECT", "")},
				CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1("SELECT", "secret"))},
			},
			want: []string{"GRANT SELECT ON [dbo].[t1] TO [reader] WITH GRANT OPTION", "DENY SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a revoke on a table over a column deny the database holds",
			diff: &difftypes.SchemaDiff{
				GrantsRemoved: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentGrants: []difftypes.GrantRef{onT1("SELECT", "")},
				CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1("SELECT", "secret"))},
			},
			want: []string{"REVOKE SELECT ON [dbo].[t1] FROM [reader]", "DENY SELECT ([secret]) ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a grant option revoked on a table keeps the column deny",
			diff: &difftypes.SchemaDiff{
				GrantOptionsRevoked: []difftypes.GrantRef{grantable(onT1("SELECT", ""))}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", ""))},
				CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1("SELECT", "secret"))},
			},
			want: []string{"REVOKE GRANT OPTION FOR SELECT ON [dbo].[t1] FROM [reader] CASCADE"},
		},
		{
			name: "a grant on a table over a column deny the diff takes away",
			diff: &difftypes.SchemaDiff{
				GrantsAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1("SELECT", "secret"))},
				DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "secret")},
			},
			want: []string{"REVOKE SELECT ([secret]) ON [dbo].[t1] FROM [reader]", "GRANT SELECT ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a grant on a table over a column revoke row",
			diff: &difftypes.SchemaDiff{
				GrantsAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentPartialRevokes: []difftypes.GrantRef{onT1("SELECT", "secret")},
			},
			want: []string{"GRANT SELECT ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a grant on a table over a column deny of another permission",
			diff: &difftypes.SchemaDiff{
				GrantsAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1("UPDATE", "secret"))},
			},
			want: []string{"GRANT SELECT ON [dbo].[t1] TO [reader]"},
		},
		{
			name: "a grant to PUBLIC over a column deny the catalog spells public",
			diff: &difftypes.SchemaDiff{
				GrantsAdded:           []difftypes.GrantRef{onT1To("PUBLIC", "SELECT", "")},
				CurrentPartialRevokes: []difftypes.GrantRef{deniedRow(onT1To("public", "SELECT", "secret"))},
			},
			want: []string{"GRANT SELECT ON [dbo].[t1] TO [PUBLIC]", "DENY SELECT ([secret]) ON [dbo].[t1] TO [public]"},
		},
		{
			name: "a deny on a column beside a grant the grantee gave on another column",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "secret")},
				CurrentGrants: []difftypes.GrantRef{
					grantable(onT1("SELECT", "")),
					{Role: "auditor", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t1", Column: "name", GrantedBy: "reader"},
				},
			},
			want: []string{"DENY SELECT ([secret]) ON [dbo].[t1] TO [reader] CASCADE"},
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

// TestPlan_SQLServerDenies_FailurePath refuses a DENY whose CASCADE would take
// a grant the schema keeps -- on the whole table, or on the column a column
// DENY names -- and a DENY on a target without one.
func TestPlan_SQLServerDenies_FailurePath(t *testing.T) {
	onward := difftypes.GrantRef{Role: "auditor", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t1", Column: "name", GrantedBy: "reader"}
	onwardTable := difftypes.GrantRef{Role: "auditor", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t1", GrantedBy: "reader"}
	onwardSecret := difftypes.GrantRef{Role: "auditor", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "dbo.t1", Column: "secret", GrantedBy: "reader"}
	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		wantErr string
	}{
		{
			name:    "a cascade over a grant the grantee gave on",
			dialect: "sqlserver",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", "name")), onward},
			},
			wantErr: `.*cannot deny SELECT on dbo\.t1 to "reader": "reader" holds it WITH GRANT OPTION, so SQL Server denies it with CASCADE, ` +
				`which also takes the SELECT that "reader" granted to "auditor" \(name\), and the schema does not revoke those grants.*`,
		},
		{
			name:    "a column deny over a grant the grantee gave on the whole table",
			dialect: "sqlserver",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "secret")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", "")), onwardTable},
			},
			wantErr: `.*cannot deny SELECT on dbo\.t1 to "reader": .* which also takes the SELECT that "reader" granted to "auditor", and the schema does not revoke those grants.*`,
		},
		{
			name:    "a column deny over a grant the grantee gave on the same column",
			dialect: "sqlserver",
			diff: &difftypes.SchemaDiff{
				DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "secret")}, CurrentGrants: []difftypes.GrantRef{grantable(onT1("SELECT", "")), onwardSecret},
			},
			wantErr: `.*cannot deny SELECT on dbo\.t1 to "reader": .* which also takes the SELECT that "reader" granted to "auditor" \(secret\), and the schema does not revoke those grants.*`,
		},
		{
			name:    "a deny on PostgreSQL",
			dialect: "postgres",
			diff:    &difftypes.SchemaDiff{DeniesAdded: []difftypes.GrantRef{onT1("SELECT", "")}},
			wantErr: `.*the diff changes the DENY of SELECT on table "dbo\.t1" to "reader", and the postgres target has no DENY.*`,
		},
		{
			name:    "a deny taken away on MySQL",
			dialect: "mysql",
			diff:    &difftypes.SchemaDiff{DeniesRemoved: []difftypes.GrantRef{onT1("SELECT", "")}},
			wantErr: `.*the diff changes the DENY of SELECT on table "dbo\.t1" to "reader", and the mysql target has no DENY.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(
				context.Background(), must.Must(builtin.New()), test.diff, test.dialect,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
