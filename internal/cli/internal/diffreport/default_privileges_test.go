package diffreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/diffreport"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestCategoriesTellsTwoDefaultPrivilegesApart holds the report against an
// element type with five string fields.
//
// The walk that names an element by its string fields stops after four of them,
// which is exactly the width of a grant reference. A default privilege carries
// five -- grantor, schema, object type, grantee, privilege -- so without a name
// of its own it is reported by its first four and the privilege is dropped.
// Measured by removing [difftypes.DefaultPrivilegeRef.String]: two entries of
// one object both print `app_owner app TABLES app_reader`, and an operator
// reading `ptah schema compare` is told two changes happened with no way to see
// which privilege either of them is.
//
// The rows below are the two shapes that go wrong: entries of one object that
// differ only in the truncated component, and entries that differ only in a
// component the truncation happens to keep, which is what stops the assertion
// from passing on a name that carries the privilege and nothing else.
func TestCategoriesTellsTwoDefaultPrivilegesApart(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []string
	}{
		{
			name: "two privileges of one object",
			diff: &difftypes.SchemaDiff{DefaultPrivilegesAdded: []difftypes.DefaultPrivilegeRef{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "INSERT",
				},
			}},
			want: []string{
				"SELECT on TABLES in app for app_owner to app_reader",
				"INSERT on TABLES in app for app_owner to app_reader",
			},
		},
		{
			name: "two grantors, everything else equal",
			diff: &difftypes.SchemaDiff{DefaultPrivilegesAdded: []difftypes.DefaultPrivilegeRef{
				{
					Grantor: "alpha_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "beta_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
			}},
			want: []string{
				"SELECT on TABLES in app for alpha_owner to app_reader",
				"SELECT on TABLES in app for beta_owner to app_reader",
			},
		},
		{
			name: "two grantees, everything else equal",
			diff: &difftypes.SchemaDiff{DefaultPrivilegesRemoved: []difftypes.DefaultPrivilegeRef{
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "app_reader", Privilege: "SELECT",
				},
				{
					Grantor: "app_owner", Schema: "app", ObjectType: "TABLES",
					Grantee: "PUBLIC", Privilege: "SELECT",
				},
			}},
			want: []string{
				"SELECT on TABLES in app for app_owner to app_reader",
				"SELECT on TABLES in app for app_owner to PUBLIC",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			categories := diffreport.Categories(test.diff)

			c.Assert(categories, qt.HasLen, 1)
			c.Assert(categories[0].Objects, qt.DeepEquals, test.want)
		})
	}
}
