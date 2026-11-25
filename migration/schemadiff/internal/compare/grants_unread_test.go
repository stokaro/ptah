package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// An unread grant catalog cannot establish that an explicit revoke is already
// satisfied. Keep the requested statement so the server answers for the role.
func TestGrantsWithOptions_KeepsAnExplicitRevokeWhenGrantsAreUnread(t *testing.T) {
	selectOnTable := difftypes.GrantRef{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}
	tests := []struct {
		name   string
		unread coverage.Set
		held   []catalog.Grant
		want   []difftypes.GrantRef
	}{
		{name: "all roles unread", unread: coverage.Set{}.With(coverage.Refused(coverage.Role)), want: []difftypes.GrantRef{selectOnTable}},
		{name: "this role unread", unread: coverage.Set{}.WithObject(coverage.Role, "reader"), want: []difftypes.GrantRef{selectOnTable}},
		{name: "another role unread", unread: coverage.Set{}.WithObject(coverage.Role, "other")},
		{name: "complete read with no grants"},
		{
			name:   "a grant already planned for removal is not repeated",
			unread: coverage.Set{}.With(coverage.Refused(coverage.Role)),
			held:   []catalog.Grant{{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"}},
			want:   []difftypes.GrantRef{selectOnTable},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables:        []schemamodel.Table{{Name: "t"}},
				RevokedGrants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "t"}},
			}
			current := &catalog.Database{Tables: []catalog.Table{{Name: "t"}}, Grants: test.held, NotDescribed: test.unread}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithOptions(desired, current, diff, identifier.ForDialect(platform.MySQL), compare.GrantOptions{
				TableRevokesTakeOnlyTableGrants: true,
			})

			c.Assert(diff.GrantsRemoved, qt.DeepEquals, test.want)
			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
		})
	}
}
