package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestDefaultPrivilegesWithSemantics_Revoked pins what a revoked default
// privilege plans. It asserts the privilege is absent, so it is removed where
// the database holds it even when the grantor is not a role the schema
// declares and the identity grants nothing else -- the case a declaration that
// only leaves the privilege out cannot reach.
func TestDefaultPrivilegesWithSemantics_Revoked(t *testing.T) {
	row := func(privilege string) catalog.DefaultPrivilege {
		return catalog.DefaultPrivilege{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Privilege: privilege,
		}
	}
	tests := []struct {
		name        string
		revoked     []string
		rows        []catalog.DefaultPrivilege
		wantRemoved []string
	}{
		{
			name:        "a held privilege is removed and the rest are left alone",
			revoked:     []string{"INSERT"},
			rows:        []catalog.DefaultPrivilege{row("SELECT"), row("INSERT")},
			wantRemoved: []string{"INSERT on TABLES in app for app_owner to app_reader"},
		},
		{
			name:    "a privilege the database does not hold plans nothing",
			revoked: []string{"DELETE"},
			rows:    []catalog.DefaultPrivilege{row("SELECT")},
		},
		{
			name:    "ALL removes every privilege of the identity",
			revoked: []string{"ALL"},
			rows:    []catalog.DefaultPrivilege{row("SELECT"), row("INSERT")},
			wantRemoved: []string{
				"INSERT on TABLES in app for app_owner to app_reader",
				"SELECT on TABLES in app for app_owner to app_reader",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{DefaultPrivileges: []schemamodel.DefaultPrivilege{{
				Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Revoked: test.revoked,
			}}}
			diff := &difftypes.SchemaDiff{}

			compare.DefaultPrivilegesWithSemantics(desired, &catalog.Database{DefaultPrivileges: test.rows}, diff,
				identifier.ForDialect(platform.Postgres))

			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.HasLen, 0)
			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, append(make([]string, 0), test.wantRemoved...))
		})
	}
}
