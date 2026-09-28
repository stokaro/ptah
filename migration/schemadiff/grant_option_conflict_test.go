package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

// mysqlFamilyInfo is the connection metadata a MySQL-family comparison
// carries.
func mysqlFamilyInfo(dialect string) catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: dialect, Schema: "app"}
}

// TestCompareWithDatabaseInfoRefusesAGrantOptionConflict wires
// [schemamodel.ValidateGrantOptionConsistency] into the comparison entry
// point every caller reaches, live or offline, so the refusal fires before a
// live grant is even read (stokaro/ptah-operator#481).
func TestCompareWithDatabaseInfoRefusesAGrantOptionConflict(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Grants: []schemamodel.Grant{
				{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
				{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "orders", WithOption: true},
			}}

			diff, err := schemadiff.CompareWithDatabaseInfo(desired, &catalog.Database{}, mysqlFamilyInfo(dialect), nil)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*is granted INSERT on TABLE orders WITH GRANT OPTION and SELECT on the same object without it.*`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfoStillComparesAgreeingGrants is the control: two
// declarations on the same object that agree on the grant option, or the same
// pair on PostgreSQL where the option is per privilege, plan normally.
func TestCompareWithDatabaseInfoStillComparesAgreeingGrants(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql, both without the option", dialect: "mysql"},
		{name: "postgres, one with the option and one without", dialect: "postgres"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			withOption := test.dialect == "postgres"
			desired := &schemamodel.Database{
				Roles: []schemamodel.Role{{Name: "reader", Inherit: true}},
				Grants: []schemamodel.Grant{
					{Role: "reader", Privileges: []string{"SELECT"}, OnTable: "orders"},
					{Role: "reader", Privileges: []string{"INSERT"}, OnTable: "orders", WithOption: withOption},
				},
			}

			diff, err := schemadiff.CompareWithDatabaseInfo(
				desired, &catalog.Database{}, catalog.ServerInfo{Dialect: test.dialect, Schema: "app"}, nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.GrantsAdded, qt.HasLen, 2)
		})
	}
}
