package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

func TestGrant_CanonicalizeKeepsPrincipalNames(t *testing.T) {
	for _, role := range []string{" reader", "reader ", "\treader", " public", " "} {
		t.Run(role, func(t *testing.T) {
			c := qt.New(t)
			grant := schemamodel.Grant{Role: role, OnTable: "t", Privileges: []string{" select "}}

			grant.Canonicalize()

			c.Assert(grant.Role, qt.Equals, role)
			c.Assert(grant.Privileges, qt.DeepEquals, []string{"SELECT"})
		})
	}
}

func TestGrantValidation_KeepsPrincipalNames(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Grants: []schemamodel.Grant{
			{Role: "reader", OnTable: "t", Columns: []string{"ID"}, Privileges: []string{"SELECT"}, WithOption: true},
			{Role: " reader", OnTable: "t", Columns: []string{"id"}, Privileges: []string{"SELECT"}},
		},
		RevokedGrants: []schemamodel.Grant{{Role: "reader ", OnTable: "t", Columns: []string{"Id"}, Privileges: []string{"SELECT"}}},
	}

	c.Assert(schemamodel.ValidateGrantOptionConsistency(database, "mariadb"), qt.IsNil)
	c.Assert(schemamodel.ValidateGrantColumnSpellings(database, "mariadb"), qt.IsNil)
}
