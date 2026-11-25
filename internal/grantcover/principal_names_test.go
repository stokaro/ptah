package grantcover_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/grantcover"
)

func TestFind_KeepsPrincipalNames(t *testing.T) {
	for _, test := range []struct {
		name, revoked, held, declared string
		want                          []grantcover.Cover
	}{
		{name: "a padded holder", revoked: " r", held: " r", want: []grantcover.Cover{{Grantee: " r", Grant: "SELECT ON d2.*"}}},
		{name: "another principal", revoked: " r", held: "r"},
		{name: "a padded declaration", revoked: " r", declared: " r", want: []grantcover.Cover{{Grantee: " r", Grant: "SELECT ON d2.*", Declared: true}}},
		{name: "another declaration", revoked: "r", declared: " r"},
		{name: "a held declaration", revoked: " r", held: " r", declared: " r", want: []grantcover.Cover{{Grantee: " r", Grant: "SELECT ON d2.*", AlsoDeclared: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Grants: []schemamodel.Grant{{Role: test.declared, OnSchema: "d2", Privileges: []string{"SELECT"}}}}
			current := selecting(databaseGrant(test.held, "SELECT"))

			covers, known := grantcover.Find(desired, current, test.revoked, []string{"SELECT"}, "t", listsOf("mariadb"))

			c.Assert(known, qt.IsTrue)
			c.Assert(covers, qt.DeepEquals, test.want)
		})
	}
}

func TestValidate_KeepsPrincipalNameInRefusal(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{RevokedGrants: []schemamodel.Grant{{Role: " r", OnTable: "t", Privileges: []string{"SELECT"}}}}
	current := selecting(databaseGrant(" r", "SELECT"))

	err := grantcover.Validate("mariadb", desired, current)

	c.Assert(err, qt.ErrorMatches, `role " r" holds SELECT ON d2\.\*, .*revoke SELECT ON d2\.\* from " r" instead.*`)
}
