package catalog_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// TestGrant_OwnerPrivilegeRows names the rows a read reports for what an object's
// owner holds by default, apart from a grant and from a routine's implicit rows.
func TestGrant_OwnerPrivilegeRows(t *testing.T) {
	tests := []struct {
		name         string
		grant        catalog.Grant
		ownerDefault bool
		ownerRevoked bool
	}{
		{
			name:  "a grant",
			grant: catalog.Grant{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t"},
		},
		{
			name:         "a privilege a table's owner holds",
			grant:        catalog.Grant{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Implicit: true},
			ownerDefault: true,
		},
		{
			name:         "a privilege a sequence's owner holds",
			grant:        catalog.Grant{Role: "app", Privilege: "USAGE", ObjectType: "SEQUENCE", ObjectName: "s", Implicit: true},
			ownerDefault: true,
		},
		{
			name:         "a privilege a schema's owner holds",
			grant:        catalog.Grant{Role: "app", Privilege: "USAGE", ObjectType: "SCHEMA", ObjectName: "s", Implicit: true},
			ownerDefault: true,
		},
		{
			name:         "a privilege a table's owner revoked from itself",
			grant:        catalog.Grant{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", Implicit: true, IsPartialRevoke: true},
			ownerRevoked: true,
		},
		{
			name:  "a partial revoke of a grant",
			grant: catalog.Grant{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t", IsPartialRevoke: true},
		},
		{
			name:  "PUBLIC's EXECUTE on a function",
			grant: catalog.Grant{Role: "PUBLIC", Privilege: "EXECUTE", ObjectType: "FUNCTION", ObjectName: "f", Implicit: true},
		},
		{
			name:  "a procedure owner's observation row",
			grant: catalog.Grant{Role: "app", ObjectType: "PROCEDURE", ObjectName: "p", Implicit: true},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.grant.OwnerDefault(), qt.Equals, test.ownerDefault)
			c.Assert(test.grant.OwnerRevoked(), qt.Equals, test.ownerRevoked)
		})
	}
}
