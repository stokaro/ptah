package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_ALoginsGrantIsNotDescribed is a privilege a
// MySQL login holds. No statement of the description creates the login, so a
// GRANT to it would be refused where the login is absent and reach another
// account where one has its name. A role's grant beside it stays.
func TestConvertDBSchemaToGoSchema_ALoginsGrantIsNotDescribed(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			read := &catalog.Database{
				Roles: []catalog.Role{{Name: "reader"}},
				Grants: []catalog.Grant{
					{Role: "app", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1", Login: true},
					{Role: "app", Privilege: "INSERT", ObjectType: "SCHEMA", ObjectName: "rva", Login: true},
					{Role: "reader", Privilege: "SELECT", ObjectType: "TABLE", ObjectName: "t1"},
				},
			}

			db, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), read, dialect, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(db.Grants, qt.HasLen, 1)
			c.Assert(db.Grants[0].Role, qt.Equals, "reader")
			c.Assert(db.RevokedGrants, qt.HasLen, 0)
		})
	}
}
