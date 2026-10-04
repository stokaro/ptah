package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_YDBAccessModel describes a YDB read's groups,
// memberships and grant on the database the way a declaration writes them, and
// carries the database's path for a render that names the database by it.
func TestConvertDBSchemaToGoSchema_YDBAccessModel(t *testing.T) {
	c := qt.New(t)
	read := &catalog.Database{
		Roles: []catalog.Role{
			{Name: "app", Login: true, Inherit: true},
			{Name: "readers", Inherit: true, Group: true},
		},
		RoleMemberships: []catalog.RoleMembership{{Role: "readers", Member: "app"}, {Role: "USERS", Member: "app"}},
		Grants:          []catalog.Grant{{Role: "app", Privilege: "ydb.database.connect", ObjectType: "DATABASE"}},
		DatabasePath:    "/local",
	}

	described := dbschematogo.ConvertDBSchemaToGoSchema(read, platform.YDB)

	c.Assert(described.Roles, qt.DeepEquals, []schemamodel.Role{
		{Name: "app", Login: true, Inherit: true, MemberOf: []string{"readers", "USERS"}},
		{Name: "readers", Inherit: true, Group: true},
	})
	c.Assert(described.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "app", Privileges: []string{"YDB.DATABASE.CONNECT"}, OnDatabase: true},
	})
	c.Assert(described.DatabasePath, qt.Equals, "/local")
}

// TestConvertDBSchemaToGoSchema_RoleGraphStaysAnalysisOnly describes a
// PostgreSQL read without its role memberships: that read reports them for
// analysis, and a description declaring them would be refused by the target it
// was read from, which plans no membership.
func TestConvertDBSchemaToGoSchema_RoleGraphStaysAnalysisOnly(t *testing.T) {
	c := qt.New(t)
	read := &catalog.Database{
		Roles:           []catalog.Role{{Name: "app", Login: true, Inherit: true}},
		RoleMemberships: []catalog.RoleMembership{{Role: "readers", Member: "app"}},
	}

	described := dbschematogo.ConvertDBSchemaToGoSchema(read, platform.Postgres)

	c.Assert(described.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "app", Login: true, Inherit: true}})
}
