package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertDBSchemaToGoSchema_YDBAccessModel describes a YDB read's groups,
// memberships and grant on the database the way a declaration writes them. The
// database's path stays on the read: it says where the read was made, and a
// render of the description takes it from there.
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

	described := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), read, platform.YDB, must.Must(builtin.New())))

	c.Assert(described.Roles, qt.DeepEquals, []schemamodel.Role{
		{Name: "app", Login: true, Inherit: true, MemberOf: []string{"readers", "USERS"}},
		{Name: "readers", Inherit: true, Group: true},
	})
	c.Assert(described.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "app", Privileges: []string{"YDB.DATABASE.CONNECT"}, OnDatabase: true},
	})
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

	described := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), read, platform.Postgres, must.Must(builtin.New())))

	c.Assert(described.Roles, qt.DeepEquals, []schemamodel.Role{{Name: "app", Login: true, Inherit: true}})
}
