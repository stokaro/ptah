package atlasschema

// White-box testing required: the rehearsal applies omitRealmRootGrants to the
// rebuild it derives from a live target, and its public entry point requires a
// live database connection; the YDB provider's live tests drive it.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// TestOmitRealmRootGrants pins which permissions a baseline leaves out: those
// on the database root, on both sides, and only when the dev connection names
// a dev realm under the database root, as the session's database context
// says of an engine an executable provider serves: the database it works in
// is not the root its authored references are read against. A permission on
// a directory or a table is kept.
func TestOmitRealmRootGrants(t *testing.T) {
	tests := []struct {
		name        string
		info        catalog.ServerInfo
		wantTarget  []schemamodel.Grant
		wantRevoked []schemamodel.Grant
		wantDev     []catalog.Grant
	}{
		{
			name: "a dev realm of a provider's engine",
			info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local?dev_realm=abc",
				DatabaseContext: schemaext.DatabaseContext{Database: "/local/.ptah_realms/abc", SourceDatabase: "/local"}},
			wantTarget:  []schemamodel.Grant{{Role: "reader", Privileges: []string{"ydb.generic.read"}, OnSchema: "shop"}},
			wantRevoked: make([]schemamodel.Grant, 0),
			wantDev:     []catalog.Grant{{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TABLE", ObjectName: "items"}},
		},
		{
			name: "a provider's database without a realm",
			info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local",
				DatabaseContext: schemaext.DatabaseContext{Database: "/local", SourceDatabase: "/local"}},
			wantTarget: []schemamodel.Grant{
				{Role: "ACCESS-ADMINS", Privileges: []string{"ydb.access.grant"}, OnDatabase: true},
				{Role: "reader", Privileges: []string{"ydb.generic.read"}, OnSchema: "shop"},
			},
			wantRevoked: []schemamodel.Grant{{Role: "writer", Privileges: []string{"ydb.generic.write"}, OnDatabase: true}},
			wantDev: []catalog.Grant{
				{Role: "ACCESS-ADMINS", Privilege: "ydb.access.grant", ObjectType: "DATABASE"},
				{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TABLE", ObjectName: "items"},
			},
		},
		{
			name: "a provider's session that states no database context",
			info: catalog.ServerInfo{Dialect: "ydb", URL: "ydb://localhost:2136/local?dev_realm=abc"},
			wantTarget: []schemamodel.Grant{
				{Role: "ACCESS-ADMINS", Privileges: []string{"ydb.access.grant"}, OnDatabase: true},
				{Role: "reader", Privileges: []string{"ydb.generic.read"}, OnSchema: "shop"},
			},
			wantRevoked: []schemamodel.Grant{{Role: "writer", Privileges: []string{"ydb.generic.write"}, OnDatabase: true}},
			wantDev: []catalog.Grant{
				{Role: "ACCESS-ADMINS", Privilege: "ydb.access.grant", ObjectType: "DATABASE"},
				{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TABLE", ObjectName: "items"},
			},
		},
		{
			name: "an engine whose documents name no database root",
			info: catalog.ServerInfo{Dialect: "postgres", URL: "postgres://localhost/db?dev_realm=abc"},
			wantTarget: []schemamodel.Grant{
				{Role: "ACCESS-ADMINS", Privileges: []string{"ydb.access.grant"}, OnDatabase: true},
				{Role: "reader", Privileges: []string{"ydb.generic.read"}, OnSchema: "shop"},
			},
			wantRevoked: []schemamodel.Grant{{Role: "writer", Privileges: []string{"ydb.generic.write"}, OnDatabase: true}},
			wantDev: []catalog.Grant{
				{Role: "ACCESS-ADMINS", Privilege: "ydb.access.grant", ObjectType: "DATABASE"},
				{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TABLE", ObjectName: "items"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			grants := []schemamodel.Grant{
				{Role: "ACCESS-ADMINS", Privileges: []string{"ydb.access.grant"}, OnDatabase: true},
				{Role: "reader", Privileges: []string{"ydb.generic.read"}, OnSchema: "shop"},
			}
			target := &schemamodel.Database{
				Grants:        grants,
				RevokedGrants: []schemamodel.Grant{{Role: "writer", Privileges: []string{"ydb.generic.write"}, OnDatabase: true}},
			}
			dev := &catalog.Database{Grants: []catalog.Grant{
				{Role: "ACCESS-ADMINS", Privilege: "ydb.access.grant", ObjectType: "DATABASE"},
				{Role: "reader", Privilege: "ydb.generic.read", ObjectType: "TABLE", ObjectName: "items"},
			}}

			omitRealmRootGrants(test.info, target, dev)

			c.Assert(target.Grants, qt.DeepEquals, test.wantTarget)
			c.Assert(target.RevokedGrants, qt.DeepEquals, test.wantRevoked)
			c.Assert(dev.Grants, qt.DeepEquals, test.wantDev)
			c.Assert(grants[0].OnDatabase, qt.IsTrue, qt.Commentf("the caller's slice is not written through"))
		})
	}
}
