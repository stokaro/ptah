package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestGrants_ObjectGrants_HappyPath compares grants on YDB scheme objects an
// owner manages: a declared grant on a topic matches the entry a read reports
// on it, so an applied declaration plans nothing again; an entry the
// declaration leaves out of a managed role is revoked; and a grant on a secret
// is a different grant from one on a topic of the same name.
func TestGrants_ObjectGrants_HappyPath(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Roles: []schemamodel.Role{{Name: "reader"}},
		Grants: []schemamodel.Grant{
			{Role: "reader", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}, OnObject: "app.events", ObjectKind: "TOPIC"},
			{Role: "reader", Privileges: []string{"YDB.GRANULAR.SELECT_ROW"}, OnObject: "app.events", ObjectKind: "SECRET"},
		},
	}
	database := &catalog.Database{
		Roles: []catalog.Role{{Name: "reader"}},
		Grants: []catalog.Grant{
			{Role: "reader", Privilege: "ydb.granular.select_row", ObjectType: "TOPIC", Schema: "app", ObjectName: "events"},
			{Role: "reader", Privilege: "ydb.generic.list", ObjectType: "TOPIC", Schema: "app", ObjectName: "events"},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.GrantsWithSemantics(desired, database, diff, identifier.ForDialect("ydb"))

	c.Assert(diff.GrantsAdded, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "reader", Privilege: "YDB.GRANULAR.SELECT_ROW", ObjectType: "SECRET", ObjectName: "app.events"},
	})
	c.Assert(diff.GrantsRemoved, qt.DeepEquals, []difftypes.GrantRef{
		{Role: "reader", Privilege: "YDB.GENERIC.LIST", ObjectType: "TOPIC", ObjectName: "app.events"},
	})
}

// TestGrants_KeepsASecretsImplicitEntry pins that the entry YDB gives a secret
// it creates, which the reader marks implicit, is kept for a managed role that
// a declaration does not grant it to, and still satisfies a declaration that
// does.
func TestGrants_KeepsASecretsImplicitEntry(t *testing.T) {
	tests := []struct {
		name   string
		grants []schemamodel.Grant
	}{
		{name: "left out"},
		{name: "declared", grants: []schemamodel.Grant{{Role: "reader", Privileges: []string{"YDB.GRANULAR.DESCRIBE_SCHEMA"},
			OnObject: "ext.pw", ObjectKind: "SECRET"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}, Grants: test.grants}
			database := &catalog.Database{
				Roles: []catalog.Role{{Name: "reader"}},
				Grants: []catalog.Grant{{Role: "reader", Privilege: "ydb.granular.describe_schema", ObjectType: "SECRET",
					Schema: "ext", ObjectName: "pw", Implicit: true}},
			}
			diff := &difftypes.SchemaDiff{}

			compare.GrantsWithSemantics(desired, database, diff, identifier.ForDialect("ydb"))

			c.Assert(diff.GrantsAdded, qt.HasLen, 0)
			c.Assert(diff.GrantsRemoved, qt.HasLen, 0)
		})
	}
}
