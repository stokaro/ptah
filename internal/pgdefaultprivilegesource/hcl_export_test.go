package pgdefaultprivilegesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/pgdefaultprivilegesource"
)

func TestHCLExport_RoundTripsRoleNamesAndPrivileges(t *testing.T) {
	tests := []struct {
		name, grantor, grantee string
		roles                  []schemamodel.Role
	}{
		{name: "declared roles", grantor: "owner", grantee: "reader", roles: []schemamodel.Role{{Name: "owner"}, {Name: "reader"}}},
		{name: "undeclared roles", grantor: "owner", grantee: "reader"},
		{name: "one declared role", grantor: "owner", grantee: "reader", roles: []schemamodel.Role{{Name: "owner"}}},
		{name: "quoted names", grantor: "owner.with.dot", grantee: "role.reader"},
		{name: "escaped names", grantor: "owner\"quote", grantee: "reader${name}"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			object := must.Must(pgdefaultprivilegesource.Declare(pgdefaultprivilege.Identity{Grantor: test.grantor, Schema: "app", ObjectType: "TABLES", Grantee: test.grantee}, []string{"SELECT", "INSERT"}, []string{"INSERT"}, []string{"DELETE"}, "new tables", nil))
			db := &schemamodel.Database{Roles: test.roles, FeatureObjects: must.Must(schemaext.NewObjects(object))}
			rendered, err := atlashclrender.RenderInspected(db, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL()})
			c.Assert(err, qt.IsNil)
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			c.Assert(string(rendered.Data), qt.Contains, "default_privilege {")
			parsed := must.Must(parseHCL(t.Context(), string(rendered.Data)))
			c.Assert(parsed.FeatureObjects.Equal(db.FeatureObjects), qt.IsTrue)
			second := must.Must(atlashclrender.RenderInspected(parsed, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL()}))
			c.Assert(second.Data, qt.DeepEquals, rendered.Data)
		})
	}
}

func TestHCLExport_GlobalRevokesDoNotInheritASchema(t *testing.T) {
	tests := []struct{ objectType, privilege string }{
		{objectType: "FUNCTIONS", privilege: "EXECUTE"},
		{objectType: "TYPES", privilege: "USAGE"},
	}
	for _, test := range tests {
		t.Run(test.objectType, func(t *testing.T) {
			c := qt.New(t)
			object := must.Must(pgdefaultprivilegesource.Declare(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: test.objectType, Grantee: "PUBLIC"}, nil, nil, []string{test.privilege}, "", nil))
			db := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(object))}
			rendered := must.Must(atlashclrender.RenderInspected(db, "public", atlashclrender.Options{HCL: builtintest.Runtime().HCL()}))
			c.Assert(rendered.Diagnostics, qt.HasLen, 0)
			parsed := must.Must(parseHCL(t.Context(), string(rendered.Data)))
			c.Assert(parsed.FeatureObjects.Equal(db.FeatureObjects), qt.IsTrue)
			objects := must.Must(parsed.FeatureObjects.All())
			c.Assert(must.Must(pgdefaultprivilege.IdentityOf(objects[0].Ref)).Schema, qt.Equals, "")
		})
	}
}

func TestHCLExport_IsIndependentOfDeclarationOrder(t *testing.T) {
	c := qt.New(t)
	first := hclBlock(hclIdentity + "privileges = [INSERT]\ngrantable = [INSERT]")
	second := hclBlock(hclIdentity + "privileges = [SELECT]")
	left := must.Must(parseHCL(t.Context(), first+second))
	right := must.Must(parseHCL(t.Context(), second+first))
	options := atlashclrender.Options{HCL: builtintest.Runtime().HCL()}
	c.Assert(must.Must(atlashclrender.Render(left, options)).Data, qt.DeepEquals, must.Must(atlashclrender.Render(right, options)).Data)
}
