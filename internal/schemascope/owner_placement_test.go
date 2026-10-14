package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/schemascope"
)

// An ACL can be the only declaration that names a schema. Global ACLs add no
// connected schema, and decoded identifier bytes remain significant.
func TestDeclaredSchemaNamesCountsDefaultPrivilegePlacement(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   []string
	}{
		{name: "explicit", schema: "app", want: []string{"app"}},
		{name: "decoded whitespace", schema: " app ", want: []string{" app "}},
		{name: "global"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: test.schema, ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(schemaext.Object{Ref: ref, Value: &pgdefaultprivilege.Desired{}}))}
			names, err := schemascope.DeclaredSchemaNames(desired)
			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.DeepEquals, test.want)
		})
	}
}

// An extension's installation schema holds the extension's members, not the
// extension. plpgsql is installed in pg_catalog on every PostgreSQL server, so
// a read widened to it reported the system catalog's indexes as objects to drop.
func TestDeclaredSchemaNamesLeavesExtensionInstallationSchemas(t *testing.T) {
	for _, schema := range []string{"pg_catalog", "extensions"} {
		t.Run(schema, func(t *testing.T) {
			c := qt.New(t)
			object := must.Must(pgextension.DesiredObject(pgextension.Ref("plpgsql"), pgextension.DesiredExtension{Schema: schema}))
			desired := &schemamodel.Database{
				Tables:         []schemamodel.Table{{Name: "x", Schema: "public"}},
				FeatureObjects: must.Must(schemaext.NewObjects(object)),
			}
			names, err := schemascope.DeclaredSchemaNames(desired)
			c.Assert(err, qt.IsNil)
			c.Assert(names, qt.DeepEquals, []string{"public"})
		})
	}
}
