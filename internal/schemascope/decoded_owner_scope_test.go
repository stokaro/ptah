package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/schemascope"
)

// A read scope must retain the schema named by an owner. These names are
// decoded identifiers, so whitespace and commas belong to the schema name.
func TestDefaultPrivilegePlacementSurvivesReadScope(t *testing.T) {
	for _, schema := range []string{" app ", "a,b", " "} {
		t.Run(schema, func(t *testing.T) {
			c := qt.New(t)
			ref := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", Schema: schema, ObjectType: "TABLES", Grantee: "reader"}, identifier.ForDialect("postgres")))
			desired := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.DesiredObject(ref, pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}))))}
			current := &catalog.Database{FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgdefaultprivilege.ObservedObject(ref, pgdefaultprivilege.Observed{Privileges: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}))))}
			info := catalog.ServerInfo{Dialect: "postgres", URL: "postgres://localhost/db?search_path=public", Schema: "public"}
			declared, err := schemascope.DeclaredSchemaNames(desired)
			c.Assert(err, qt.IsNil)
			scope := schemascope.Widen(info, []string{"public"}, declared)
			c.Assert(scope, qt.Contains, schema)
			c.Assert(schemascope.BeyondURL(info, []string{"public"}, scope), qt.DeepEquals, []string{schema})
			generated, err := schemascope.FilterGenerated(t.Context(), defaultPrivilegeSelections(), desired, scope)
			c.Assert(err, qt.IsNil)
			observed, err := schemascope.FilterDatabase(t.Context(), defaultPrivilegeSelections(), current, scope)
			c.Assert(err, qt.IsNil)
			c.Assert(generated.FeatureObjects.Refs(), qt.DeepEquals, desired.FeatureObjects.Refs())
			c.Assert(observed.FeatureObjects.Refs(), qt.DeepEquals, current.FeatureObjects.Refs())
		})
	}
}
