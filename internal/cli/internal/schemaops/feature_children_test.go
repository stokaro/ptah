package schemaops_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/cli/internal/schemaops"
)

// policiesOn holds a policy on orders and one on audit in schema, in the
// representation build makes.
func policiesOn(c *qt.C, build func(objectidentity.ID) schemaext.Object, schema string) schemaext.Objects {
	c.Helper()
	return must.Must(schemaext.NewObjects(build(pgpolicy.PolicyRef(schema, "orders", "tenant")), build(pgpolicy.PolicyRef(schema, "audit", "tenant"))))
}

// TestFilterTables_DropsAnIgnoredTablesFeatureChildren pins that ignoring a
// table takes its row-level security policies with it on both sides, matched
// by the names the table itself is matched by, and keeps every other table's.
func TestFilterTables_DropsAnIgnoredTablesFeatureChildren(t *testing.T) {
	declared := func(ref objectidentity.ID) schemaext.Object {
		return must.Must(pgpolicy.DesiredPolicyObject(ref, pgpolicy.DesiredPolicy{}))
	}
	observed := func(ref objectidentity.ID) schemaext.Object {
		return must.Must(pgpolicy.ObservedPolicyObject(ref, pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll,
			Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Composition: pgpolicy.Permissive}))
	}
	tests := []struct {
		name, schema, ignored string
	}{
		{name: "a bare name", ignored: "audit"},
		{name: "a qualified name", schema: "app", ignored: "app.audit"},
		{name: "a bare name for a qualified table", schema: "app", ignored: "audit"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want := []objectidentity.ID{pgpolicy.PolicyRef(test.schema, "orders", "tenant")}

			generated := schemaops.FilterGeneratedTables(&schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Order", Schema: test.schema, Name: "orders"},
					{StructName: "Audit", Schema: test.schema, Name: "audit"}},
				FeatureObjects: policiesOn(c, declared, test.schema),
			}, []string{test.ignored})
			database := schemaops.FilterDatabaseTables(&catalog.Database{
				Tables:         []catalog.Table{{Schema: test.schema, Name: "orders"}, {Schema: test.schema, Name: "audit"}},
				FeatureObjects: policiesOn(c, observed, test.schema),
			}, []string{test.ignored})

			c.Assert(generated.Tables, qt.HasLen, 1)
			c.Assert(generated.FeatureObjects.Refs(), qt.DeepEquals, want)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(database.FeatureObjects.Refs(), qt.DeepEquals, want)
		})
	}
}
