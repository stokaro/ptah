package schemascope_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/schemascope"
)

// TestFilter_KeepsAFeatureChildWithItsTable pins that a table's row-level
// security policy stays when the selection keeps its table and leaves with
// it, on both sides, including a policy on an unqualified table that belongs
// to the default schema.
func TestFilter_KeepsAFeatureChildWithItsTable(t *testing.T) {
	c := qt.New(t)
	declared := func(schema, table string) schemaext.Object {
		return must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef(schema, table, "tenant"), pgpolicy.DesiredPolicy{}))
	}
	observed := func(schema, table string) schemaext.Object {
		return must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef(schema, table, "tenant"), pgpolicy.ObservedPolicy{
			Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}}, Composition: pgpolicy.Permissive}))
	}

	generated := schemascope.FilterGeneratedWithDefaultSchema(&schemamodel.Database{
		Tables:         []schemamodel.Table{{StructName: "Order", Name: "orders"}, {StructName: "Audit", Schema: "audit", Name: "events"}},
		FeatureObjects: must.Must(schemaext.NewObjects(declared("", "orders"), declared("audit", "events"))),
	}, []string{"public"}, "public")
	database := schemascope.FilterDatabaseWithDefaultSchema(&catalog.Database{
		Tables:         []catalog.Table{{Schema: "public", Name: "orders"}, {Schema: "audit", Name: "events"}},
		FeatureObjects: must.Must(schemaext.NewObjects(observed("public", "orders"), observed("audit", "events"))),
	}, []string{"public"}, "public")

	c.Assert(generated.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgpolicy.PolicyRef("", "orders", "tenant")})
	c.Assert(database.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgpolicy.PolicyRef("public", "orders", "tenant")})
}
