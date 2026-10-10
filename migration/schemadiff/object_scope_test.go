package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// heldTenantPolicy is the policy tenant the database holds on orders.
var heldTenantPolicy = pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
	Using: new("tenant_id = 1"), Composition: pgpolicy.Permissive}

// ordersDeclaringPolicies declares table orders, unqualified, with the given feature
// objects and complete row-security coverage.
func ordersDeclaringPolicies(objects ...schemaext.Object) *schemamodel.Database {
	return &schemamodel.Database{
		Tables:          []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields:          []schemamodel.Field{{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true}},
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
	}
}

// ordersHoldingTenant is a database whose schema holds orders and its policy
// tenant, which the read recorded under policySchema; an empty one leaves the
// schema to the connection.
func ordersHoldingTenant(schema, policySchema string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Schema: schema, Name: "orders",
			Columns: []catalog.Column{{Name: "id", DataType: "integer", IsPrimaryKey: true}}}},
		FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(pgpolicy.ObservedPolicyObject(pgpolicy.PolicyRef(policySchema, "orders", "tenant"), heldTenantPolicy)))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}
}

// featureChanges collects every feature change a diff holds, at the top and
// on each table it modifies.
func featureChanges(diff *difftypes.SchemaDiff) []schemaext.ChangeRecord {
	changes := slices.Clone(diff.FeatureChanges)
	for _, table := range diff.TablesModified {
		changes = append(changes, table.FeatureChanges...)
	}
	return changes
}

// pgConnection is a PostgreSQL connection whose default schema is schema.
func pgConnection(schema string) catalog.ServerInfo {
	semantics := identifier.ForDialect("postgres")
	semantics.DefaultSchema = schema
	return catalog.ServerInfo{Dialect: "postgres", IdentifierSemantics: semantics}
}

// connectionCases are the shapes in which a declaration that leaves the
// schema to the connection meets the policy a read recorded: in the default
// schema, in a connection default other than public, and under a read that
// left the schema to the connection too.
var connectionCases = []struct {
	name         string
	schema       string
	policySchema string
}{
	{name: "the default schema", schema: "public", policySchema: "public"},
	{name: "a connection default", schema: "app", policySchema: "app"},
	{name: "a read that leaves the schema out", schema: "app", policySchema: ""},
}

// TestCompare_LeavesAFeatureObjectScopedElsewhere pins that a feature object
// declared for another target suppresses the object the database holds under
// its identity, so the exclusion is not read as a drop. The identities match
// once both are bound to the connection's default, which is how the
// comparison pairs them.
func TestCompare_LeavesAFeatureObjectScopedElsewhere(t *testing.T) {
	for _, test := range connectionCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "tenant"), pgpolicy.DesiredPolicy{Using: new("tenant_id = 2")}))
			declared.Targets = []string{"cockroachdb"}

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), ordersDeclaringPolicies(declared), ordersHoldingTenant(test.schema, test.policySchema),
				pgConnection(test.schema), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(featureChanges(diff), qt.HasLen, 0)
		})
	}
}

// TestCompare_StillDropsAnUndeclaredFeatureObject is the control: with no
// declaration of the policy at all, the same comparison plans its drop.
func TestCompare_StillDropsAnUndeclaredFeatureObject(t *testing.T) {
	for _, test := range connectionCases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), ordersDeclaringPolicies(), ordersHoldingTenant(test.schema, test.policySchema),
				pgConnection(test.schema), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			changes := featureChanges(diff)
			c.Assert(changes, qt.HasLen, 1)
			c.Assert(changes[0].Subject.Key(), qt.Equals, pgpolicy.PolicyRef(test.schema, "orders", "tenant").Key())
			c.Assert(changes[0].Value.(*pgpolicy.PolicyChange).After, qt.IsNil)
		})
	}
}
