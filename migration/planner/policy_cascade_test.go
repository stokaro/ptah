package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// tenantVisibility is the clause of a policy that reads the view
// active_tenants, as the catalog prints it.
const tenantVisibility = "(tenant_id IN ( SELECT active_tenants.id FROM active_tenants))"

// policyCascadeSchemas declares a view active_tenants with body, and a policy
// on orders that reads it; the database holds the view as
// `SELECT id, name FROM tenants WHERE active` and the same policy.
func policyCascadeSchemas(body string) (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "tenants", StructName: "Tenant"}, {Name: "orders", StructName: "Order"}},
		Fields: []schemamodel.Field{
			{StructName: "Tenant", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Tenant", Name: "name", Type: "TEXT", Nullable: true},
			{StructName: "Tenant", Name: "active", Type: "BOOLEAN", Nullable: true},
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "tenant_id", Type: "INTEGER", Nullable: true},
		},
		Views: []schemamodel.View{{Name: "active_tenants", Body: body}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
			pgpolicy.PolicyRef("", "orders", "tenant_visible"),
			pgpolicy.DesiredPolicy{Using: new(tenantVisibility)})))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
	}
	desired.Tables[1].Facets = must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true}))
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "tenants", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "name", DataType: "text", IsNullable: "YES", OrdinalPosition: 2},
				{Name: "active", DataType: "boolean", IsNullable: "YES", OrdinalPosition: 3},
			}},
			{Name: "orders", Type: "BASE TABLE", Facets: must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true})),
				Columns: []catalog.Column{
					{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
					{Name: "tenant_id", DataType: "integer", IsNullable: "YES", OrdinalPosition: 2},
				}},
		},
		Views: []catalog.View{{Name: "active_tenants", Body: "SELECT id, name FROM tenants WHERE active"}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.ObservedPolicyObject(
			pgpolicy.PolicyRef("", "orders", "tenant_visible"),
			pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
				Using: new(tenantVisibility), Composition: pgpolicy.Permissive})))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}
	return desired, current
}

// planPolicyCascade plans the change from the database to the declaration.
func planPolicyCascade(c *qt.C, body string) (string, error) {
	c.Helper()
	desired, current := policyCascadeSchemas(body)
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	return planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), diff, platform.Postgres)
}

// TestGenerateSchemaDiffSQL_RefusesACascadeThatDropsAPolicy_FailurePath pins
// stokaro/ptah#4305. Swapping two projected columns is a view change CREATE
// OR REPLACE VIEW cannot make, so the plan drops the view with CASCADE, and
// the cascade drops the policy that reads it. The owner plans no change for
// the policy, so nothing would put it back; the plan is refused, naming it.
func TestGenerateSchemaDiffSQL_RefusesACascadeThatDropsAPolicy_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := planPolicyCascade(c, "SELECT name, id FROM tenants WHERE active")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*drops active_tenants with CASCADE, which also drops policy "tenant_visible" on table public\.orders.*`)
	c.Assert(sql, qt.Equals, "")
}

// TestGenerateSchemaDiffSQL_ReplacesAViewAPolicyReads_HappyPath is the control:
// appending a column is a change CREATE OR REPLACE VIEW makes in place, which
// keeps the policy, so the plan goes ahead.
func TestGenerateSchemaDiffSQL_ReplacesAViewAPolicyReads_HappyPath(t *testing.T) {
	c := qt.New(t)

	sql, err := planPolicyCascade(c, "SELECT id, name, active FROM tenants WHERE active")

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `CREATE OR REPLACE VIEW "active_tenants"`)
	c.Assert(sql, qt.Not(qt.Contains), "CASCADE")
}
