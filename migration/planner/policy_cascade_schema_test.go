package planner_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// namesakePolicy pairs a view a column type change rebuilds, declared as view
// and stored in schema, with a policy whose clause reads a view of the same
// bare name in another schema.
type namesakePolicy struct {
	name   string
	view   string
	schema string
	using  string
}

var (
	bareViewPolicy = namesakePolicy{
		name: "a view declared without a schema and a policy on reporting's",
		view: "active_tenants", using: "(tenant_id IN ( SELECT active_tenants.id FROM reporting.active_tenants))",
	}
	publicViewPolicy = namesakePolicy{
		name: "a view declared in public and a policy on reporting's",
		view: "public.active_tenants", schema: "public",
		using: "(tenant_id IN ( SELECT active_tenants.id FROM reporting.active_tenants))",
	}
	reportingViewPolicy = namesakePolicy{
		name: "a view in reporting and a policy on the default schema's",
		view: "reporting.active_tenants", schema: "reporting",
		using: "(tenant_id IN ( SELECT active_tenants.id FROM active_tenants))",
	}
)

// sessionSemantics is what a connection whose search path is path reports:
// the first schema is the default one.
func sessionSemantics(path ...string) *identifier.Semantics {
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = path[0]
	semantics.SearchPath = path
	return &semantics
}

// namesakeDiff compares a declaration that retypes tenants.id under the view
// with the database, where the policy on orders has the clause using.
// semantics is what the comparison's connection reported, nil for a
// comparison made without one.
func namesakeDiff(c *qt.C, test namesakePolicy, semantics *identifier.Semantics) *difftypes.SchemaDiff {
	c.Helper()
	view, using := test.view, test.using
	body := "SELECT id FROM public.tenants WHERE active"
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "tenants", StructName: "Tenant"}, {Name: "orders", StructName: "Order"}},
		Fields: []schemamodel.Field{
			{StructName: "Tenant", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Tenant", Name: "active", Type: "BOOLEAN", Nullable: true},
			{StructName: "Order", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Order", Name: "tenant_id", Type: "INTEGER", Nullable: true},
		},
		Views: []schemamodel.View{{Name: view, Body: body}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.DesiredPolicyObject(
			pgpolicy.PolicyRef("", "orders", "audit_visible"), pgpolicy.DesiredPolicy{Using: new(using)})))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
	}
	desired.Tables[1].Facets = must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true}))
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "tenants", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
				{Name: "active", DataType: "boolean", IsNullable: "YES", OrdinalPosition: 2},
			}},
			{Name: "orders", Type: "BASE TABLE", Facets: must.Must(schemaext.NewFacets(&pgpolicy.ObservedTableState{Enabled: true})),
				Columns: []catalog.Column{
					{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
					{Name: "tenant_id", DataType: "integer", IsNullable: "YES", OrdinalPosition: 2},
				}},
		},
		Views: []catalog.View{{Schema: test.schema, Name: "active_tenants", Body: body}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgpolicy.ObservedPolicyObject(
			pgpolicy.PolicyRef("", "orders", "audit_visible"),
			pgpolicy.ObservedPolicy{Command: pgpolicy.CommandAll, Roles: []pgpolicy.RoleSelector{{Keyword: pgpolicy.Public}},
				Using: new(using), Composition: pgpolicy.Permissive})))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Observed)),
	}
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	diff.IdentifierSemantics = semantics
	return diff
}

// TestGenerateSchemaDiffSQL_KeepsAPolicyOnANamesake_HappyPath pins the plan a
// policy on a namesake does not stop: the column type change rebuilds the
// view, and the policy reads a view of that name in another schema, which the
// plan leaves alone. The default schema the comparison's connection reported
// is what tells the two apart.
func TestGenerateSchemaDiffSQL_KeepsAPolicyOnANamesake_HappyPath(t *testing.T) {
	for _, test := range []namesakePolicy{bareViewPolicy, publicViewPolicy, reportingViewPolicy} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
				namesakeDiff(c, test, sessionSemantics("public")), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "DROP VIEW")
			c.Assert(sql, qt.Contains, `ALTER COLUMN "id" TYPE BIGINT`)
		})
	}
}

// TestGenerateSchemaDiffSQL_RefusesANamesakeInAnUnknownSchema_FailurePath is
// the control: without the default schema, an unqualified name may be in
// either schema, and the policy may read the view the plan drops, so the plan
// is refused rather than risk the policy.
func TestGenerateSchemaDiffSQL_RefusesANamesakeInAnUnknownSchema_FailurePath(t *testing.T) {
	for _, test := range []namesakePolicy{bareViewPolicy, reportingViewPolicy} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
				namesakeDiff(c, test, nil), platform.Postgres)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*active_tenants.*public\.orders\.audit_visible.*`)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestGenerateSchemaDiffSQL_RefusesAPolicyAlongTheSearchPath_FailurePath pins
// a search path that names two schemas. PostgreSQL prints back without a schema
// every relation the path reaches, so under public, reporting the policy's
// unqualified `active_tenants` may be reporting.active_tenants, which the
// column type change rebuilds; the drop would take the policy. The same policy
// under the path public alone is the happy path above.
func TestGenerateSchemaDiffSQL_RefusesAPolicyAlongTheSearchPath_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()),
		namesakeDiff(c, reportingViewPolicy, sessionSemantics("public", "reporting")), platform.Postgres)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `(?s).*reporting\.active_tenants.*public\.orders\.audit_visible.*`)
	c.Assert(sql, qt.Equals, "")
}
