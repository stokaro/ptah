package planner_test

import (
	"context"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
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

// policyCascadeSchemas declares a view active_tenants with body, tenants.id
// of type idType, and a policy on orders that reads the view; the database
// holds the view as `SELECT id, name FROM tenants WHERE active`, tenants.id as
// integer, and the same policy.
func policyCascadeSchemas(body, idType string) (*schemamodel.Database, *catalog.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "tenants", StructName: "Tenant"}, {Name: "orders", StructName: "Order"}},
		Fields: []schemamodel.Field{
			{StructName: "Tenant", Name: "id", Type: idType, Primary: true},
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
	desired, current := policyCascadeSchemas(body, "INTEGER")
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	return planner.GenerateSchemaDiffSQL(context.Background(), must.Must(builtin.New()), diff, platform.Postgres)
}

// planPolicyCascadeInStyle plans the change from current to desired in the
// statement style.
func planPolicyCascadeInStyle(c *qt.C, desired *schemamodel.Database, current *catalog.Database, style planext.StatementStyle) ([]string, error) {
	c.Helper()
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), desired, current, platform.Postgres, must.Must(builtin.New())))
	return planner.GenerateSchemaDiffSQLStatementsWithOptions(context.Background(), must.Must(builtin.New()), diff, platform.Postgres,
		planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: style})
}

// materializedPolicyCascadeSchemas declares a materialized view tenant_flags
// over tenants with body, a view active_tenants that reads it, and a policy on
// orders that reads the view; the database holds the materialized view as
// `SELECT id, active FROM tenants` and the rest as declared.
func materializedPolicyCascadeSchemas(body string) (*schemamodel.Database, *catalog.Database) {
	desired, current := policyCascadeSchemas("SELECT id FROM tenant_flags WHERE active", "INTEGER")
	desired.MaterializedViews = []schemamodel.MaterializedView{{Name: "tenant_flags", Body: body}}
	current.Views = []catalog.View{{Name: "active_tenants", Body: "SELECT id FROM tenant_flags WHERE active"}}
	current.MatViews = []catalog.MaterializedView{{Name: "tenant_flags", Body: "SELECT id, active FROM tenants"}}
	return desired, current
}

// TestGenerateSchemaDiffSQL_RefusesACascadeThatDropsAPolicy_FailurePath pins
// stokaro/ptah#4305. Swapping two projected columns is a view change CREATE
// OR REPLACE VIEW cannot make, so the plan drops the view and creates it
// again, and the drop takes the policy that reads it. The owner plans no
// change for the policy, so nothing would put it back; the plan is refused,
// naming it and the change the drop is for.
func TestGenerateSchemaDiffSQL_RefusesACascadeThatDropsAPolicy_FailurePath(t *testing.T) {
	c := qt.New(t)

	sql, err := planPolicyCascade(c, "SELECT name, id FROM tenants WHERE active")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(lostPolicy(planext.StatementStyleAnnotated,
		"the change to view active_tenants, which CREATE OR REPLACE VIEW cannot make", "change view active_tenants")))
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

// policyLossStyles is both statement styles. The refusal holds in each: Ptah's
// own style would drop the view with CASCADE and lose the policy, and the
// exact style would drop it without CASCADE, which the server refuses while
// the policy reads it, or with CASCADE where the drop relies on it.
var policyLossStyles = []struct {
	name  string
	style planext.StatementStyle
}{
	{name: "Ptah's own style", style: planext.StatementStyleAnnotated},
	{name: "the exact style", style: planext.StatementStyleExact},
}

// lostPolicy is the refusal of a plan that drops active_tenants and creates it
// again, for the change because names, where the policy tenant_visible reads
// the view: what the drop would do to the policy in the style, and the two
// migrations that make the change, the second doing then.
func lostPolicy(style planext.StatementStyle, because, then string) string {
	outcome := "the drop's CASCADE would drop it too, with nothing to create it again"
	if style == planext.StatementStyleExact {
		outcome = "PostgreSQL refuses the drop while it does, and a drop with CASCADE would take it too, with nothing to create it again"
	}
	return "unsupported feature: the plan drops view active_tenants and creates it again, for " + because +
		"; ptah.run/pgpolicy/policy public.orders.tenant_visible reads it, and " + outcome +
		". Change ptah.run/pgpolicy/policy public.orders.tenant_visible so it no longer reads active_tenants in one migration, then " +
		then + " in the next"
}

// TestGenerateSchemaDiffSQL_RefusesARetypeThatDropsAPolicy_FailurePath pins
// stokaro/ptah#4965. A column type change under a view drops the view before
// the ALTER and creates it after, and the drop takes the policy that reads
// the view. The owner plans no change for the policy, so the plan is refused,
// naming it, as a view change that drops the view is.
func TestGenerateSchemaDiffSQL_RefusesARetypeThatDropsAPolicy_FailurePath(t *testing.T) {
	for _, test := range policyLossStyles {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := policyCascadeSchemas("SELECT id, name FROM tenants WHERE active", "BIGINT")

			statements, err := planPolicyCascadeInStyle(c, desired, current, test.style)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(lostPolicy(test.style,
				"the type change of tenants.id", "change the type of tenants.id")))
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestGenerateSchemaDiffSQL_RefusesAMaterializedViewChangeThatDropsAPolicy_FailurePath
// pins the policy half of stokaro/ptah#4964. A materialized view change drops
// the view that reads it, and the drop takes the policy that reads that view.
func TestGenerateSchemaDiffSQL_RefusesAMaterializedViewChangeThatDropsAPolicy_FailurePath(t *testing.T) {
	for _, test := range policyLossStyles {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := materializedPolicyCascadeSchemas("SELECT id, active, name FROM tenants")

			statements, err := planPolicyCascadeInStyle(c, desired, current, test.style)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(lostPolicy(test.style,
				"the change to materialized view tenant_flags", "change materialized view tenant_flags")))
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestGenerateSchemaDiffSQL_RefusesARemovedViewAPolicyReads_FailurePath pins
// the refusal for a view the declaration removes while a policy it keeps still
// reads it: the rebuild of the changed materialized view drops the view, and
// nothing creates it again, so the error says the declaration removes it
// rather than naming a change it is created again for.
func TestGenerateSchemaDiffSQL_RefusesARemovedViewAPolicyReads_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		style   planext.StatementStyle
		outcome string
	}{
		{name: "Ptah's own style", style: planext.StatementStyleAnnotated,
			outcome: "the drop's CASCADE would drop it too, with nothing to create it again"},
		{name: "the exact style", style: planext.StatementStyleExact,
			outcome: "PostgreSQL refuses the drop while it does, and a drop with CASCADE would take it too, with nothing to create it again"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := materializedPolicyCascadeSchemas("SELECT id, active, name FROM tenants")
			desired.Views = nil

			statements, err := planPolicyCascadeInStyle(c, desired, current, test.style)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta("unsupported feature: the plan drops view active_tenants, which the declaration "+
				"removes; ptah.run/pgpolicy/policy public.orders.tenant_visible reads it, and "+test.outcome+
				". Change ptah.run/pgpolicy/policy public.orders.tenant_visible so it no longer reads active_tenants first"))
			c.Assert(statements, qt.IsNil)
		})
	}
}

// TestGenerateSchemaDiffSQL_KeepsAPolicyNoRebuildReaches_HappyPath is the
// control for both refusals: with the column type and the materialized view
// unchanged the plan drops nothing the policy reads, and a change to a
// column nothing reads goes ahead.
func TestGenerateSchemaDiffSQL_KeepsAPolicyNoRebuildReaches_HappyPath(t *testing.T) {
	for _, test := range policyLossStyles {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := materializedPolicyCascadeSchemas("SELECT id, active FROM tenants")
			desired.Fields[2].Nullable = false

			statements, err := planPolicyCascadeInStyle(c, desired, current, test.style)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, `ALTER COLUMN "active" SET NOT NULL`)
		})
	}
}

// TestGenerateSchemaDiffSQL_KeepsAPolicyThatReadsANamesake_HappyPath pins a
// policy that reads a view by its bare name, public.active_tenants, while the
// plan rebuilds the view's namesake in the schema reporting for the change to
// the materialized view under it. The bare name resolves in the default
// schema, so the rebuild drops nothing the policy reads, and the plan goes
// ahead.
func TestGenerateSchemaDiffSQL_KeepsAPolicyThatReadsANamesake_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		style planext.StatementStyle
		want  []string
	}{
		{name: "Ptah's own style", style: planext.StatementStyleAnnotated, want: []string{
			`DROP VIEW IF EXISTS "reporting"."active_tenants" CASCADE`,
			`DROP MATERIALIZED VIEW IF EXISTS "reporting"."tenant_flags" CASCADE`,
			createReportingFlags,
			createReportingTenants,
		}},
		{name: "the exact style", style: planext.StatementStyleExact, want: []string{
			`DROP VIEW "reporting"."active_tenants"`,
			`DROP MATERIALIZED VIEW "reporting"."tenant_flags"`,
			createReportingFlags,
			createReportingTenants,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := policyCascadeSchemas("SELECT id, name FROM tenants WHERE active", "INTEGER")
			desired.MaterializedViews = []schemamodel.MaterializedView{{Name: "reporting.tenant_flags", Body: "SELECT id, active, name FROM tenants"}}
			desired.Views = append(desired.Views, schemamodel.View{Name: "reporting.active_tenants", Body: reportingTenants})
			current.MatViews = []catalog.MaterializedView{{Name: "tenant_flags", Schema: "reporting", Body: "SELECT id, active FROM tenants"}}
			current.Views = append(current.Views, catalog.View{Name: "active_tenants", Schema: "reporting", Body: reportingTenants})
			semantics := identifier.ForDialect(platform.Postgres)
			semantics.SearchPath = []string{"public"}
			diff := must.Must(schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
				catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public", IdentifierSemantics: semantics}, nil, must.Must(builtin.New())))

			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(context.Background(), must.Must(builtin.New()), diff,
				platform.Postgres, planext.Options{Capabilities: capability.ForDialect(platform.Postgres), Statements: test.style})

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}

const (
	// reportingTenants is the body of reporting.active_tenants, which reads
	// the materialized view reporting.tenant_flags.
	reportingTenants       = "SELECT id FROM reporting.tenant_flags WHERE active"
	createReportingFlags   = "CREATE MATERIALIZED VIEW \"reporting\".\"tenant_flags\" AS\nSELECT id, active, name FROM tenants"
	createReportingTenants = "CREATE VIEW \"reporting\".\"active_tenants\" AS\n" + reportingTenants
)

// TestGenerateSchemaDiffSQL_RemovesAPolicyWithTheViewItReads_HappyPath pins
// the policy the plan removes: the declaration drops the view and the policy
// that reads it, so the drop of the view loses nothing the schema declares.
// The plan drops the policy first, and the view after it needs no CASCADE.
// The exact style keeps IF EXISTS on the view: the policy's drop is an owner
// operation, which may remove objects the plan cannot name.
func TestGenerateSchemaDiffSQL_RemovesAPolicyWithTheViewItReads_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		style planext.StatementStyle
		want  []string
	}{
		{name: "Ptah's own style", style: planext.StatementStyleAnnotated,
			want: []string{`DROP POLICY "tenant_visible" ON "orders"`, `DROP VIEW IF EXISTS "active_tenants" CASCADE`}},
		{name: "the exact style", style: planext.StatementStyleExact,
			want: []string{`DROP POLICY "tenant_visible" ON "orders"`, `DROP VIEW IF EXISTS "active_tenants"`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := policyCascadeSchemas("SELECT id, name FROM tenants WHERE active", "INTEGER")
			desired.Views = nil
			desired.FeatureObjects = schemaext.Objects{}

			statements, err := planPolicyCascadeInStyle(c, desired, current, test.style)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
