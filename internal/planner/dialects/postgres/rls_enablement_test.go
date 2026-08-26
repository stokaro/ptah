package postgres_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/planner/dialects/postgres"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// TestPlannerRendersRLSEnablementFromDiff covers stokaro/ptah#1284: the
// planner read RLS enablement from the desired schema's policies alone, so a
// table whose row-level security was turned off in the database produced no
// statement even though the comparator had recorded the difference.
func TestPlannerRendersRLSEnablementFromDiff(t *testing.T) {
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		desired *schemamodel.Database
		want    []string
	}{
		{
			name: "enablement recorded for an existing table",
			diff: &difftypes.SchemaDiff{
				RLSEnabledTablesAdded: []string{"other.secured"},
			},
			desired: &schemamodel.Database{},
			want: []string{
				"-- Enable RLS for other.secured table",
				`ALTER TABLE "other"."secured" ENABLE ROW LEVEL SECURITY;`,
			},
		},
		{
			name: "disablement recorded for an existing table",
			diff: &difftypes.SchemaDiff{
				RLSEnabledTablesRemoved: []string{"public.p"},
			},
			desired: &schemamodel.Database{},
			want: []string{
				"-- Disable RLS for public.p table",
				`ALTER TABLE "public"."p" DISABLE ROW LEVEL SECURITY;`,
			},
		},
		{
			name: "enablement and disablement in one diff",
			diff: &difftypes.SchemaDiff{
				RLSEnabledTablesAdded:   []string{"other.secured"},
				RLSEnabledTablesRemoved: []string{"public.p"},
			},
			desired: &schemamodel.Database{},
			want: []string{
				"-- Enable RLS for other.secured table",
				`ALTER TABLE "other"."secured" ENABLE ROW LEVEL SECURITY;`,
				"-- Disable RLS for public.p table",
				`ALTER TABLE "public"."p" DISABLE ROW LEVEL SECURITY;`,
			},
		},
		{
			name: "a new table carrying a policy is enabled without a diff entry",
			diff: &difftypes.SchemaDiff{
				TablesAdded:      []string{"tenants"},
				RLSPoliciesAdded: []difftypes.RLSPolicyRef{{PolicyName: "tenant_isolation", TableName: "tenants"}},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "tenants", StructName: "Tenant"}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "Tenant", Type: "TEXT"}},
				RLSPolicies: []schemamodel.RLSPolicy{
					{Name: "tenant_isolation", Table: "tenants", PolicyFor: "ALL", ToRoles: "app"},
				},
			},
			want: []string{
				"-- POSTGRES TABLE: tenants --",
				`CREATE TABLE "tenants" (`,
				`  "id" TEXT NOT NULL`,
				`);`,
				"",
				"-- Enable RLS for tenants table",
				`ALTER TABLE "tenants" ENABLE ROW LEVEL SECURITY;`,
				`DROP POLICY IF EXISTS "tenant_isolation" ON "tenants";`,
				`CREATE POLICY "tenant_isolation" ON "tenants" FOR ALL TO "app"`,
				";",
			},
		},
		{
			name: "a table listed twice is enabled once",
			diff: &difftypes.SchemaDiff{
				TablesAdded:           []string{"tenants"},
				RLSEnabledTablesAdded: []string{"tenants"},
				RLSPoliciesAdded: []difftypes.RLSPolicyRef{
					{PolicyName: "tenant_isolation", TableName: "tenants"},
				},
			},
			desired: &schemamodel.Database{
				Tables: []schemamodel.Table{{Name: "tenants", StructName: "Tenant"}},
				Fields: []schemamodel.Field{{Name: "id", StructName: "Tenant", Type: "TEXT"}},
				RLSPolicies: []schemamodel.RLSPolicy{
					{Name: "tenant_isolation", Table: "tenants", PolicyFor: "ALL", ToRoles: "app"},
				},
			},
			want: []string{
				"-- POSTGRES TABLE: tenants --",
				`CREATE TABLE "tenants" (`,
				`  "id" TEXT NOT NULL`,
				`);`,
				"",
				"-- Enable RLS for tenants table",
				`ALTER TABLE "tenants" ENABLE ROW LEVEL SECURITY;`,
				`DROP POLICY IF EXISTS "tenant_isolation" ON "tenants";`,
				`CREATE POLICY "tenant_isolation" ON "tenants" FOR ALL TO "app"`,
				";",
			},
		},
		{
			name: "an existing table keeps its enablement when only a policy changes",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesModified: []difftypes.RLSPolicyDiff{
					{PolicyName: "tenant_isolation", TableName: "tenants", Changes: map[string]string{"using": "a -> b"}},
				},
			},
			desired: &schemamodel.Database{
				RLSPolicies: []schemamodel.RLSPolicy{
					{Name: "tenant_isolation", Table: "tenants", PolicyFor: "ALL", ToRoles: "app"},
				},
			},
			want: []string{
				"-- Modify RLS policy tenant_isolation on table tenants: using",
				`DROP POLICY IF EXISTS "tenant_isolation" ON "tenants";`,
				`CREATE POLICY "tenant_isolation" ON "tenants" FOR ALL TO "app"`,
				";",
			},
		},
		{
			name: "a dropped table is not disabled before it is dropped",
			diff: &difftypes.SchemaDiff{
				TablesRemoved:           []string{"public.legacy"},
				RLSEnabledTablesRemoved: []string{"public.legacy"},
			},
			desired: &schemamodel.Database{},
			want: []string{
				"-- WARNING: This will delete all data!",
				`DROP TABLE IF EXISTS "public"."legacy" CASCADE;`,
			},
		},
		{
			name: "losing every policy is not losing enablement",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesRemoved: []difftypes.RLSPolicyRef{{PolicyName: "tenant_isolation", TableName: "tenants"}},
			},
			desired: &schemamodel.Database{},
			want: []string{
				"-- Drop RLS policy tenant_isolation from table tenants",
				`DROP POLICY IF EXISTS "tenant_isolation" ON "tenants";`,
				"-- NOTE: RLS policies were removed from table tenants - verify if RLS should be disabled --",
			},
		},
		{
			name: "a disabled table gets the statement rather than the advisory comment",
			diff: &difftypes.SchemaDiff{
				RLSPoliciesRemoved:      []difftypes.RLSPolicyRef{{PolicyName: "tenant_isolation", TableName: "tenants"}},
				RLSEnabledTablesRemoved: []string{"tenants"},
			},
			desired: &schemamodel.Database{},
			want: []string{
				"-- Drop RLS policy tenant_isolation from table tenants",
				`DROP POLICY IF EXISTS "tenant_isolation" ON "tenants";`,
				"-- Disable RLS for tenants table",
				`ALTER TABLE "tenants" DISABLE ROW LEVEL SECURITY;`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := postgres.New().GenerateMigrationAST(test.diff, test.desired)
			c.Assert(err, qt.IsNil)

			sql, err := renderer.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)
			c.Assert(strings.Split(strings.TrimRight(sql, "\n"), "\n"), qt.DeepEquals, test.want)
		})
	}
}

// TestPlannerNamesRLSItCannotCarry records that a PostgreSQL-family target
// without the row-level security capability (currently Spanner) receives no
// PostgreSQL-only RLS DDL, and is TOLD so by name.
//
// This test used to assert `nodes, qt.HasLen, 0` — the planner skipped the RLS
// phases outright when the capability was absent, and the plan came back with
// no statement and no diagnostic about the tables it had dropped. Meanwhile
// `schema render` reached the renderer's RLS gate, which returned an error,
// and exited 2 rendering nothing at all. Two commands, one desired schema, one
// target, two different answers (stokaro/ptah#929 items 1 and 4).
//
// The planner now emits the node and the renderer answers, so the assertion is
// on the rendered plan rather than on an empty slice: the objects are still not
// carried, and now the plan names each one. The compare command still reports
// the category, because it reads the diff rather than the planner's output.
func TestPlannerNamesRLSItCannotCarry(t *testing.T) {
	c := qt.New(t)

	diff := &difftypes.SchemaDiff{
		RLSEnabledTablesAdded:   []string{"other.secured"},
		RLSEnabledTablesRemoved: []string{"public.p"},
	}

	nodes, err := postgres.NewForDialect(platform.Spanner, capability.SpannerPostgres()).
		GenerateMigrationAST(diff, &schemamodel.Database{})
	c.Assert(err, qt.IsNil)

	sql, err := renderer.RenderSQL(platform.Spanner, nodes...)
	c.Assert(err, qt.IsNil)

	c.Assert(strings.Split(strings.TrimRight(sql, "\n"), "\n"), qt.DeepEquals, []string{
		"-- SPANNER: row-level security on other.secured is not supported by this target; skipped.",
		"-- SPANNER: row-level security on public.p is not supported by this target; skipped.",
	})
	// The DDL itself must be absent, not merely accompanied by a comment: a
	// renderer that wrote the skip line and then the statement would satisfy a
	// "names the object" assertion while still sending Spanner an ALTER TABLE
	// ... ENABLE ROW LEVEL SECURITY it cannot run.
	c.Assert(sql, qt.Not(qt.Contains), "ROW LEVEL SECURITY;")
}
