package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestGenerateSchemaDiffSQLStatements_ClickHouseViewLifecycle(t *testing.T) {
	c := qt.New(t)
	viewBody := "SELECT id\nFROM `analytics`.`users`"
	generated := &goschema.Database{Views: []goschema.View{{
		StructName: "ActiveUsers",
		Name:       "analytics.active_users",
		Body:       viewBody,
	}}}
	tests := []struct {
		name      string
		diff      *types.SchemaDiff
		generated *goschema.Database
		want      string
	}{
		{
			name:      "create",
			diff:      &types.SchemaDiff{ViewsAdded: []string{"analytics.active_users"}},
			generated: generated,
			want:      "CREATE VIEW `analytics`.`active_users` AS\n" + viewBody,
		},
		{
			name: "replace",
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName: "analytics.active_users",
				Changes:  map[string]string{"body": "SELECT 1 -> " + viewBody},
			}}},
			generated: generated,
			want:      "CREATE OR REPLACE VIEW `analytics`.`active_users` AS\n" + viewBody,
		},
		{
			name:      "drop",
			diff:      &types.SchemaDiff{ViewsRemoved: []string{"analytics.active_users"}},
			generated: &goschema.Database{},
			want:      "DROP VIEW IF EXISTS `analytics`.`active_users`",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				test.diff,
				test.generated,
				platform.ClickHouse,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}

func TestGenerateSchemaDiffSQLStatements_ClickHouseViewCapabilityDisabled(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{Views: []goschema.View{{
		StructName: "ActiveUsers",
		Name:       "analytics.active_users",
		Body:       "SELECT 1",
	}}}
	caps := capability.ClickHouse24().
		With(capability.MaterializedViews, false).
		With(capability.Views, false)
	tests := []struct {
		name string
		diff *types.SchemaDiff
		want string
	}{
		{
			name: "create",
			diff: &types.SchemaDiff{ViewsAdded: []string{"analytics.active_users"}},
			want: `-- CLICKHOUSE: CREATE VIEW "analytics.active_users" is not supported`,
		},
		{
			name: "replace",
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{{
				ViewName: "analytics.active_users",
				Changes:  map[string]string{"body": "SELECT 0 -> SELECT 1"},
			}}},
			want: `-- CLICKHOUSE: CREATE OR REPLACE VIEW "analytics.active_users" is not supported`,
		},
		{
			name: "drop",
			diff: &types.SchemaDiff{ViewsRemoved: []string{"analytics.active_users"}},
			want: `-- CLICKHOUSE: DROP VIEW "analytics.active_users" is not supported`,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
				test.diff,
				generated,
				platform.ClickHouse,
				caps,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}

func TestGenerateSchemaDiffSQLStatements_ClickHouseDropsViewBeforeSourceTable(t *testing.T) {
	c := qt.New(t)

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&types.SchemaDiff{
			ViewsRemoved:  []string{"analytics.active_users"},
			TablesRemoved: []string{"analytics.users"},
		},
		&goschema.Database{},
		platform.ClickHouse,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, "DROP VIEW IF EXISTS")
	c.Assert(statements[1], qt.Contains, "DROP TABLE IF EXISTS")
}

func TestGenerateSchemaDiffSQLStatements_ClickHouseOrdersAddedViewDependencies(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name          string
		dependentBody string
	}{
		{
			name:          "backtick qualified reference",
			dependentBody: "SELECT n FROM `analytics`.`z_base`",
		},
		{
			name:          "unqualified reference to qualified declaration",
			dependentBody: "SELECT n FROM z_base",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				&types.SchemaDiff{ViewsAdded: []string{"analytics.a_dep", "analytics.z_base"}},
				clickHouseDependentViews(test.dependentBody),
				platform.ClickHouse,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 2)
			c.Assert(statements[0], qt.Contains, "CREATE VIEW `analytics`.`z_base`")
			c.Assert(statements[1], qt.Contains, "CREATE VIEW `analytics`.`a_dep`")
		})
	}
}

func TestGenerateSchemaDiffSQLStatements_ClickHouseOrdersReplacementDependencies(t *testing.T) {
	c := qt.New(t)
	generated := clickHouseDependentViews("SELECT n FROM `analytics`.`z_base`")
	tests := []struct {
		name       string
		diff       *types.SchemaDiff
		firstVerb  string
		secondVerb string
	}{
		{
			name: "two replacements",
			diff: &types.SchemaDiff{ViewsModified: []types.ViewDiff{
				{ViewName: "analytics.a_dep", Changes: map[string]string{"body": "changed"}},
				{ViewName: "analytics.z_base", Changes: map[string]string{"body": "changed"}},
			}},
			firstVerb:  "CREATE OR REPLACE VIEW `analytics`.`z_base`",
			secondVerb: "CREATE OR REPLACE VIEW `analytics`.`a_dep`",
		},
		{
			name: "added dependent waits for replaced base",
			diff: &types.SchemaDiff{
				ViewsAdded: []string{"analytics.a_dep"},
				ViewsModified: []types.ViewDiff{{
					ViewName: "analytics.z_base",
					Changes:  map[string]string{"body": "changed"},
				}},
			},
			firstVerb:  "CREATE OR REPLACE VIEW `analytics`.`z_base`",
			secondVerb: "CREATE VIEW `analytics`.`a_dep`",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				test.diff,
				generated,
				platform.ClickHouse,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 2)
			c.Assert(statements[0], qt.Contains, test.firstVerb)
			c.Assert(statements[1], qt.Contains, test.secondVerb)
		})
	}
}

func clickHouseDependentViews(dependentBody string) *goschema.Database {
	return &goschema.Database{Views: []goschema.View{
		{
			StructName: "Dependent",
			Name:       "analytics.a_dep",
			Body:       dependentBody,
		},
		{
			StructName: "Base",
			Name:       "analytics.z_base",
			Body:       "SELECT 1 AS n",
		},
	}}
}

// TestGenerateSchemaDiffSQLStatements_ClickHouseMaterializedViewCarriesItsBody
// pins the whole plan surface, not the renderer alone: the statement the plan
// hands to the executor has to be the executable one, qualified name and query
// intact.
func TestGenerateSchemaDiffSQLStatements_ClickHouseMaterializedViewCarriesItsBody(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		StructName: "UserCounts",
		Name:       "analytics.user_counts",
		Body:       "SELECT count() FROM users",
	}}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&types.SchemaDiff{MaterializedViewsAdded: []string{"analytics.user_counts"}},
		generated,
		platform.ClickHouse,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(
		statements[0],
		qt.Contains,
		"CREATE MATERIALIZED VIEW `analytics`.`user_counts` "+
			"ENGINE = MergeTree ORDER BY tuple() AS\nSELECT count() FROM users",
	)
}
