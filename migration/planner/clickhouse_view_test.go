package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestGenerateSchemaDiffSQLStatements_ClickHouseViewLifecycle(t *testing.T) {
	viewBody := "SELECT id\nFROM `analytics`.`users`"
	desired := &schemamodel.Database{Views: []schemamodel.View{{
		StructName: "ActiveUsers",
		Name:       "analytics.active_users",
		Body:       viewBody,
	}}}
	tests := []struct {
		name    string
		diff    *difftypes.SchemaDiff
		desired *schemamodel.Database
		want    string
	}{
		{
			name:    "create",
			diff:    &difftypes.SchemaDiff{ViewsAdded: []string{"analytics.active_users"}},
			desired: desired,
			want:    "CREATE VIEW `analytics`.`active_users` AS\n" + viewBody,
		},
		{
			name: "replace",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName: "analytics.active_users",
				Changes:  map[string]string{"body": "SELECT 1 -> " + viewBody},
			}}},
			desired: desired,
			want:    "CREATE OR REPLACE VIEW `analytics`.`active_users` AS\n" + viewBody,
		},
		{
			name:    "drop",
			diff:    &difftypes.SchemaDiff{ViewsRemoved: []string{"analytics.active_users"}},
			desired: &schemamodel.Database{},
			want:    "DROP VIEW IF EXISTS `analytics`.`active_users`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				test.diff,
				test.desired,
				platform.ClickHouse,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}

func TestGenerateSchemaDiffSQLStatements_ClickHouseViewCapabilityDisabled(t *testing.T) {
	desired := &schemamodel.Database{Views: []schemamodel.View{{
		StructName: "ActiveUsers",
		Name:       "analytics.active_users",
		Body:       "SELECT 1",
	}}}
	caps := capability.ClickHouse24().
		With(capability.MaterializedViews, false).
		With(capability.Views, false)
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "create",
			diff: &difftypes.SchemaDiff{ViewsAdded: []string{"analytics.active_users"}},
			want: `-- CLICKHOUSE: CREATE VIEW "analytics.active_users" is not supported`,
		},
		{
			name: "replace",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName: "analytics.active_users",
				Changes:  map[string]string{"body": "SELECT 0 -> SELECT 1"},
			}}},
			want: `-- CLICKHOUSE: CREATE OR REPLACE VIEW "analytics.active_users" is not supported`,
		},
		{
			name: "drop",
			diff: &difftypes.SchemaDiff{ViewsRemoved: []string{"analytics.active_users"}},
			want: `-- CLICKHOUSE: DROP VIEW "analytics.active_users" is not supported`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				test.diff,
				desired,
				platform.ClickHouse,
				planner.Options{Capabilities: caps},
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
		&difftypes.SchemaDiff{
			ViewsRemoved:  []string{"analytics.active_users"},
			TablesRemoved: []string{"analytics.users"},
		},
		&schemamodel.Database{},
		platform.ClickHouse,
	)

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(statements[0], qt.Contains, "DROP VIEW IF EXISTS")
	c.Assert(statements[1], qt.Contains, "DROP TABLE IF EXISTS")
}

func TestGenerateSchemaDiffSQLStatements_ClickHouseOrdersAddedViewDependencies(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				&difftypes.SchemaDiff{ViewsAdded: []string{"analytics.a_dep", "analytics.z_base"}},
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
	desired := clickHouseDependentViews("SELECT n FROM `analytics`.`z_base`")
	tests := []struct {
		name       string
		diff       *difftypes.SchemaDiff
		firstVerb  string
		secondVerb string
	}{
		{
			name: "two replacements",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{
				{ViewName: "analytics.a_dep", Changes: map[string]string{"body": "changed"}},
				{ViewName: "analytics.z_base", Changes: map[string]string{"body": "changed"}},
			}},
			firstVerb:  "CREATE OR REPLACE VIEW `analytics`.`z_base`",
			secondVerb: "CREATE OR REPLACE VIEW `analytics`.`a_dep`",
		},
		{
			name: "added dependent waits for replaced base",
			diff: &difftypes.SchemaDiff{
				ViewsAdded: []string{"analytics.a_dep"},
				ViewsModified: []difftypes.ViewDiff{{
					ViewName: "analytics.z_base",
					Changes:  map[string]string{"body": "changed"},
				}},
			},
			firstVerb:  "CREATE OR REPLACE VIEW `analytics`.`z_base`",
			secondVerb: "CREATE VIEW `analytics`.`a_dep`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				test.diff,
				desired,
				platform.ClickHouse,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 2)
			c.Assert(statements[0], qt.Contains, test.firstVerb)
			c.Assert(statements[1], qt.Contains, test.secondVerb)
		})
	}
}

func clickHouseDependentViews(dependentBody string) *schemamodel.Database {
	return &schemamodel.Database{Views: []schemamodel.View{
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
	desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
		StructName: "UserCounts",
		Name:       "analytics.user_counts",
		Body:       "SELECT count() FROM users",
	}}}

	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&difftypes.SchemaDiff{MaterializedViewsAdded: []string{"analytics.user_counts"}},
		desired,
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
