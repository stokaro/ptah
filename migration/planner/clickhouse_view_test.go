package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
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
			diff:    &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{{Name: "analytics.active_users", Body: viewBody}}},
			desired: desired,
			want:    "CREATE VIEW `analytics`.`active_users` AS\n" + viewBody,
		},
		{
			name: "replace",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName: "analytics.active_users",
				// The view this change leaves behind travels WITH it
				// (stokaro/ptah#2315).
				Desired: schemamodel.View{Name: "analytics.active_users", Body: viewBody},
				Changes: map[string]string{"body": "SELECT 1 -> " + viewBody},
			}}},
			desired: desired,
			want:    "CREATE OR REPLACE VIEW `analytics`.`active_users` AS\n" + viewBody,
		},
		{
			name:    "drop",
			diff:    &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "analytics.active_users"}}},
			desired: &schemamodel.Database{},
			want:    "DROP VIEW IF EXISTS `analytics`.`active_users`",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatements(
				test.diff,

				platform.ClickHouse,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 1)
			c.Assert(statements[0], qt.Contains, test.want)
		})
	}
}

func TestGenerateSchemaDiffSQLStatements_ClickHouseViewCapabilityDisabled(t *testing.T) {
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
			diff: &difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{
				{Name: "analytics.active_users", Body: "SELECT 1"},
			}},
			want: `-- CLICKHOUSE: CREATE VIEW "analytics.active_users" is not supported`,
		},
		{
			name: "replace",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{{
				ViewName: "analytics.active_users",
				Desired:  schemamodel.View{Name: "analytics.active_users", Body: "SELECT 1"},
				Changes:  map[string]string{"body": "SELECT 0 -> SELECT 1"},
			}}},
			want: `-- CLICKHOUSE: CREATE OR REPLACE VIEW "analytics.active_users" is not supported`,
		},
		{
			name: "drop",
			diff: &difftypes.SchemaDiff{ViewsRemoved: difftypes.ViewChanges{{Name: "analytics.active_users"}}},
			want: `-- CLICKHOUSE: DROP VIEW "analytics.active_users" is not supported`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
				test.diff,

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
			ViewsRemoved:  difftypes.ViewChanges{{Name: "analytics.active_users"}},
			TablesRemoved: []string{"analytics.users"},
		},

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
				&difftypes.SchemaDiff{ViewsAdded: difftypes.ViewChanges{

					{Name: "analytics.a_dep", Body: test.dependentBody},
					{Name: "analytics.z_base", Body: "SELECT 1 AS n"},
				}},

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
	tests := []struct {
		name       string
		diff       *difftypes.SchemaDiff
		firstVerb  string
		secondVerb string
	}{
		{
			name: "two replacements",
			diff: &difftypes.SchemaDiff{ViewsModified: []difftypes.ViewDiff{
				{
					ViewName: "analytics.a_dep",
					Desired:  schemamodel.View{Name: "analytics.a_dep", Body: "SELECT n FROM `analytics`.`z_base`"},
					Changes:  map[string]string{"body": "changed"},
				},
				{
					ViewName: "analytics.z_base",
					Desired:  schemamodel.View{Name: "analytics.z_base", Body: "SELECT 1 AS n"},
					Changes:  map[string]string{"body": "changed"},
				},
			}},
			firstVerb:  "CREATE OR REPLACE VIEW `analytics`.`z_base`",
			secondVerb: "CREATE OR REPLACE VIEW `analytics`.`a_dep`",
		},
		{
			name: "added dependent waits for replaced base",
			diff: &difftypes.SchemaDiff{
				ViewsAdded: difftypes.ViewChanges{
					{Name: "analytics.a_dep", Body: "SELECT n FROM `analytics`.`z_base`"},
				},
				ViewsModified: []difftypes.ViewDiff{{
					ViewName: "analytics.z_base",
					Desired:  schemamodel.View{Name: "analytics.z_base", Body: "SELECT 1 AS n"},
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

				platform.ClickHouse,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.HasLen, 2)
			c.Assert(statements[0], qt.Contains, test.firstVerb)
			c.Assert(statements[1], qt.Contains, test.secondVerb)
		})
	}
}

// TestGenerateSchemaDiffSQLStatements_ClickHouseMaterializedViewCarriesItsBody
// pins the whole plan surface, not the renderer alone: the statement the plan
// hands to the executor has to be the executable one, qualified name and query
// intact.
func TestGenerateSchemaDiffSQLStatements_ClickHouseMaterializedViewCarriesItsBody(t *testing.T) {
	c := qt.New(t)
	statements, err := planner.GenerateSchemaDiffSQLStatements(
		&difftypes.SchemaDiff{MaterializedViewsAdded: difftypes.MaterializedViewChanges{{Name: "analytics.user_counts", Body: "SELECT count() FROM users"}}},

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
