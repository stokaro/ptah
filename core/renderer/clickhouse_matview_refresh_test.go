package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// TestRenderSQL_ClickHouseMaterializedViewRefreshClause pins where the clause
// goes and what it says.
//
// The position is measured rather than chosen: ClickHouse prints the clause
// between the view name and the storage clause, and that is where its own
// parser expects it (stokaro/ptah#1802).
func TestRenderSQL_ClickHouseMaterializedViewRefreshClause(t *testing.T) {
	tests := []struct {
		name    string
		refresh *ast.MatViewRefreshSpec
		want    string
	}{
		{
			name:    "no schedule renders as it always did",
			refresh: nil,
			want:    "CREATE MATERIALIZED VIEW `mv` ENGINE = MergeTree ORDER BY tuple() AS",
		},
		{
			name:    "every",
			refresh: &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"},
			want:    "CREATE MATERIALIZED VIEW `mv` REFRESH EVERY 1 HOUR ENGINE = MergeTree ORDER BY tuple() AS",
		},
		{
			name:    "after",
			refresh: &ast.MatViewRefreshSpec{Mode: "AFTER", Interval: "30 MINUTE"},
			want:    "CREATE MATERIALIZED VIEW `mv` REFRESH AFTER 30 MINUTE ENGINE = MergeTree ORDER BY tuple() AS",
		},
		{
			name: "every clause at once",
			refresh: &ast.MatViewRefreshSpec{
				Mode: "EVERY", Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE",
				DependsOn: []string{"ptah_test.other"}, Append: true,
			},
			want: "CREATE MATERIALIZED VIEW `mv` REFRESH EVERY 1 DAY OFFSET 2 HOUR " +
				"RANDOMIZE FOR 30 MINUTE DEPENDS ON ptah_test.other APPEND " +
				"ENGINE = MergeTree ORDER BY tuple() AS",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			node := ast.NewCreateMaterializedView("mv").SetBody("SELECT count() AS c FROM src")
			node.Refresh = test.refresh

			sql, err := renderer.RenderSQL(platform.ClickHouse, node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestRenderSQL_ClickHouseAlterMaterializedViewRefresh is the statement that
// changes a schedule without losing the view's rows.
func TestRenderSQL_ClickHouseAlterMaterializedViewRefresh(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQL(platform.ClickHouse, ast.NewAlterMaterializedViewRefresh(
		"analytics.mv", &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "2 HOUR"}))

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `analytics`.`mv` MODIFY REFRESH EVERY 2 HOUR;")
}

// TestRenderSQL_AlterMaterializedViewRefreshIsClickHouseOnly states the
// boundary the whole design rests on: a refresh SCHEDULE is one engine's
// feature, not a shared abstraction. PostgreSQL refreshes a materialized view
// with a statement someone runs, and carries no schedule to alter
// (stokaro/ptah#1625, stokaro/ptah#1802).
func TestRenderSQL_AlterMaterializedViewRefreshIsClickHouseOnly(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite, platform.SQLServer,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, ast.NewAlterMaterializedViewRefresh(
				"mv", &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"}))

			c.Assert(err, qt.IsNotNil)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
