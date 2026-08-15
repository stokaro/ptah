package renderer_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
)

func TestMaterializedViewRefreshStrategyFailsClosedBeforeRendering(t *testing.T) {
	dialects := []string{
		platform.Postgres,
		platform.MySQL,
		platform.MariaDB,
		platform.SQLite,
		platform.ClickHouse,
		platform.SQLServer,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Spanner,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			node := materializedViewNode("concurrently")

			sql, err := renderer.RenderSQL(dialect, node)

			c.Assert(sql, qt.Equals, "")
			assertMaterializedViewRefreshStrategyError(c, err, dialect, "concurrently")
		})
	}
}

func TestMaterializedViewScheduledRefreshStrategyFailsClosedBeforeRendering(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.ClickHouse} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, materializedViewNode("every 5 minutes"))

			c.Assert(sql, qt.Equals, "")
			assertMaterializedViewRefreshStrategyError(c, err, dialect, "every 5 minutes")
		})
	}
}

func TestMaterializedViewRefreshStrategyFailsClosedBeforeWholeSchemaRendering(t *testing.T) {
	tests := []struct {
		dialect  string
		strategy string
	}{
		{dialect: platform.Postgres, strategy: "concurrently"},
		{dialect: platform.ClickHouse, strategy: "every 5 minutes"},
	}

	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatements(
				materializedViewDatabase(test.strategy),
				test.dialect,
			)

			c.Assert(statements, qt.IsNil)
			assertMaterializedViewRefreshStrategyError(c, err, test.dialect, test.strategy)
		})
	}
}

func TestMaterializedViewRefreshStrategyVisitorPathFailsClosedAndResetsOutput(t *testing.T) {
	c := qt.New(t)
	r, err := renderer.NewRenderer(platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(materializedViewNode("manual").Accept(r), qt.IsNil)
	c.Assert(r.Output(), qt.Not(qt.Equals), "")

	err = materializedViewNode("concurrently").Accept(r)

	assertMaterializedViewRefreshStrategyError(c, err, platform.Postgres, "concurrently")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestMaterializedViewManualRefreshStrategyRenders(t *testing.T) {
	for _, dialect := range []string{
		platform.Postgres,
		platform.ClickHouse,
		platform.CockroachDB,
		platform.YugabyteDB,
	} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(dialect, materializedViewNode("manual"))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW")
		})
	}
}

func materializedViewNode(strategy string) *ast.CreateMaterializedViewNode {
	return ast.NewCreateMaterializedView("analytics.user_counts").
		SetBody("SELECT count(*) AS total FROM analytics.users").
		SetRefreshStrategy(strategy)
}

func materializedViewDatabase(strategy string) *goschema.Database {
	return &goschema.Database{MaterializedViews: []goschema.MaterializedView{{
		Name:            "analytics.user_counts",
		Body:            "SELECT count(*) AS total FROM analytics.users",
		RefreshStrategy: strategy,
	}}}
}

func assertMaterializedViewRefreshStrategyError(
	c *qt.C,
	err error,
	dialect string,
	strategy string,
) {
	c.Helper()
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(
		err,
		qt.ErrorMatches,
		fmt.Sprintf(
			`%s cannot represent materialized view "analytics.user_counts" refresh strategy %q; only "manual" is currently supported`,
			dialect,
			strategy,
		),
	)
	var capabilityErr *ptaherr.CapabilityError
	c.Assert(err, qt.ErrorAs, &capabilityErr)
	c.Assert(capabilityErr.Feature, qt.Equals, "materialized view refresh strategy")
}
