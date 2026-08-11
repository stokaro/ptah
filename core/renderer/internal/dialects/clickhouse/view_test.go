package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/clickhouse"
)

func TestViewDDL_HappyPath(t *testing.T) {
	c := qt.New(t)
	viewBody := "SELECT id, name\nFROM `analytics`.`users`\nWHERE active = true"
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "create preserves qualified identity and body",
			node: ast.NewCreateView("analytics.active_users").SetBody(viewBody),
			want: "CREATE VIEW `analytics`.`active_users` AS\n" + viewBody + "\n;\n",
		},
		{
			name: "replace uses native replace syntax",
			node: ast.NewCreateView("analytics.active_users").SetBody(viewBody).SetReplace(),
			want: "CREATE OR REPLACE VIEW `analytics`.`active_users` AS\n" + viewBody + "\n;\n",
		},
		{
			name: "drop preserves guard and qualified identity",
			node: ast.NewDropView("analytics.active_users").SetIfExists(),
			want: "DROP VIEW IF EXISTS `analytics`.`active_users`;\n",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			rendered, err := clickhouse.NewWithCapabilities(capability.ClickHouse24()).Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

func TestViewDDL_CapabilityDisabledNamesSkippedObject(t *testing.T) {
	c := qt.New(t)
	caps := capability.ClickHouse24().
		With(capability.MaterializedViews, false).
		With(capability.Views, false)
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "create",
			node: ast.NewCreateView("analytics.active_users").SetBody("SELECT 1"),
			want: "-- CLICKHOUSE: CREATE VIEW \"analytics.active_users\" is not supported\n",
		},
		{
			name: "replace",
			node: ast.NewCreateView("analytics.active_users").SetBody("SELECT 1").SetReplace(),
			want: "-- CLICKHOUSE: CREATE OR REPLACE VIEW \"analytics.active_users\" is not supported\n",
		},
		{
			name: "drop",
			node: ast.NewDropView("analytics.active_users").SetIfExists(),
			want: "-- CLICKHOUSE: DROP VIEW \"analytics.active_users\" is not supported\n",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			rendered, err := clickhouse.NewWithCapabilities(caps).Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

func TestViewDDL_NilCapabilitySetIsConservative(t *testing.T) {
	c := qt.New(t)
	rendered, err := clickhouse.NewWithCapabilities(nil).Render(
		ast.NewCreateView("analytics.active_users").SetBody("SELECT 1"),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(
		rendered,
		qt.Equals,
		"-- CLICKHOUSE: CREATE VIEW \"analytics.active_users\" is not supported\n",
	)
}

func TestCreateViewDDL_FailurePath(t *testing.T) {
	c := qt.New(t)
	renderer := clickhouse.NewWithCapabilities(capability.ClickHouse24())

	c.Run("empty body", func(c *qt.C) {
		rendered, err := renderer.Render(ast.NewCreateView("empty_view"))

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, `.*CREATE VIEW "empty_view" requires a non-empty body.*`)
		c.Assert(rendered, qt.Equals, "")
	})

	c.Run("with check option", func(c *qt.C) {
		rendered, err := renderer.Render(
			ast.NewCreateView("checked_view").SetBody("SELECT 1").SetWithCheck(true),
		)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `.*WITH CHECK OPTION.*not supported.*`)
		c.Assert(rendered, qt.Equals, "")
	})
}

func TestDropViewDDL_FailurePath(t *testing.T) {
	c := qt.New(t)
	rendered, err := clickhouse.NewWithCapabilities(capability.ClickHouse24()).Render(
		ast.NewDropView("dependent_view").SetCascade(),
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*DROP VIEW CASCADE.*not supported.*`)
	c.Assert(rendered, qt.Equals, "")
}

func TestMaterializedViewDDL_RemainsNamedDiagnostic(t *testing.T) {
	c := qt.New(t)
	rendered, err := clickhouse.NewWithCapabilities(capability.ClickHouse24()).Render(
		ast.NewCreateMaterializedView("analytics.user_counts").SetBody("SELECT count() FROM users"),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(
		rendered,
		qt.Equals,
		"-- CLICKHOUSE: CREATE MATERIALIZED VIEW \"analytics.user_counts\" is not supported\n",
	)
}
