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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			rendered, err := clickhouse.NewWithCapabilities(capability.ClickHouse24()).Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

func TestViewDDL_CapabilityDisabledNamesSkippedObject(t *testing.T) {
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
	renderer := clickhouse.NewWithCapabilities(capability.ClickHouse24())

	t.Run("empty body", func(t *testing.T) {
		c := qt.New(t)
		rendered, err := renderer.Render(ast.NewCreateView("empty_view"))

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, `.*CREATE VIEW "empty_view" requires a non-empty body.*`)
		c.Assert(rendered, qt.Equals, "")
	})

	t.Run("with check option", func(t *testing.T) {
		c := qt.New(t)
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

// TestMaterializedViewDDL_HappyPath pins the three spellings against what
// clickhouse/clickhouse-server:26.7 accepts, and pins each against the
// plausible alternative it is not:
//
//   - the create carries ENGINE = MergeTree ORDER BY tuple() rather than the
//     bare PostgreSQL "CREATE MATERIALIZED VIEW x AS", and carries no POPULATE;
//   - the drop is DROP VIEW rather than DROP MATERIALIZED VIEW, which the
//     server answers with a syntax error;
//   - the refresh stays a named diagnostic even though create and drop emit.
func TestMaterializedViewDDL_HappyPath(t *testing.T) {
	viewBody := "SELECT count() AS c\nFROM `analytics`.`users`"
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "create carries the storage clause and the body",
			node: ast.NewCreateMaterializedView("analytics.user_counts").SetBody(viewBody),
			want: "CREATE MATERIALIZED VIEW `analytics`.`user_counts` " +
				"ENGINE = MergeTree ORDER BY tuple() AS\n" + viewBody + "\n;\n",
		},
		{
			name: "create writes a comment above the statement",
			node: ast.NewCreateMaterializedView("analytics.user_counts").
				SetBody(viewBody).
				SetComment("rolled up per user"),
			want: "-- rolled up per user\n" +
				"CREATE MATERIALIZED VIEW `analytics`.`user_counts` " +
				"ENGINE = MergeTree ORDER BY tuple() AS\n" + viewBody + "\n;\n",
		},
		{
			name: "create ignores the refresh strategy the target has no statement for",
			node: ast.NewCreateMaterializedView("analytics.user_counts").
				SetBody(viewBody).
				SetRefreshStrategy("concurrently"),
			want: "CREATE MATERIALIZED VIEW `analytics`.`user_counts` " +
				"ENGINE = MergeTree ORDER BY tuple() AS\n" + viewBody + "\n;\n",
		},
		{
			name: "drop uses the DROP VIEW spelling",
			node: ast.NewDropMaterializedView("analytics.user_counts"),
			want: "DROP VIEW `analytics`.`user_counts`;\n",
		},
		{
			name: "drop preserves the guard",
			node: ast.NewDropMaterializedView("analytics.user_counts").SetIfExists(),
			want: "DROP VIEW IF EXISTS `analytics`.`user_counts`;\n",
		},
		{
			name: "refresh stays a named diagnostic",
			node: ast.NewRefreshMaterializedView("analytics.user_counts"),
			want: "-- CLICKHOUSE: REFRESH MATERIALIZED VIEW " +
				"\"analytics.user_counts\" is not supported\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := clickhouse.NewWithCapabilities(capability.ClickHouse24()).Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

func TestMaterializedViewDDL_CapabilityDisabledNamesSkippedObject(t *testing.T) {
	caps := capability.ClickHouse24().With(capability.MaterializedViews, false)
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "create",
			node: ast.NewCreateMaterializedView("analytics.user_counts").SetBody("SELECT 1"),
			want: "-- CLICKHOUSE: CREATE MATERIALIZED VIEW " +
				"\"analytics.user_counts\" is not supported\n",
		},
		{
			name: "drop",
			node: ast.NewDropMaterializedView("analytics.user_counts").SetIfExists(),
			want: "-- CLICKHOUSE: DROP MATERIALIZED VIEW " +
				"\"analytics.user_counts\" is not supported\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			rendered, err := clickhouse.NewWithCapabilities(caps).Render(test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(rendered, qt.Equals, test.want)
		})
	}
}

func TestMaterializedViewDDL_FailurePath(t *testing.T) {
	renderer := clickhouse.NewWithCapabilities(capability.ClickHouse24())

	t.Run("empty body", func(t *testing.T) {
		c := qt.New(t)

		rendered, err := renderer.Render(ast.NewCreateMaterializedView("empty_mv"))

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
		c.Assert(err, qt.ErrorMatches, `.*CREATE MATERIALIZED VIEW "empty_mv" requires a non-empty body.*`)
		c.Assert(rendered, qt.Equals, "")
	})

	t.Run("cascade drop", func(t *testing.T) {
		c := qt.New(t)

		rendered, err := renderer.Render(
			ast.NewDropMaterializedView("dependent_mv").SetCascade(),
		)

		c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		c.Assert(err, qt.ErrorMatches, `.*DROP MATERIALIZED VIEW CASCADE.*not supported.*`)
		c.Assert(rendered, qt.Equals, "")
	})
}
