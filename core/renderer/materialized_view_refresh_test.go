package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
)

// matviewBody is the same projection on every row, so a difference in the
// outcome can only come from the refresh strategy.
const matviewBody = "SELECT id, count(*) AS c FROM users GROUP BY id"

// matviewRenderingDialects are the targets whose default capability preset
// hosts materialized views, and therefore the targets where a declared refresh
// strategy used to be discarded into DDL that exited 0 (stokaro/ptah#1523).
var matviewRenderingDialects = []string{
	platform.Postgres,
	platform.CockroachDB,
	platform.YugabyteDB,
	platform.ClickHouse,
}

func assertRefreshStrategyRendered(c *qt.C, dialect, sql string, err error) {
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "CREATE MATERIALIZED VIEW",
		qt.Commentf("dialect %s rendered:\n%s", dialect, sql))
}

func assertRefreshStrategyAccepted(c *qt.C, dialect string, err error) {
	c.Assert(err, qt.IsNil, qt.Commentf("dialect %s", dialect))
}

// assertRefreshStrategyError pins the three things the refusal owes the
// operator: the sentinel a caller can branch on, the value they wrote spelled
// as they wrote it, and the target that refused it.
func assertRefreshStrategyError(c *qt.C, dialect, declared string, err error) {
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, `refresh_strategy "`+declared+`"`)
	c.Assert(err.Error(), qt.Contains, dialect)
	c.Assert(err.Error(), qt.Contains, `materialized view "user_counts"`)
}

// assertRefreshStrategyRefused adds the half that closes the issue to the
// message assertions: a partial statement would still reach a server.
func assertRefreshStrategyRefused(declared string) func(c *qt.C, dialect, sql string, err error) {
	return func(c *qt.C, dialect, sql string, err error) {
		assertRefreshStrategyError(c, dialect, declared, err)
		c.Assert(sql, qt.Equals, "")
	}
}

func assertRefreshStrategyRejected(declared string) func(c *qt.C, dialect string, err error) {
	return func(c *qt.C, dialect string, err error) {
		assertRefreshStrategyError(c, dialect, declared, err)
	}
}

type refreshStrategyCase struct {
	name       string
	strategy   string
	assert     func(c *qt.C, dialect, sql string, err error)
	assertOnly func(c *qt.C, dialect string, err error)
}

func refreshStrategyCases() []refreshStrategyCase {
	return []refreshStrategyCase{
		{
			name:       "an unset strategy is the default policy",
			strategy:   "",
			assert:     assertRefreshStrategyRendered,
			assertOnly: assertRefreshStrategyAccepted,
		},
		{
			name:       "manual is accepted",
			strategy:   "manual",
			assert:     assertRefreshStrategyRendered,
			assertOnly: assertRefreshStrategyAccepted,
		},
		{
			name:       "manual is folded before it is judged",
			strategy:   "  MANUAL  ",
			assert:     assertRefreshStrategyRendered,
			assertOnly: assertRefreshStrategyAccepted,
		},
		{
			name:       "concurrently is refused",
			strategy:   "concurrently",
			assert:     assertRefreshStrategyRefused("concurrently"),
			assertOnly: assertRefreshStrategyRejected("concurrently"),
		},
		{
			name:       "an uppercase concurrently is refused",
			strategy:   "CONCURRENTLY",
			assert:     assertRefreshStrategyRefused("CONCURRENTLY"),
			assertOnly: assertRefreshStrategyRejected("CONCURRENTLY"),
		},
		{
			name:       "a scheduled strategy is refused",
			strategy:   "every 5 minutes",
			assert:     assertRefreshStrategyRefused("every 5 minutes"),
			assertOnly: assertRefreshStrategyRejected("every 5 minutes"),
		},
	}
}

// TestRenderSQL_MaterializedViewRefreshStrategy measures the surface
// stokaro/ptah#1523 measured: the same materialized view rendered through
// RenderSQL for three strategies produced byte-identical DDL at err=<nil> on
// every dialect, so the declaration was lost with nothing said. Only the policy
// a target can carry renders now; the rest is refused by name.
func TestRenderSQL_MaterializedViewRefreshStrategy(t *testing.T) {
	for _, dialect := range matviewRenderingDialects {
		for _, test := range refreshStrategyCases() {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				node := ast.NewCreateMaterializedView("user_counts").
					SetBody(matviewBody).
					SetRefreshStrategy(test.strategy)

				sql, err := renderer.RenderSQL(dialect, node)

				test.assert(c, dialect, sql, err)
			})
		}
	}
}

// TestValidateSchema_MaterializedViewRefreshStrategy pins the declaration-time
// half. ValidateSchema renders nothing, and migration planning runs it before
// it emits any statement, so an apply refuses the declaration rather than
// discovering it halfway through a script.
func TestValidateSchema_MaterializedViewRefreshStrategy(t *testing.T) {
	for _, dialect := range matviewRenderingDialects {
		for _, test := range refreshStrategyCases() {
			t.Run(dialect+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)

				database := &goschema.Database{
					MaterializedViews: []goschema.MaterializedView{{
						Name:            "user_counts",
						Body:            matviewBody,
						RefreshStrategy: test.strategy,
					}},
				}

				err := renderer.ValidateSchema(database, dialect)

				test.assertOnly(c, dialect, err)
			})
		}
	}
}

// TestGetOrderedCreateStatements_MaterializedViewRefreshStrategy covers the
// whole-schema render path, which builds its own AST and renders node by node.
func TestGetOrderedCreateStatements_MaterializedViewRefreshStrategy(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		assert   func(c *qt.C, statements []string, err error)
	}{
		{
			name:     "manual renders the create",
			strategy: "manual",
			assert: func(c *qt.C, statements []string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(statements, qt.HasLen, 1)
				c.Assert(statements[0], qt.Contains, "CREATE MATERIALIZED VIEW")
			},
		},
		{
			name:     "concurrently is refused and nothing is rendered",
			strategy: "concurrently",
			assert: func(c *qt.C, statements []string, err error) {
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err.Error(), qt.Contains, `refresh_strategy "concurrently"`)
				c.Assert(statements, qt.IsNil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := &goschema.Database{
				MaterializedViews: []goschema.MaterializedView{{
					Name:            "user_counts",
					Body:            matviewBody,
					RefreshStrategy: test.strategy,
				}},
			}

			statements, err := renderer.GetOrderedCreateStatements(database, platform.Postgres)

			test.assert(c, statements, err)
		})
	}
}

// TestRenderSQL_MaterializedViewRefreshStrategyLeavesHostlessTargetsAlone is
// the non-interference control. A target whose materialized views are switched
// off already answers for the whole object, and that answer must not change
// into a complaint about the refresh policy: the object kind is the larger
// problem and the one worth reporting.
func TestRenderSQL_MaterializedViewRefreshStrategyLeavesHostlessTargetsAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		assert  func(c *qt.C, sql string, err error)
	}{
		{
			name:    "mysql refuses the object kind",
			dialect: platform.MySQL,
			assert:  assertMaterializedViewsUnsupported,
		},
		{
			name:    "mariadb refuses the object kind",
			dialect: platform.MariaDB,
			assert:  assertMaterializedViewsUnsupported,
		},
		{
			name:    "sqlite refuses the object kind",
			dialect: platform.SQLite,
			assert:  assertMaterializedViewsUnsupported,
		},
		{
			name:    "sqlserver refuses the object kind",
			dialect: platform.SQLServer,
			assert:  assertMaterializedViewsUnsupported,
		},
		{
			name:    "spanner keeps skipping the object with a comment",
			dialect: platform.Spanner,
			assert: func(c *qt.C, sql string, err error) {
				c.Assert(err, qt.IsNil)
				c.Assert(sql, qt.Contains, "materialized view user_counts is not supported by this target; skipped.")
				c.Assert(sql, qt.Not(qt.Contains), "CREATE MATERIALIZED VIEW")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			node := ast.NewCreateMaterializedView("user_counts").
				SetBody(matviewBody).
				SetRefreshStrategy("concurrently")

			sql, err := renderer.RenderSQL(test.dialect, node)

			test.assert(c, sql, err)
		})
	}
}

func assertMaterializedViewsUnsupported(c *qt.C, sql string, err error) {
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, "materialized views are not supported")
	c.Assert(err.Error(), qt.Not(qt.Contains), "refresh_strategy")
	c.Assert(sql, qt.Equals, "")
}
