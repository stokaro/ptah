package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
)

// renderRowPolicy renders one node through the ClickHouse renderer.
func renderRowPolicy(c *qt.C, node ast.Node) string {
	c.Helper()
	statement, err := renderer.RenderSQL(platform.ClickHouse, node)
	c.Assert(err, qt.IsNil)
	return statement
}

func TestClickHouseRenderer_VisitCreatePolicy(t *testing.T) {
	t.Run("a declared policy becomes a row policy", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("tenant_isolation", "orders").
			SetPolicyFor("ALL").
			SetToRoles("app_reader").
			SetUsingExpression("tenant_id = 1")

		c.Assert(renderRowPolicy(c, node), qt.Equals,
			"CREATE ROW POLICY IF NOT EXISTS `tenant_isolation` ON `orders` "+
				"AS PERMISSIVE FOR SELECT USING tenant_id = 1 TO app_reader;\n")
	})

	t.Run("a replacement is an ALTER, not a second CREATE", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("tenant_isolation", "orders").
			SetUsingExpression("tenant_id = 2").
			SetReplace()

		out := renderRowPolicy(c, node)
		// CREATE OR REPLACE ROW POLICY is a syntax error here, and CREATE ...
		// IF NOT EXISTS against a policy that exists succeeds while changing
		// nothing -- the plan would report success and leave the old filter.
		c.Assert(out, qt.Contains, "ALTER ROW POLICY `tenant_isolation` ON `orders`")
		c.Assert(out, qt.Not(qt.Contains), "CREATE ROW POLICY")
	})

	// The refusals below are the design rather than edge cases: each one is a
	// declaration this target would otherwise accept and then not honor.
	t.Run("a write check is refused, because the engine swallows it", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("tenant_isolation", "orders").
			SetUsingExpression("tenant_id = 1").
			SetWithCheckExpression("tenant_id = 1")

		out := renderRowPolicy(c, node)
		c.Assert(out, qt.Contains, "parses and then ignores")
		c.Assert(out, qt.Not(qt.Contains), "ROW POLICY `tenant_isolation`")
	})

	t.Run("an explicit FOR SELECT is refused, because it cannot converge", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("tenant_isolation", "orders").
			SetPolicyFor("SELECT").
			SetUsingExpression("tenant_id = 1")

		out := renderRowPolicy(c, node)
		c.Assert(out, qt.Contains, "declare FOR ALL or leave it unset")
		c.Assert(out, qt.Not(qt.Contains), "ROW POLICY `tenant_isolation`")
	})

	t.Run("a write operation has no rendering at all", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("tenant_isolation", "orders").
			SetPolicyFor("INSERT").
			SetUsingExpression("tenant_id = 1")

		out := renderRowPolicy(c, node)
		c.Assert(out, qt.Contains, "FOR INSERT")
		c.Assert(out, qt.Not(qt.Contains), "ROW POLICY `tenant_isolation`")
	})

	t.Run("a policy with no filter is refused rather than rendered as USING ;", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewCreatePolicy("tenant_isolation", "orders")

		out := renderRowPolicy(c, node)
		c.Assert(out, qt.Contains, "declares no USING expression")
		c.Assert(out, qt.Not(qt.Contains), "USING ;")
	})
}

func TestClickHouseRenderer_VisitDropPolicy(t *testing.T) {
	t.Run("a guarded drop uses the clause the engine accepts", func(t *testing.T) {
		c := qt.New(t)

		node := ast.NewDropPolicy("tenant_isolation", "orders").SetIfExists()

		c.Assert(renderRowPolicy(c, node), qt.Equals,
			"DROP ROW POLICY IF EXISTS `tenant_isolation` ON `orders`;\n")
	})
}
