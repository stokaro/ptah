package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/mysql"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestPlanner_ChangesIndexVisibilityInPlace hides an index from the optimizer
// with `ALTER INDEX`, which MySQL 8.4.11 and MariaDB 11.8.9 apply without a
// rebuild (stokaro/ptah#3853).
func TestPlanner_ChangesIndexVisibilityInPlace(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{IndexVisibilityChanged: []difftypes.IndexVisibilityChange{
		{TableName: "orders", Name: "k_total", Invisible: true},
	}}

	nodes, err := mysql.NewForDialect(platform.MySQL, capability.MySQL84()).GenerateMigrationAST(diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.MySQL, capability.MySQL84(), nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "ALTER TABLE `orders` ALTER INDEX `k_total` INVISIBLE;")
}
