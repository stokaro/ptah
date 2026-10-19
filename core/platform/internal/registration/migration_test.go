package registration_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/ptahdirective"
	"ptah.run/internal/txrequire"
)

// TestRegister_DescribesTheMigrationsOfAnEngineNobodyShips registers an
// engine with migration facts of its own: its YQL reading takes part in a read
// of directives without a dialect, and its concurrent indexes, enum values and
// NOT VALID validations leave a generated migration's transaction. The
// directive scan and the generator read them, though nothing names the
// engine. Each is asked first, so the answers derived from the earlier
// registry exist and have to give way to the new one.
func TestRegister_DescribesTheMigrationsOfAnEngineNobodyShips(t *testing.T) {
	const betweenSystemVariables = "SELECT @@session.sql_mode;\n-- +ptah no_transaction\nSELECT @@session.time_zone;\n"
	nodes := []ast.Node{
		&ast.IndexNode{Name: "idx", Concurrently: true},
		&ast.DropIndexNode{Name: "idx", Concurrently: true},
		&ast.AlterTypeNode{Name: "status", Operations: []ast.TypeOperation{&ast.AddEnumValueOperation{Value: "archived"}}},
		&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.ValidateConstraintOperation{ConstraintName: "c"}}},
	}
	c := qt.New(t)
	c.Assert(slices.Collect(ptahdirective.ConservativeBodies(betweenSystemVariables)), qt.DeepEquals, []string{" no_transaction"})
	for _, node := range nodes {
		c.Assert(txrequire.NodeRequiresAutocommit("migdb", node), qt.IsFalse)
	}

	engine := described("migrating", "", "migdb")
	engine.Lexical = descriptor.Lexical{Lexer: descriptor.LexerYQL}
	engine.ConcurrentIndexes = true
	engine.OnlineAlter = descriptor.OnlineAlterNotValid
	engine.Migration = descriptor.Migration{DirectivesWithoutDialect: true, EnumValuesCommitFirst: true}
	c.Assert(platform.Register(engine), qt.IsNil)

	// Read by YQL, the two @@ open and close one string around the line, and
	// a directive is kept only where every reader sees it.
	c.Assert(slices.Collect(ptahdirective.ConservativeBodies(betweenSystemVariables)), qt.HasLen, 0)
	for _, node := range nodes {
		c.Assert(txrequire.NodeRequiresAutocommit("migdb", node), qt.IsTrue)
	}
}
