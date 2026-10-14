package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// Default-privilege syntax requires a selected owner. The owner package tests
// the operation grammar; this boundary must refuse a missing owner by name.
func TestParser_DefaultPrivilegesRequireSelectedOwner(t *testing.T) {
	c := qt.New(t)
	statements, err := parser.NewParser("ALTER DEFAULT PRIVILEGES FOR ROLE owner GRANT SELECT ON TABLES TO reader;", parser.WithDialect(platform.Postgres)).Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported ALTER target: DEFAULT at position \d+`)
	c.Assert(statements, qt.IsNil)
}

// ALTER TABLE remains available without selecting an extension parser.
func TestParser_ParseAlterTargetDispatch(t *testing.T) {
	c := qt.New(t)

	statements, err := parser.NewParser("ALTER TABLE users ADD COLUMN email TEXT;").Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	alter, ok := statements.Statements[0].(*ast.AlterTableNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(alter.Name, qt.Equals, "users")

	_, err = parser.NewParser("ALTER SEQUENCE app.counter RESTART;").Parse()
	c.Assert(err, qt.ErrorMatches, `unsupported ALTER target: SEQUENCE at position \d+`)
}
