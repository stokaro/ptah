package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// TestToDatabase_RefusesNoForce refuses a NO FORCE node that reaches the
// converter. The parser refuses the statement before it gets here; a caller
// that builds the statements itself meets the same answer, because accepting it
// would drop the node, and a dropped NO FORCE reads as a table nobody asked to
// exempt its owner on.
func TestToDatabase_RefusesNoForce(t *testing.T) {
	c := qt.New(t)
	statements := &ast.StatementList{Statements: []ast.Node{
		ast.NewAlterTableEnableRLS("sites"),
		ast.NewAlterTableForceRLS("sites").SetNoForce(),
	}}

	database, err := sqlschema.ToDatabase(statements, platform.Postgres)

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `.*ALTER TABLE sites NO FORCE ROW LEVEL SECURITY.*`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}
