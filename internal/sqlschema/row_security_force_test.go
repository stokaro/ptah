package sqlschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/pgpolicyprovider"
	"ptah.run/internal/sqlschema"
)

// TestToDatabase_RefusesNoForce refuses a common row-security node. SQL
// statements reach the converter through the selected policy owner instead.
func TestToDatabase_RefusesNoForce(t *testing.T) {
	c := qt.New(t)
	statements := &ast.StatementList{Statements: []ast.Node{
		ast.NewAlterTableForceRLS("sites").SetNoForce(),
	}}

	database, err := sqlschema.ToDatabase(statements, platform.Postgres)

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `.*\*ast.AlterTableForceRLSNode`)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}

// readWithPolicyOwners isolates policy statements from other optional readers.
func readWithPolicyOwners(data []byte, dialect string) (schemamodel.Database, *ast.StatementList, error) {
	return sqlschema.ReadWith(context.Background(), data, dialect, nil, sqlschema.Options{Owners: must.Must(sqlext.NewSet(pgpolicyprovider.Provider().SQL...))})
}
