package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func TestGenerateSchemaDiffAST_SQLServerInvalidSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog(
		"SQL_Latin1_General_CP1_CI_AS",
	)
	diff := &difftypes.SchemaDiff{
		IdentifierSemantics: &semantics,
	}

	nodes, err := planner.GenerateSchemaDiffAST(
		diff,

		platform.SQLServer,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*invalid identifier semantics snapshot.*`)
	c.Assert(nodes, qt.IsNil)
}
