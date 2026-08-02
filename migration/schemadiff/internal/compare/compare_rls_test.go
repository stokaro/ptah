package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestRLSEnabledTables_MatchesSchemaQualifiedTables(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		RLSEnabledTables: []goschema.RLSEnabledTable{
			{Table: "auth.users"},
		},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{
			{Schema: "auth", Name: "users", RLSEnabled: true},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSEnabledTables(generated, database, diff)

	c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
}
