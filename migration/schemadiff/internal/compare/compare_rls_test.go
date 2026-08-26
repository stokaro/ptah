package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

func TestRLSEnabledTables_MatchesSchemaQualifiedTables(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		RLSEnabledTables: []schemamodel.RLSEnabledTable{
			{Table: "auth.users"},
		},
	}
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Schema: "auth", Name: "users", RLSEnabled: true},
		},
	}
	diff := &difftypes.SchemaDiff{}

	compare.RLSEnabledTables(desired, database, diff)

	c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
}
