package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
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
