package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// rlsEnabledIdentityCase is one pair of spellings for a table's RLS enablement
// and what the comparison owes it.
type rlsEnabledIdentityCase struct {
	name      string
	generated []goschema.RLSEnabledTable
	database  []types.DBTable
	assert    func(c *qt.C, diff *difftypes.SchemaDiff)
}

// TestRLSEnabledTablesWithSemantics_QualifiedTableIdentity pins that RLS
// enablement is matched by table identity rather than by the string the table
// was written as.
//
// The database side comes from [types.DBTable.QualifiedName] and carries the
// schema the reader found; a declaration carries whatever the author wrote. So
// `secured` and `public.secured` were two tables, and an unchanged schema
// planned ENABLE ROW LEVEL SECURITY on a table that already had it and DISABLE
// on that same table, on every run.
//
// Same defect as tableMemberKey's (stokaro/ptah#1232) in a comparator that keys
// by raw string, collected as an instance of stokaro/ptah#1276.
func TestRLSEnabledTablesWithSemantics_QualifiedTableIdentity(t *testing.T) {
	c := qt.New(t)

	tests := []rlsEnabledIdentityCase{
		{
			name:      "a bare declaration matches the qualified table the reader reports",
			generated: []goschema.RLSEnabledTable{{Table: "secured"}},
			database:  []types.DBTable{{Schema: "public", Name: "secured", RLSEnabled: true}},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 0)
				c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
			},
		},
		{
			name:      "a qualified declaration matches the same table",
			generated: []goschema.RLSEnabledTable{{Table: "public.secured"}},
			database:  []types.DBTable{{Schema: "public", Name: "secured", RLSEnabled: true}},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 0)
				c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
			},
		},
		{
			// The control against the fix becoming "everything matches".
			name:      "the same table name in another schema is another table",
			generated: []goschema.RLSEnabledTable{{Table: "other.secured"}},
			database:  []types.DBTable{{Schema: "public", Name: "secured", RLSEnabled: true}},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.RLSEnabledTablesAdded, qt.DeepEquals, []string{"other.secured"})
				c.Assert(diff.RLSEnabledTablesRemoved, qt.DeepEquals, []string{"public.secured"})
			},
		},
		{
			// A table whose RLS the database does not have still needs
			// enabling, and it is reported under the name the declaration used
			// because that is what the planner renders.
			name:      "a declared table the database has not enabled is still added",
			generated: []goschema.RLSEnabledTable{{Table: "secured"}},
			database:  []types.DBTable{{Schema: "public", Name: "secured"}},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.RLSEnabledTablesAdded, qt.DeepEquals, []string{"secured"})
				c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
			},
		},
		{
			// And the reverse: RLS on a table nothing declares is still
			// reported for removal, under the qualified name the reader gave.
			name:      "an undeclared enabled table is still removed",
			generated: nil,
			database:  []types.DBTable{{Schema: "public", Name: "unmanaged", RLSEnabled: true}},
			assert: func(c *qt.C, diff *difftypes.SchemaDiff) {
				c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 0)
				c.Assert(diff.RLSEnabledTablesRemoved, qt.DeepEquals, []string{"public.unmanaged"})
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			generated := &goschema.Database{RLSEnabledTables: test.generated}
			database := &types.DBSchema{Tables: test.database}
			diff := &difftypes.SchemaDiff{}

			compare.RLSEnabledTablesWithSemantics(
				generated, database, diff, identifier.ForDialect(platform.Postgres),
			)

			test.assert(c, diff)
		})
	}
}
