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
//
// The expectation is data because every row asks the same question -- which
// tables the comparison planned to enable or disable -- and both answers are
// already lists of names. A row answering with its own assertions hides that
// the question is shared, and hands the checker to a field the tooling cannot
// follow. See AGENTS.md, "A Table Row Carries Data, Not A Checker".
type rlsEnabledIdentityCase struct {
	name        string
	generated   []goschema.RLSEnabledTable
	database    []types.DBTable
	wantAdded   []string
	wantRemoved []string
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
	tests := []rlsEnabledIdentityCase{
		{
			name:      "a bare declaration matches the qualified table the reader reports",
			generated: []goschema.RLSEnabledTable{{Table: "secured"}},
			database:  []types.DBTable{{Schema: "public", Name: "secured", RLSEnabled: true}},
		},
		{
			name:      "a qualified declaration matches the same table",
			generated: []goschema.RLSEnabledTable{{Table: "public.secured"}},
			database:  []types.DBTable{{Schema: "public", Name: "secured", RLSEnabled: true}},
		},
		{
			// The control against the fix becoming "everything matches".
			name:        "the same table name in another schema is another table",
			generated:   []goschema.RLSEnabledTable{{Table: "other.secured"}},
			database:    []types.DBTable{{Schema: "public", Name: "secured", RLSEnabled: true}},
			wantAdded:   []string{"other.secured"},
			wantRemoved: []string{"public.secured"},
		},
		{
			// A table whose RLS the database does not have still needs
			// enabling, and it is reported under the name the declaration used
			// because that is what the planner renders.
			name:      "a declared table the database has not enabled is still added",
			generated: []goschema.RLSEnabledTable{{Table: "secured"}},
			database:  []types.DBTable{{Schema: "public", Name: "secured"}},
			wantAdded: []string{"secured"},
		},
		{
			// And the reverse: RLS on a table nothing declares is still
			// reported for removal, under the qualified name the reader gave.
			name:        "an undeclared enabled table is still removed",
			generated:   nil,
			database:    []types.DBTable{{Schema: "public", Name: "unmanaged", RLSEnabled: true}},
			wantRemoved: []string{"public.unmanaged"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			generated := &goschema.Database{RLSEnabledTables: test.generated}
			database := &types.DBSchema{Tables: test.database}
			diff := &difftypes.SchemaDiff{}

			compare.RLSEnabledTablesWithSemantics(
				generated, database, diff, identifier.ForDialect(platform.Postgres),
			)

			c.Assert(diff.RLSEnabledTablesAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.RLSEnabledTablesRemoved, qt.DeepEquals, test.wantRemoved)
		})
	}
}
