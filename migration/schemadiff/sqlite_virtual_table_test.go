package schemadiff_test

import (
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestCompareDoesNotPlanColumnChangesForASQLiteVirtualTable guards the seam the
// virtual-table read opens in the comparator.
//
// A SQLite virtual table has no column list of its own: its columns are the
// module's answer, and when the module is not registered in the build reading
// the database the catalog reports none at all. A desired schema is free to
// declare an ordinary table of the same name -- nothing in Ptah's desired-state
// sources can say "virtual", so that is the only shape it can take -- and
// comparing the two column lists plans `ALTER TABLE docs ADD COLUMN`, which
// SQLite refuses on a virtual table.
//
// The second row is the non-interference control: an ordinary table whose
// columns really are missing must still be planned, or "skip the comparison"
// would have been implemented as "skip every comparison".
func TestCompareDoesNotPlanColumnChangesForASQLiteVirtualTable(t *testing.T) {
	tests := []struct {
		name            string
		dbTable         types.DBTable
		wantModified    bool
		wantColumnAdded []string
	}{
		{
			name: "a virtual table is not compared column by column",
			dbTable: types.DBTable{
				Name:             "docs",
				Type:             "TABLE",
				VirtualModule:    "fts5",
				VirtualArguments: "title, body",
			},
			wantModified: false,
		},
		{
			name: "an ordinary table missing the same columns still is",
			dbTable: types.DBTable{
				Name: "docs",
				Type: "TABLE",
			},
			wantModified:    true,
			wantColumnAdded: []string{"body", "title"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			generated := &goschema.Database{
				Tables: []goschema.Table{{StructName: "Doc", Name: "docs"}},
				Fields: []goschema.Field{
					{StructName: "Doc", Name: "title", Type: "TEXT"},
					{StructName: "Doc", Name: "body", Type: "TEXT"},
				},
			}
			database := &types.DBSchema{Tables: []types.DBTable{tt.dbTable}}

			diff := schemadiff.CompareWithDialect(generated, database, "sqlite")

			c.Assert(diff.TablesAdded, qt.HasLen, 0)
			c.Assert(diff.TablesRemoved, qt.HasLen, 0)
			c.Assert(len(diff.TablesModified) > 0, qt.Equals, tt.wantModified)
			c.Assert(modifiedColumnAdditions(diff.TablesModified), qt.DeepEquals, tt.wantColumnAdded)
		})
	}
}

// modifiedColumnAdditions flattens every planned column addition into one
// sorted list, so a failing row prints the columns rather than the whole diff.
func modifiedColumnAdditions(modified []difftypes.TableDiff) []string {
	var columns []string
	for _, tableDiff := range modified {
		columns = append(columns, tableDiff.ColumnsAdded...)
	}
	sort.Strings(columns)
	return columns
}
