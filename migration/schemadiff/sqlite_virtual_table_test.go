package schemadiff_test

import (
	"sort"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/sqlite/sqlitetable"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// observedVirtualTable is the facet the SQLite reader records on a virtual
// table.
func observedVirtualTable(module, arguments string) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&sqlitetable.ObservedVirtual{Virtual: sqlitetable.Virtual{Module: module, Arguments: arguments}}))
}

// declaredVirtualTable is the facet a SQLite SQL document declares a virtual
// table with.
func declaredVirtualTable(module, arguments string) schemaext.Facets {
	return must.Must(schemaext.NewFacets(&sqlitetable.DesiredVirtual{Virtual: sqlitetable.Virtual{Module: module, Arguments: arguments}}))
}

// TestCompareDoesNotPlanColumnChangesForASQLiteVirtualTable guards the seam the
// virtual-table read opens in the comparator, through the neutral contract a
// table facet that defines its table follows ([schemaext.TableDefinition]).
//
// A SQLite virtual table has no column list of its own: its columns are the
// module's answer, and when the module is not registered in the build reading
// the database the catalog reports none at all. Comparing a virtual table's
// column list with an ordinary table's plans `ALTER TABLE docs ADD COLUMN`, or
// DROP COLUMN the other way round, against an object those statements cannot
// reach.
//
// The ordinary row is the non-interference control: an ordinary table whose
// columns really are missing must still be planned, or "skip the comparison"
// would have been implemented as "skip every comparison".
func TestCompareDoesNotPlanColumnChangesForASQLiteVirtualTable(t *testing.T) {
	tests := []struct {
		name               string
		desiredFacets      schemaext.Facets
		desiredFields      []schemamodel.Field
		currentTable       catalog.Table
		wantColumnsAdded   []string
		wantColumnsRemoved []string
	}{
		{
			name: "a live virtual table is not compared column by column",
			desiredFields: []schemamodel.Field{
				{StructName: "Doc", Name: "title", Type: "TEXT"},
				{StructName: "Doc", Name: "body", Type: "TEXT"},
			},
			currentTable: catalog.Table{Name: "docs", Type: "TABLE", Facets: observedVirtualTable("fts5", "title, body")},
		},
		{
			name:          "a declared virtual table is not compared column by column",
			desiredFacets: declaredVirtualTable("fts5", "title, body"),
			currentTable: catalog.Table{Name: "docs", Type: "TABLE", Columns: []catalog.Column{
				{Name: "title", DataType: "TEXT", IsNullable: "YES"}, {Name: "body", DataType: "TEXT", IsNullable: "YES"},
			}},
		},
		{
			name: "an ordinary table missing the same columns still is",
			desiredFields: []schemamodel.Field{
				{StructName: "Doc", Name: "title", Type: "TEXT"},
				{StructName: "Doc", Name: "body", Type: "TEXT"},
			},
			currentTable:     catalog.Table{Name: "docs", Type: "TABLE"},
			wantColumnsAdded: []string{"body", "title"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs", Facets: tt.desiredFacets}},
				Fields: tt.desiredFields,
			}
			database := &catalog.Database{Tables: []catalog.Table{tt.currentTable}}

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, "sqlite", must.Must(builtin.New())))

			c.Assert(diff.TablesAdded, qt.HasLen, 0)
			c.Assert(diff.TablesRemoved.Names(), qt.HasLen, 0)
			c.Assert(modifiedColumnAdditions(diff.TablesModified), qt.DeepEquals, tt.wantColumnsAdded)
			c.Assert(modifiedColumnRemovals(diff.TablesModified), qt.DeepEquals, tt.wantColumnsRemoved)
		})
	}
}

// modifiedColumnAdditions flattens every planned column addition into one
// sorted list, so a failing row prints the columns rather than the whole diff.
func modifiedColumnAdditions(modified []difftypes.TableDiff) []string {
	var columns []string
	for _, tableDiff := range modified {
		columns = append(columns, tableDiff.ColumnsAdded.Names()...)
	}
	sort.Strings(columns)
	return columns
}

// modifiedColumnRemovals is [modifiedColumnAdditions] for planned removals.
func modifiedColumnRemovals(modified []difftypes.TableDiff) []string {
	var columns []string
	for _, tableDiff := range modified {
		columns = append(columns, tableDiff.ColumnsRemoved.Names()...)
	}
	sort.Strings(columns)
	return columns
}

// TestCompareRemovesALiveVirtualTableOnlyWhenTheDesiredSourceDescribesThem
// pins the other half of the neutral contract: a live table a facet defines is
// removed only when the desired state's coverage describes the defining kind
// ([schemaext.PlansDefinedTableRemoval]). Go annotations, HCL and YAML have no
// CREATE VIRTUAL TABLE, so a live FTS5 index they never mention is outside the
// surface they manage, and the drop takes the index and everything in it
// (stokaro/ptah#1028). A SQLite SQL document describes virtual tables, so its
// silence asks for the removal.
//
// The ordinary row is the control: a table no facet defines is removed with no
// coverage at all, so the rows that keep the virtual table are not keeping it
// because removal stopped working.
func TestCompareRemovesALiveVirtualTableOnlyWhenTheDesiredSourceDescribesThem(t *testing.T) {
	tests := []struct {
		name        string
		coverage    schemaext.Coverage
		current     schemaext.Facets
		wantRemoved []string
	}{
		{
			name:        "a source that describes virtual tables asks for the removal",
			coverage:    must.Must(sqlitetable.VirtualCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
			current:     observedVirtualTable("fts5", "title, body"),
			wantRemoved: []string{"docs"},
		},
		{
			name:    "a source that makes no claim keeps the table",
			current: observedVirtualTable("fts5", "title, body"),
		},
		{
			name: "a source that cannot represent virtual tables keeps the table",
			coverage: must.Must(sqlitetable.VirtualCoverage(schemaext.Desired,
				schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the format has no virtual table"}, nil)),
			current: observedVirtualTable("fts5", "title, body"),
		},
		{
			name:        "an ordinary table is removed without any claim",
			wantRemoved: []string{"docs"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &schemamodel.Database{FeatureCoverage: tt.coverage}
			database := &catalog.Database{Tables: []catalog.Table{{Name: "docs", Type: "TABLE", Facets: tt.current}}}

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, database, "sqlite", must.Must(builtin.New())))

			c.Assert(diff.TablesRemoved.Names(), qt.DeepEquals, tt.wantRemoved)
		})
	}
}
