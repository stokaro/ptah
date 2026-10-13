package compare_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// deleteListDeclaration is a child table whose composite foreign key sets the
// listed columns to NULL when the parent row goes.
func deleteListDeclaration(listed ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parent", PrimaryKey: []string{"tenant", "id"}},
			{StructName: "Child", Name: "child"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "tenant", Type: "INTEGER"},
			{StructName: "Parent", Name: "id", Type: "INTEGER"},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "tenant", Type: "INTEGER", Nullable: true},
			{StructName: "Child", Name: "pid", Type: "INTEGER", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Table: "child", Name: "fk_child_parent", Type: "FOREIGN KEY",
			Columns: []string{"tenant", "pid"}, ForeignTable: "parent",
			ForeignColumns: []string{"tenant", "id"},
			OnDelete:       "SET NULL", Facets: must.Must(pgtable.WithDeleteColumns(schemaext.Facets{}, listed)),
		}},
	}
}

// deleteListCatalog is the same key as the server reports it.
func deleteListCatalog(listed ...string) *catalog.Database {
	parent := "parent"
	setNull := "SET NULL"
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parent", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "tenant", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "child", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "tenant", DataType: "integer", IsNullable: "YES"},
				{Name: "pid", DataType: "integer", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			TableName: "child", Name: "fk_child_parent", Type: "FOREIGN KEY",
			ColumnNames: []string{"tenant", "pid"}, ForeignTable: &parent,
			ForeignColumns: []string{"tenant", "id"},
			DeleteRule:     &setNull, Facets: must.Must(pgtable.WithObservedDeleteColumns(schemaext.Facets{}, listed)),
		}},
	}
}

func deleteListDiff(desired *schemamodel.Database, current *catalog.Database) *difftypes.SchemaDiff {
	desired.FeatureCoverage = must.Must(pgtable.DeleteColumnsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	current.FeatureCoverage = must.Must(pgtable.DeleteColumnsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	return must.Must(schemadiff.CompareWithDialect(context.Background(), desired, current, "postgres", builtintest.Runtime()))
}

// The list is a set of the key's columns, and no list means all of them, so
// these pairs describe one key. pg_get_constraintdef prints a list naming every
// column back rather than dropping it, so without the fold a key declared
// either way would plan a drop and an add on every run.
func TestConstraints_TheSameDeleteColumnSetIsNotPlanned(t *testing.T) {
	tests := []struct {
		name     string
		declared []string
		live     []string
	}{
		{name: "one column on both sides", declared: []string{"pid"}, live: []string{"pid"}},
		{name: "no list on either side", declared: nil, live: nil},
		{name: "every column listed against no list", declared: nil, live: []string{"tenant", "pid"}},
		{name: "no list against every column listed", declared: []string{"pid", "tenant"}, live: nil},
		{name: "the same columns in another order", declared: []string{"pid", "tenant"}, live: []string{"tenant", "pid"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := deleteListDiff(deleteListDeclaration(tt.declared...), deleteListCatalog(tt.live...))

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// The control: a list that changes which columns the action clears is a
// difference owned by PostgreSQL, without a common constraint replacement.
func TestConstraints_AChangedDeleteColumnSetIsPlanned(t *testing.T) {
	tests := []struct {
		name     string
		declared []string
		live     []string
	}{
		{name: "the declaration narrows the action", declared: []string{"pid"}, live: nil},
		{name: "the declaration widens the action", declared: nil, live: []string{"pid"}},
		{name: "another column", declared: []string{"pid"}, live: []string{"tenant"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := deleteListDiff(deleteListDeclaration(tt.declared...), deleteListCatalog(tt.live...))

			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].FeatureChanges, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].FeatureChanges[0].Value.Kind(), qt.Equals, pgtable.DeleteColumnsChangeKind)
		})
	}
}

// TestConstraints_CommonAndOwnedDeleteChangesReplaceOnce drives comparison
// through SQL planning, so both halves use the host's actual effect metadata.
func TestConstraints_CommonAndOwnedDeleteChangesReplaceOnce(t *testing.T) {
	c := qt.New(t)
	desired := deleteListDeclaration("tenant")
	desired.Constraints[0].OnDelete = "SET DEFAULT"
	desired.Constraints[0].OnUpdate = "CASCADE"
	desired.Constraints[0].Comment = "Parent key"
	diff := deleteListDiff(desired, deleteListCatalog("pid"))
	statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), builtintest.Runtime(), diff, "postgres")
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(strings.Count(sql, "DROP CONSTRAINT"), qt.Equals, 1)
	c.Assert(strings.Count(sql, "ADD CONSTRAINT"), qt.Equals, 1)
	c.Assert(sql, qt.Contains, `ON DELETE SET DEFAULT ("tenant") ON UPDATE CASCADE`)
	c.Assert(sql, qt.Contains, "IS 'Parent key'")
}
