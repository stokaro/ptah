package schemadiff_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// takeoverChildren is a read of children, whose foreign key f over a stands on the
// index the server built for it, f.
func takeoverChildren() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parents", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "children", Type: "BASE TABLE", Columns: []catalog.Column{
				{Name: "id", DataType: "bigint", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "a", DataType: "bigint", ColumnType: "bigint", IsNullable: "YES"},
				{Name: "b", DataType: "bigint", ColumnType: "bigint", IsNullable: "YES"},
			}},
		},
		Indexes: []catalog.Index{{Name: "f", TableName: "children", Columns: []string{"a"}}},
		Constraints: []catalog.Constraint{{
			Name: "f", TableName: "children", Type: "FOREIGN KEY", ColumnName: "a", ColumnNames: []string{"a"},
			ForeignTable: new("parents"), ForeignColumn: new("id"),
		}},
	}
}

// takeoverDeclaration declares parents and children with the foreign key f, a declared
// unique where unique holds, and the constraints and indexes given beside f.
func takeoverDeclaration(unique bool, constraints []schemamodel.Constraint, indexes ...schemamodel.Index) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Parents", Name: "parents"}, {StructName: "Children", Name: "children"}},
		Fields: []schemamodel.Field{
			{StructName: "Parents", Name: "id", Type: "bigint", Primary: true},
			{StructName: "Children", Name: "id", Type: "bigint", Primary: true},
			{StructName: "Children", Name: "a", Type: "bigint", Nullable: true, Unique: unique},
			{StructName: "Children", Name: "b", Type: "bigint", Nullable: true},
		},
		Constraints: append([]schemamodel.Constraint{{
			StructName: "Children", Name: "f", Type: "FOREIGN KEY", Table: "children", Columns: []string{"a"},
			ForeignTable: "parents", ForeignColumn: "id",
		}}, constraints...),
		Indexes: indexes,
	}
}

// takeovers lists what each addition of diff takes over from, as "<kind> <name>: <index>",
// in the order the diff lists the additions.
func takeovers(diff *difftypes.SchemaDiff) []string {
	var listed []string
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			for _, ref := range column.Replaces {
				listed = append(listed, fmt.Sprintf("column %s: %s", column.ColumnName, ref.Name))
			}
		}
	}
	for _, add := range diff.ConstraintsAdded {
		for _, ref := range add.Replaces {
			listed = append(listed, fmt.Sprintf("constraint %s: %s", add.Name, ref.Name))
		}
	}
	for _, change := range diff.IndexesAdded {
		for _, ref := range change.Replaces {
			listed = append(listed, fmt.Sprintf("index %s: %s", change.Index.Name, ref.Name))
		}
	}
	return listed
}

// TestCompare_AnAdditionTakesOverAForeignKeysBuiltIndex pins the record of the index the
// server built for a foreign key the plan keeps, where the plan adds a key or an index
// over the foreign key's column. The server drops the built index once the addition is
// in, so the index joins the removals, and every addition it would drop it for names it.
func TestCompare_AnAdditionTakesOverAForeignKeysBuiltIndex(t *testing.T) {
	unique := schemamodel.Constraint{StructName: "Children", Name: "u", Type: "UNIQUE", Table: "children", Columns: []string{"a"}}
	plain := schemamodel.Index{StructName: "Children", Name: "idx_a", Fields: []string{"a"}}
	wide := schemamodel.Index{StructName: "Children", Name: "w", Fields: []string{"a", "b"}}
	tests := []struct {
		name     string
		declared *schemamodel.Database
		want     []string
	}{
		{
			name:     "a UNIQUE constraint",
			declared: takeoverDeclaration(false, []schemamodel.Constraint{unique}),
			want:     []string{"constraint u: f"},
		},
		{
			name:     "the column's own UNIQUE",
			declared: takeoverDeclaration(true, nil),
			want:     []string{"column a: f"},
		},
		{
			name:     "an index",
			declared: takeoverDeclaration(false, nil, plain),
			want:     []string{"index idx_a: f"},
		},
		{
			name:     "a UNIQUE constraint and a wider index",
			declared: takeoverDeclaration(false, []schemamodel.Constraint{unique}, wide),
			want:     []string{"constraint u: f", "index w: f"},
		},
	}
	for _, engine := range ownershipEngines {
		for _, test := range tests {
			t.Run(engine.name+" "+test.name, func(t *testing.T) {
				c := qt.New(t)
				opts := config.DefaultCompareOptions()
				opts.Dialect = engine.dialect

				diff, err := schemadiff.CompareWithOptions(c.Context(), test.declared, takeoverChildren(), opts, must.Must(builtin.New()))

				c.Assert(err, qt.IsNil)
				c.Assert(takeovers(diff), qt.DeepEquals, test.want)
				c.Assert(removedIndexNames(diff), qt.DeepEquals, []string{"f"})
				c.Assert(diff.IndexesTakenOver(), qt.DeepEquals, map[difftypes.IndexRef]struct{}{{Name: "f", TableName: "children"}: {}})
			})
		}
	}
}

// TestCompare_AnAdditionTakesOverAForeignKeysBuiltIndex_FailurePath pins the comparisons
// that record no takeover: the addition does not begin with the foreign key's column, the
// declaration holds the built index, or the target's server builds no index for a key.
func TestCompare_AnAdditionTakesOverAForeignKeysBuiltIndex_FailurePath(t *testing.T) {
	other := schemamodel.Constraint{StructName: "Children", Name: "u", Type: "UNIQUE", Table: "children", Columns: []string{"b", "a"}}
	unique := schemamodel.Constraint{StructName: "Children", Name: "u", Type: "UNIQUE", Table: "children", Columns: []string{"a"}}
	built := schemamodel.Index{StructName: "Children", Name: "f", Fields: []string{"a"}}
	tests := []struct {
		name     string
		dialect  string
		declared *schemamodel.Database
	}{
		{
			name:     "a UNIQUE that begins with another column",
			dialect:  platform.MySQL,
			declared: takeoverDeclaration(false, []schemamodel.Constraint{other}),
		},
		{
			name:     "the declaration holds the built index",
			dialect:  platform.MySQL,
			declared: takeoverDeclaration(false, []schemamodel.Constraint{unique}, built),
		},
		{
			name:     "PostgreSQL builds no index for a key",
			dialect:  platform.Postgres,
			declared: takeoverDeclaration(false, []schemamodel.Constraint{unique}, built),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := config.DefaultCompareOptions()
			opts.Dialect = test.dialect

			diff, err := schemadiff.CompareWithOptions(c.Context(), test.declared, takeoverChildren(), opts, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(takeovers(diff), qt.HasLen, 0)
			c.Assert(diff.IndexesTakenOver(), qt.HasLen, 0)
		})
	}
}
