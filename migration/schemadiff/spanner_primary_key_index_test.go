package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// TestCompare_SpannerPrimaryKeyIndexesShareOneName pins stokaro/ptah#4287 off
// line. Spanner reports every table's primary key as an index named
// PRIMARY_KEY, the shape the live read returns, so a database holding two
// tables holds two indexes of that one name. Each is its own table's key, and
// the comparison finds both tables synced rather than refusing the read.
func TestCompare_SpannerPrimaryKeyIndexesShareOneName(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "A", Name: "a"}, {StructName: "B", Name: "b"}},
		Fields: []schemamodel.Field{
			{StructName: "A", Name: "id", Type: "bigint", Primary: true},
			{StructName: "B", Name: "id", Type: "bigint", Primary: true},
		},
	}
	current := &catalog.Database{}
	for _, table := range []string{"a", "b"} {
		current.Tables = append(current.Tables, catalog.Table{Name: table, Type: "BASE TABLE", Columns: []catalog.Column{{
			Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1,
		}}})
		current.Indexes = append(current.Indexes, catalog.Index{
			Name: "PRIMARY_KEY", TableName: table, IsPrimary: true, IsUnique: true, Columns: []string{"id"},
			Parts: []catalog.IndexPart{{Name: "id"}}, Definition: "CREATE UNIQUE INDEX PRIMARY_KEY ON " + table + " (id)",
		})
		current.Constraints = append(current.Constraints, catalog.Constraint{
			Name: "PK_" + table, TableName: table, Type: "PRIMARY KEY", ColumnName: "id", ColumnNames: []string{"id"},
		})
	}

	diff, err := schemadiff.CompareWithDialect(t.Context(), desired, current, platform.Spanner, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
}
