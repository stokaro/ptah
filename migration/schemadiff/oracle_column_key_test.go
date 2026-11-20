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

// oracleKeyColumn is table T as Oracle's reader reports it for a column whose
// key Oracle named: the key is a flag on the column and no constraint row,
// because the reader drops a key it named itself. fromKey states that the
// column's NOT NULL is the key's alone.
func oracleKeyColumn(fromKey bool) *catalog.Database {
	return &catalog.Database{
		StoredNames: true,
		Tables: []catalog.Table{{Name: "T", Columns: []catalog.Column{
			{Name: "ID", DataType: "NUMBER(10)", IsNullable: "NO", IsPrimaryKey: true, NotNullFromKey: fromKey},
		}}},
	}
}

// oracleKeyDeclared declares column id, with or without the primary key.
func oracleKeyDeclared(key bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "NUMBER(10)", Primary: key}},
	}
}

// TestCompare_OracleColumnKey_HappyPath compares a column that loses the key
// its flag carries. The loss is a change of the column, and the diff says
// whether the column's NOT NULL came from the key alone, which the plan needs
// to keep NOT NULL or to leave the column nullable.
func TestCompare_OracleColumnKey_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		fromKey bool
	}{
		{name: "a NOT NULL that only the key holds", fromKey: true},
		{name: "a NOT NULL the column states itself"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := must.Must(schemadiff.CompareWithDialect(
				t.Context(), oracleKeyDeclared(false), oracleKeyColumn(test.fromKey), platform.Oracle, must.Must(builtin.New())))

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			columns := diff.TablesModified[0].ColumnsModified
			c.Assert(columns, qt.HasLen, 1)
			c.Assert(columns[0].Changes["primary_key"], qt.Equals, "true -> false")
			c.Assert(columns[0].CurrentNotNullFromKey, qt.Equals, test.fromKey)
		})
	}
}

// TestCompare_OracleColumnKey_FailurePath is the control that a column which
// keeps its key is not a change, whatever its NOT NULL rests on, and that a
// column which gains one is planned as the table's key, as the other engines
// plan it.
func TestCompare_OracleColumnKey_FailurePath(t *testing.T) {
	c := qt.New(t)

	kept := must.Must(schemadiff.CompareWithDialect(
		t.Context(), oracleKeyDeclared(true), oracleKeyColumn(true), platform.Oracle, must.Must(builtin.New())))
	live := oracleKeyColumn(false)
	live.Tables[0].Columns[0].IsPrimaryKey = false
	gained := must.Must(schemadiff.CompareWithDialect(
		t.Context(), oracleKeyDeclared(true), live, platform.Oracle, must.Must(builtin.New())))

	c.Assert(kept.TablesModified, qt.HasLen, 0)
	c.Assert(kept.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(gained.ConstraintsAdded, qt.HasLen, 1)
	c.Assert(gained.ConstraintsAdded[0].Type, qt.Equals, "PRIMARY KEY")
	c.Assert(gained.ConstraintsAdded[0].Columns, qt.DeepEquals, []string{"id"})
}
