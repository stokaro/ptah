package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// heldSerialColumn is a read of t.id, a column that owns the sequence its default
// draws from when owned names one.
func heldSerialColumn(owned string) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "t", Type: "BASE TABLE",
		Columns: []catalog.Column{{
			Name: "id", DataType: "bigint", UDTName: "int8", ColumnType: "bigint", IsNullable: "NO", IsPrimaryKey: true,
			IsAutoIncrement: true, OwnedSequence: owned, ColumnDefault: new("nextval('t_id_seq'::regclass)"),
		}},
	}}}
}

// plainDeclaration declares t with its key column as field describes it.
func plainDeclaration(field schemamodel.Field) *schemamodel.Database {
	field.StructName, field.Name, field.Primary = "T", "id", true
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{field},
	}
}

// TestCompareSerialAgainstPlainType_ReportsTheDefault declares a plain type over a
// column the database holds as serial. The declared column has no default, so inserts
// that name no key stop working once the two agree. The comparison read them as
// synced. The sequence leaves with the default: with no default the reader reports
// it as a sequence of its own, and the next run would drop it.
func TestCompareSerialAgainstPlainType_ReportsTheDefault(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDialect(c.Context(), plainDeclaration(schemamodel.Field{Type: "BIGINT"}),
		heldSerialColumn("t_id_seq"), "postgres", builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes, qt.DeepEquals,
		map[string]string{"default_expr": "nextval('t_id_seq'::regclass) -> "})
	c.Assert(diff.SequencesRemoved.Names(), qt.DeepEquals, []string{"t_id_seq"})
}

// TestCompareSerialAgainstPlainType_DeclarationsThatImplyTheSequence are the
// declarations whose column is built with a sequence, and a declared default on it.
func TestCompareSerialAgainstPlainType_DeclarationsThatImplyTheSequence(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
	}{
		{name: "a serial type", field: schemamodel.Field{Type: "BIGSERIAL"}},
		{name: "a numbered alias", field: schemamodel.Field{Type: "SERIAL8"}},
		{name: "an auto-increment flag", field: schemamodel.Field{Type: "BIGINT", AutoInc: true}},
		{name: "an identity clause", field: schemamodel.Field{Type: "BIGINT", IdentityGeneration: "BY DEFAULT"}},
		{name: "a default that names the sequence", field: schemamodel.Field{Type: "BIGINT", DefaultExpr: "nextval('t_id_seq')"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(c.Context(), plainDeclaration(test.field),
				heldSerialColumn("t_id_seq"), "postgres", builtintest.Runtime())

			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
			c.Assert(diff.SequencesRemoved, qt.HasLen, 0)
		})
	}
}

// TestCompareSerialAgainstPlainType_ADefaultNoSequenceBacks reads an auto-increment
// column that owns no sequence, whose default is the engine's own and which only a
// reader of another engine reports.
func TestCompareSerialAgainstPlainType_ADefaultNoSequenceBacks(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDialect(c.Context(), plainDeclaration(schemamodel.Field{Type: "BIGINT"}),
		heldSerialColumn(""), "oracle", builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.SequencesRemoved, qt.HasLen, 0)
}

// TestCompareSerialAgainstPlainType_IdentityOverAPlainDefault declares an identity over
// a column that holds a default of no sequence. An identity column holds no default, so
// the live one stays a difference, and the server refuses an identity on a column that
// keeps one.
func TestCompareSerialAgainstPlainType_IdentityOverAPlainDefault(t *testing.T) {
	c := qt.New(t)
	held := heldSerialColumn("")
	held.Tables[0].Columns[0].IsAutoIncrement = false
	held.Tables[0].Columns[0].ColumnDefault = new("7")

	diff, err := schemadiff.CompareWithDialect(c.Context(), plainDeclaration(schemamodel.Field{Type: "bigint", IdentityGeneration: "BY_DEFAULT"}),
		held, "postgres", builtintest.Runtime())

	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["default_expr"], qt.Equals, "7 -> ")
}
