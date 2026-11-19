package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// sequenceDefaultTables declares things with a column n whose default is
// field's, and holds THINGS in schema APP with N defaulting to held.
func sequenceDefaultTables(field schemamodel.Field, held string) (*schemamodel.Database, catalog.Table) {
	desired := declaredTable("things")
	field.StructName, field.Name, field.Type, field.Nullable = "things", "n", "NUMBER", true
	desired.Fields = append(desired.Fields, field)
	table := heldTable("THINGS", "N")
	table.Columns[1].ColumnDefault = &held
	return desired, table
}

// changedDefaults names the columns whose default a comparison plans to
// change.
func changedDefaults(diff *difftypes.SchemaDiff) []string {
	names := make([]string, 0)
	for _, table := range diff.TablesModified {
		for _, column := range table.ColumnsModified {
			_, value := column.Changes["default"]
			_, expression := column.Changes["default_expr"]
			if value || expression {
				names = append(names, column.ColumnName)
			}
		}
	}
	return names
}

// TestCompareWithDatabaseInfo_SequenceDefault_HappyPath compares a declared
// Oracle default reading a sequence with the one Oracle stores for it, which
// qualifies and quotes an unqualified reference and keeps a qualified one as
// written. Each pair reads one sequence, so the comparison plans nothing.
// Compared as text, every apply plans the default again.
func TestCompareWithDatabaseInfo_SequenceDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name, declared, held string
	}{
		{name: "unqualified", declared: "seq.NEXTVAL", held: `"APP"."SEQ"."NEXTVAL"`},
		{name: "CURRVAL", declared: "seq.CURRVAL", held: `"APP"."SEQ"."CURRVAL"`},
		{name: "qualified", declared: "app.seq.nextval", held: "app.seq.nextval"},
		{name: "a quoted sequence", declared: `"Seq".NEXTVAL`, held: `"APP"."Seq"."NEXTVAL"`},
		{name: "an operator", declared: "seq.NEXTVAL + 1", held: `"APP"."SEQ"."NEXTVAL"+1`},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := sequenceDefaultTables(schemamodel.Field{DefaultExpr: row.declared}, row.held)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{StoredNames: true, Tables: []catalog.Table{held}},
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(changedDefaults(diff), qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_SequenceDefault_Changed compares defaults that
// read another sequence, another schema's sequence, or a string spelling a
// reference. Each pair differs, so the comparison plans the default.
func TestCompareWithDatabaseInfo_SequenceDefault_Changed(t *testing.T) {
	rows := []struct {
		name  string
		field schemamodel.Field
		held  string
	}{
		{name: "a quoted sequence", field: schemamodel.Field{DefaultExpr: `"Seq".NEXTVAL`}, held: `"APP"."SEQ"."NEXTVAL"`},
		{name: "another pseudocolumn", field: schemamodel.Field{DefaultExpr: "seq.CURRVAL"}, held: `"APP"."SEQ"."NEXTVAL"`},
		{name: "another schema", field: schemamodel.Field{DefaultExpr: "other.seq.NEXTVAL"}, held: `"APP"."SEQ"."NEXTVAL"`},
		{name: "a declared string", field: schemamodel.Field{Default: "seq.NEXTVAL", DefaultSet: true}, held: `"APP"."SEQ"."NEXTVAL"`},
		{name: "a held string", field: schemamodel.Field{DefaultExpr: "seq.NEXTVAL"}, held: "'seq.NEXTVAL'"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			desired, held := sequenceDefaultTables(row.field, row.held)

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, &catalog.Database{StoredNames: true, Tables: []catalog.Table{held}},
				oracleRead(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(changedDefaults(diff), qt.DeepEquals, []string{"n"})
		})
	}
}

// sqlServerSequenceDefault compares a declared SQL Server default with the
// one the catalog holds for column n of dbo.things.
func sqlServerSequenceDefault(c *qt.C, field schemamodel.Field, held string) *difftypes.SchemaDiff {
	c.Helper()
	field.StructName, field.Name, field.Type, field.Nullable = "things", "n", "BIGINT", true
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "things", Name: "things", Schema: "dbo"}},
		Fields: []schemamodel.Field{{StructName: "things", Name: "id", Type: "INT", Primary: true}, field},
	}
	table := catalog.Table{Name: "things", Schema: "dbo", Type: "BASE TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "int", UDTName: "int", IsNullable: "NO", IsPrimaryKey: true},
		{Name: "n", DataType: "bigint", UDTName: "bigint", IsNullable: "YES", ColumnDefault: &held},
	}}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, &catalog.Database{Tables: []catalog.Table{table}},
		catalog.ServerInfo{Dialect: "sqlserver", Schema: "dbo"}, nil, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return diff
}

// TestCompareWithDatabaseInfo_SQLServerNextValueDefault_HappyPath compares a
// declared NEXT VALUE FOR default with the one SQL Server 2022 stores for it,
// with the clause in upper case and each name bracketed. Each pair reads one
// sequence, so the comparison plans nothing. Compared as text, every apply
// plans the default again.
func TestCompareWithDatabaseInfo_SQLServerNextValueDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name, declared, held string
	}{
		{name: "in parentheses", declared: "(NEXT VALUE FOR ptah_s)", held: "(NEXT VALUE FOR [ptah_s])"},
		{name: "without parentheses", declared: "NEXT VALUE FOR PTAH_S", held: "(NEXT VALUE FOR [PTAH_S])"},
		{name: "qualified, in lower case", declared: "(next value for dbo.ptah_s)", held: "(NEXT VALUE FOR [dbo].[ptah_s])"},
		{name: "bracketed", declared: "(NEXT VALUE FOR [dbo].[ptah_s])", held: "(NEXT VALUE FOR [dbo].[ptah_s])"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := sqlServerSequenceDefault(c, schemamodel.Field{DefaultExpr: row.declared}, row.held)

			c.Assert(changedDefaults(diff), qt.HasLen, 0)
		})
	}
}

// TestCompareWithDatabaseInfo_SQLServerNextValueDefault_Changed compares
// defaults that read another sequence, the same name in another schema, or a
// string spelling a reference. Each pair differs, so the comparison plans the
// default.
func TestCompareWithDatabaseInfo_SQLServerNextValueDefault_Changed(t *testing.T) {
	rows := []struct {
		name  string
		field schemamodel.Field
		held  string
	}{
		{name: "another sequence", field: schemamodel.Field{DefaultExpr: "(NEXT VALUE FOR other_s)"}, held: "(NEXT VALUE FOR [ptah_s])"},
		{name: "qualified", field: schemamodel.Field{DefaultExpr: "(NEXT VALUE FOR dbo.ptah_s)"}, held: "(NEXT VALUE FOR [ptah_s])"},
		{name: "a declared string", field: schemamodel.Field{Default: "NEXT VALUE FOR ptah_s", DefaultSet: true}, held: "(NEXT VALUE FOR [ptah_s])"},
		{name: "a held string", field: schemamodel.Field{DefaultExpr: "(NEXT VALUE FOR ptah_s)"}, held: "('NEXT VALUE FOR ptah_s')"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			diff := sqlServerSequenceDefault(c, row.field, row.held)

			c.Assert(changedDefaults(diff), qt.DeepEquals, []string{"n"})
		})
	}
}
