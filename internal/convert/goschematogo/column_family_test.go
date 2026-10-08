package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_ColumnFamilies writes a YDB table's column families as the
// annotations the parser reads them back from, so a schema read from a
// database and written as Go keeps each column in its family. Each family
// keeps the settings the read found, `off` included.
func TestRender_ColumnFamilies(t *testing.T) {
	c := qt.New(t)
	families := []ast.YDBColumnFamilySpec{
		{Name: "cold", Data: "hdd", Compression: "lz4", CacheMode: "in_memory", Columns: []string{"blob", "body"}},
		{Name: "default", Compression: "lz4"},
		{Name: "empty", Compression: "off"},
	}
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"}, YDBColumnFamilies: families}},
		Fields: []schemamodel.Field{
			{StructName: "Item", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Item", FieldName: "Body", Name: "body", Type: "TEXT", Nullable: true},
			{StructName: "Item", FieldName: "Blob", Name: "blob", Type: "BYTEA", Nullable: true},
		},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	reparsed, err := goschema.ParseSource("schema.go", string(files[0].Data))

	c.Assert(err, qt.IsNil)
	c.Assert(string(files[0].Data), qt.Contains,
		`//ptah:schema:columnfamily name="cold" data="hdd" compression="lz4" cache_mode="in_memory" fields="blob,body"`)
	c.Assert(reparsed.Tables, qt.HasLen, 1)
	c.Assert(reparsed.Tables[0].YDBColumnFamilies, qt.DeepEquals, families)
}

// TestRender_ColumnFamilies_PlainDefaultFamily leaves out the default family
// a read finds on a table nobody gave families, which holds what YDB gives a
// family stating nothing: a declaration that leaves it out keeps what the
// table holds, and an annotation on every table would say nothing.
func TestRender_ColumnFamilies_PlainDefaultFamily(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Item", Name: "items", PrimaryKey: []string{"id"},
			YDBColumnFamilies: []ast.YDBColumnFamilySpec{{Name: "default", Compression: "off", CacheMode: "regular"}}}},
		Fields: []schemamodel.Field{{StructName: "Item", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}},
	}

	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	c.Assert(string(files[0].Data), qt.Not(qt.Contains), "ptah:schema:columnfamily")
}
