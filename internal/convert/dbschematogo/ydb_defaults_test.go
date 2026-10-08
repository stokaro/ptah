package dbschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbmlrender"
)

func TestConvertDBSchemaToGoSchema_YDBEmptyDefaultsSurviveExport(t *testing.T) {
	c := qt.New(t)
	db := &catalog.Database{Tables: []catalog.Table{{Name: "items", Columns: []catalog.Column{
		{Name: "id", DataType: "Int64", IsPrimaryKey: true, IsNullable: "NO"},
		{Name: "text_value", DataType: "Utf8", IsNullable: "YES", ColumnDefault: new(`''u`)},
		{Name: "bytes_value", DataType: "String", IsNullable: "YES", ColumnDefault: new(`''`)},
		{Name: "no_default", DataType: "Utf8", IsNullable: "YES"},
	}}}}

	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	model, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), db, platform.YDB, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(model, platform.YDB, capability.YDB251())
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, "`text_value` Utf8 DEFAULT ''u")
	c.Assert(sql, qt.Contains, "`bytes_value` String DEFAULT ''")
	c.Assert(sql, qt.Not(qt.Contains), "`no_default` Utf8 DEFAULT")

	exported, err := dbmlrender.Render(c.Context(), model, dbmlrender.Options{Target: platform.YDB}, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(exported.DBML, qt.Contains, `"text_value" Utf8 [default: '']`)
	c.Assert(exported.DBML, qt.Contains, `"bytes_value" String [default: '']`)
	c.Assert(exported.DBML, qt.Not(qt.Contains), `"no_default" Utf8 [default:`)
}
