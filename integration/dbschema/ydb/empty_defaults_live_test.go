//go:build integration

package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/internal/convert/dbschematogo"
)

// A read and SQL export must preserve an empty default as a present value.
// Recreating the table from that export proves that both string types retain
// the defaults the server reported before the original table was dropped.
func TestYDBEmptyDefaults_ExportRecreatesTheStoredDefaults(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			const schema = "ptah_ydb_empty_defaults"
			schemas := []string{schema}
			ownDirectory(c, conn, schema)
			apply(c, conn, []string{"CREATE TABLE `" + schema + "/items` (" +
				"id Int64 NOT NULL, text_value Utf8 DEFAULT ''u, bytes_value String DEFAULT '', PRIMARY KEY (id))"})

			before := tableNamed(c, readScoped(c, conn, schemas), schema, "items")
			textDefault := defaultOf(columnNamed(c, before, "text_value"))
			bytesDefault := defaultOf(columnNamed(c, before, "bytes_value"))
			c.Assert(textDefault, qt.Equals, "''u")
			c.Assert(bytesDefault, qt.Equals, "''")
			// Export only the owned table, without the catalog's global grants.
			model := dbschematogo.ConvertDBSchemaToGoSchema(
				&catalog.Database{Tables: []catalog.Table{before}}, platform.YDB,
			)
			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(model, platform.YDB, conn.Info().Capabilities)
			c.Assert(err, qt.IsNil)
			c.Assert(planAgainst(c, conn, model, schemas), qt.HasLen, 0)
			dropTables(c, conn, schemas)
			apply(c, conn, statements)

			after := tableNamed(c, readScoped(c, conn, schemas), schema, "items")
			c.Assert(defaultOf(columnNamed(c, after, "text_value")), qt.Equals, textDefault)
			c.Assert(defaultOf(columnNamed(c, after, "bytes_value")), qt.Equals, bytesDefault)
			c.Assert(planAgainst(c, conn, model, schemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, model, schemas))
			c.Assert(planAgainst(c, conn, model, schemas), qt.HasLen, 0)
		})
	}
}
