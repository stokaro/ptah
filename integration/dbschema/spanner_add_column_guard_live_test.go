//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// The ADD COLUMN IF NOT EXISTS the renderer writes for Spanner is accepted,
// adds the column once, and changes nothing when run again or when it names a
// column the table already has (stokaro/ptah#3637).
func TestSpannerLiveAddColumnIfNotExists(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_sp_guard_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE TABLE "`+table+`" (id bigint PRIMARY KEY, a text)`)
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`) }()

	add := func(column string) string {
		c.Helper()
		sql, err := renderer.RenderSQL(platform.Spanner, &ast.AlterTableNode{Name: table, Operations: []ast.AlterOperation{
			&ast.AddColumnOperation{Column: ast.NewColumn(column, "text"), IfNotExists: true},
		}})
		c.Assert(err, qt.IsNil)
		return sql
	}
	for _, statement := range []string{add("b"), add("b"), add("a")} {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	c.Assert(liveColumnNames(c, live, table), qt.DeepEquals, []string{"id", "a", "b"})
}
