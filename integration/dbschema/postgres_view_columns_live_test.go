//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/renderer"
	"ptah.run/dbschema"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// viewColumnsDocument declares a view whose output columns are named by
// `column` blocks rather than by aliases in the body.
func viewColumnsDocument(schemaName string) []byte {
	return []byte(`
schema "` + schemaName + `" {}

table "t" {
  schema = schema.` + schemaName + `
  column "id" {
    type = int
    null = true
  }
  column "label" {
    type = text
    null = true
  }
}

view "v" {
  schema = schema.` + schemaName + `
  as     = "SELECT id, label FROM t"
  column "ident" {
  }
  column "name" {
  }
}
`)
}

// TestPostgresLiveViewColumnsConverge is the test this rewrite could not have
// been added without.
//
// PostgreSQL does not keep an alias list. `CREATE VIEW v (a, b) AS SELECT x, y`
// is stored as `SELECT x AS a, y AS b`, so a declaration that rendered the
// alias list would never match its own catalog row and the view would be
// planned for replacement on every run (stokaro/ptah#3172). Only a server shows
// that: both sides of an offline comparison come from the same declaration.
//
// Three steps: the server accepts the rewritten statement, the columns it holds
// are the ones the declaration named, and comparing the document against what
// the server now holds finds nothing to do.
func TestPostgresLiveViewColumnsConverge(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_viewcol_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()

	description, err := atlashcl.Parse(viewColumnsDocument(schemaName), "schema.hcl")
	c.Assert(err, qt.IsNil)

	statements, err := renderer.GetOrderedCreateStatements(description, platform.Postgres)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// The columns the view holds are the declared ones, which is the property
	// the blocks exist for.
	rows, err := conn.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns
		 WHERE table_schema = $1 AND table_name = 'v' ORDER BY ordinal_position`, schemaName)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var column string
		c.Assert(rows.Scan(&column), qt.IsNil)
		columns = append(columns, column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(columns, qt.DeepEquals, []string{"ident", "name"})

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	c.Assert(live.Views, qt.HasLen, 1)

	settled := schemadiff.CompareWithDialect(description, live, platform.Postgres)
	c.Assert(settled.ViewsAdded, qt.HasLen, 0)
	c.Assert(settled.ViewsModified, qt.HasLen, 0)
	c.Assert(settled.ViewsRemoved, qt.HasLen, 0)
}
