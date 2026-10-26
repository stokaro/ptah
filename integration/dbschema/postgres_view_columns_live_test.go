//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/parser"
	"ptah.run/migration/schemadiff"
)

// viewColumnsDocument declares a view whose output columns are named by
// `column` blocks rather than by aliases in the body.
//
// The body names its table with the schema. A view body is the author's SQL and
// Ptah renders it verbatim, so an unqualified name is resolved by the server
// through search_path, which does not hold a schema this test just created.
// That holds for a view with no column blocks too, and is not what this test is
// about.
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
  as     = "SELECT id AS old_id, label AS old_label FROM ` + schemaName + `.t"
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
// PostgreSQL gives the column list priority over aliases already in SELECT.
// The rendered body must replace those aliases so the server accepts it and
// the catalog read converges on the declared names (stokaro/ptah#4933).
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

	description, err := atlashcl.ParseWithOptions(t.Context(), viewColumnsDocument(schemaName), "schema.hcl", atlashcl.Options{HCL: must.Must(builtin.New()).HCL()})
	c.Assert(err, qt.IsNil)

	statements, err := builtin.GetOrderedCreateStatements(description, platform.Postgres)
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

	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), description, live, platform.Postgres, must.Must(builtin.New())))
	c.Assert(settled.ViewsAdded, qt.HasLen, 0)
	c.Assert(settled.ViewsModified, qt.HasLen, 0)
	c.Assert(settled.ViewsRemoved, qt.HasLen, 0)
}

// TestPostgresLiveSQLViewColumnListOverridesSelectAliases proves that the SQL
// reader's rewritten view executes and returns values under the names declared
// by the column list, even when the SELECT already names both items.
func TestPostgresLiveSQLViewColumnListOverridesSelectAliases(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_viewalias_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(ctx, `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
	}()
	_, err = conn.ExecContext(ctx, `CREATE TABLE "`+schemaName+`".t (id integer PRIMARY KEY, name text)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `INSERT INTO "`+schemaName+`".t (id, name) VALUES (7, 'Ada')`)
	c.Assert(err, qt.IsNil)

	statements, err := parser.NewParser(`CREATE VIEW v (label, key) AS SELECT name AS shown, id AS old_id FROM "` + schemaName + `".t;`).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	view, ok := statements.Statements[0].(*ast.CreateViewNode)
	c.Assert(ok, qt.IsTrue)
	_, err = conn.ExecContext(ctx, `CREATE VIEW "`+schemaName+`".v AS `+view.Body)
	c.Assert(err, qt.IsNil)

	var label string
	var key int
	err = conn.QueryRowContext(ctx, `SELECT label, key FROM "`+schemaName+`".v`).Scan(&label, &key)
	c.Assert(err, qt.IsNil)
	c.Assert(label, qt.Equals, "Ada")
	c.Assert(key, qt.Equals, 7)
}
