package atlasschema_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// TestInspect_EmptyDatabaseReportsItsSchema pins the dialect half of
// stokaro/ptah#1264 that no PostgreSQL fixture can reach: a reader that
// describes no schemas of its own.
//
// SQLite's is one of those, so before the fix an empty SQLite database rendered
// `{}` — the connection was open on `main` and the document said the database
// had no schema at all. The pinned Atlas community binary v1.3.0 renders
// `{"schemas":[{"name":"main"}]}`, measured 2026-08-07.
//
// The same shape holds on MySQL 9.7, where the row also carries the character
// set and collation the binary prints; that cell has no fixture here because
// this package's tests do not reach a live MySQL server.
func TestInspect_EmptyDatabaseReportsItsSchema(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c.TB, filepath.Join(t.TempDir(), "inspect-empty.db"))
	defer dbschema.CloseAndWarn(conn)

	rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Format: "json",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Equals, `{"schemas":[{"name":"main"}]}`)
}

// TestInspect_EmptyDatabaseRendersItsSchemaBlock is the same state in the HCL
// rendering, where the binary prints `schema "main" {}`.
//
// It also pins that describing `main` as a schema does not put SQLite's
// "schemas are not supported" refusal in the output: `main` is the namespace
// the connection is already in, so there is nothing to refuse.
func TestInspect_EmptyDatabaseRendersItsSchemaBlock(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLite(c.TB, filepath.Join(t.TempDir(), "inspect-empty-hcl.db"))
	defer dbschema.CloseAndWarn(conn)

	rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Format: "hcl",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `schema "main" {`)
	c.Assert(rendered, qt.Not(qt.Contains), "not supported")
}

// TestInspect_SQLOutputHasNoStatementForTheDefaultNamespace pins the SQL
// rendering of the same schema row: the binary emits no statement for `main`,
// and an empty rendering must not reach the script as a bare `;`.
func TestInspect_SQLOutputHasNoStatementForTheDefaultNamespace(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "inspect-sql.db")
	conn := connectSQLite(c.TB, dbPath)
	defer dbschema.CloseAndWarn(conn)
	createInspectSchema(c.TB, conn)

	rendered, err := atlasschema.Inspect(context.Background(), conn, atlasschema.InspectOptions{
		Format: "sql",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `CREATE TABLE "users"`)
	c.Assert(rendered, qt.Not(qt.Contains), "not supported")
	c.Assert(rendered, qt.Not(qt.Matches), "(?s)^;.*")
}
