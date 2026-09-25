//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
	"ptah.run/migration/schemadiff"
)

// These tests apply a SQL schema file declaring two tables whose names derive
// one struct name: "Docs" and docs, which PostgreSQL keeps as two relations
// (stokaro/ptah#3642). Sharing the struct name, the pair is one table to the
// renderer: each CREATE TABLE carries the other table's columns, and a
// comparison against the database the file creates plans an ADD COLUMN on each
// and drops the primary key of one.

// tableIdentityColumns is what the fixture file creates, measured by sending
// the file to PostgreSQL 18 as written.
var tableIdentityColumns = map[string][]string{
	"Docs": {"id", "a"},
	"docs": {"id", "b", "x"},
}

// tableIdentityFixture is one throwaway schema and the file that declares the
// two tables in it.
type tableIdentityFixture struct {
	conn    *dbschema.DatabaseConnection
	dialect string
	schema  string
	body    string
}

func newTableIdentityFixture(c *qt.C) tableIdentityFixture {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	f := tableIdentityFixture{
		conn:    conn,
		dialect: conn.Info().Dialect,
		schema:  fmt.Sprintf("ptah_table_identity_%d", time.Now().UnixNano()),
	}
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{f.schema}.Sanitize()+" CASCADE")
		c.Check(dropErr, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	_, err = conn.ExecContext(ctx, "CREATE SCHEMA "+pgx.Identifier{f.schema}.Sanitize())
	c.Assert(err, qt.IsNil)
	f.body = strings.ReplaceAll(`CREATE TABLE SCHEMA."Docs" (id bigint PRIMARY KEY, a text);
CREATE TABLE SCHEMA.docs (id bigint PRIMARY KEY, b text);
ALTER TABLE SCHEMA.docs ADD COLUMN x int;
CREATE INDEX docs_b_idx ON SCHEMA.docs (b);
`, "SCHEMA", f.schema)
	return f
}

func (f tableIdentityFixture) load(c *qt.C) *schemamodel.Database {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(f.body), 0o600), qt.IsNil)
	desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: f.dialect})
	c.Assert(err, qt.IsNil)
	return desired
}

// execute runs statements one at a time, the way a client applies a file.
func (f tableIdentityFixture) execute(c *qt.C, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// fileStatements splits the fixture file into the statements a client sends.
func (f tableIdentityFixture) fileStatements() []string {
	var statements []string
	for statement := range strings.SplitSeq(f.body, ";\n") {
		if strings.TrimSpace(statement) != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

// columns reads each table of the fixture schema and its columns back from
// the catalog, in the order the server holds them.
func (f tableIdentityFixture) columns(c *qt.C) map[string][]string {
	c.Helper()
	rows, err := f.conn.QueryContext(c.Context(), `
		SELECT cl.relname, a.attname
		FROM pg_class cl
		JOIN pg_namespace n ON n.oid = cl.relnamespace
		JOIN pg_attribute a ON a.attrelid = cl.oid
		WHERE n.nspname = $1 AND cl.relkind = 'r' AND a.attnum > 0 AND NOT a.attisdropped
		ORDER BY cl.relname, a.attnum`, f.schema)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	columns := make(map[string][]string)
	for rows.Next() {
		var table, column string
		c.Assert(rows.Scan(&table, &column), qt.IsNil)
		columns[table] = append(columns[table], column)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return columns
}

// assertNothingPlannedForTheTables compares the file with what the server holds
// and checks the parts of the comparison the two tables make up.
func (f tableIdentityFixture) assertNothingPlannedForTheTables(c *qt.C) {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	diff := schemadiff.CompareWithDialect(f.load(c), live, f.dialect)
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesModified))
	c.Assert(diff.IndexesAdded, qt.HasLen, 0)
	c.Assert(diff.IndexesRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.IndexesRemoved))
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.ConstraintsAdded))
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.ConstraintsRemoved))
}

// TestTableIdentity_LiveRenderedFileCreatesEachTable renders the file, applies
// the render, and reads each table back: every column reaches the table the
// file declared it on, and nothing is left to plan.
func TestTableIdentity_LiveRenderedFileCreatesEachTable(t *testing.T) {
	c := qt.New(t)
	f := newTableIdentityFixture(c)
	statements, err := renderer.GetOrderedCreateStatements(f.load(c), f.dialect)
	c.Assert(err, qt.IsNil)

	f.execute(c, statements)

	c.Assert(f.columns(c), qt.DeepEquals, tableIdentityColumns)
	f.assertNothingPlannedForTheTables(c)
}

// TestTableIdentity_LiveFileTheServerAppliedComparesEqual sends the file to the
// server as written, the way psql applies it, and compares the same file with
// the result. The read-back is the control: it is the server's own answer to
// which columns belong to which table.
func TestTableIdentity_LiveFileTheServerAppliedComparesEqual(t *testing.T) {
	c := qt.New(t)
	f := newTableIdentityFixture(c)

	f.execute(c, f.fileStatements())

	c.Assert(f.columns(c), qt.DeepEquals, tableIdentityColumns)
	f.assertNothingPlannedForTheTables(c)
}
