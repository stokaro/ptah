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
	"ptah.run/migration/schemadiff/difftypes"
)

// These tests apply a SQL schema file that names its tables and columns in
// mixed case, and compare the file with what the server holds
// (stokaro/ptah#3592). The server folds an unquoted name, so the file has to be
// read the same way: kept as written, the render created "Docs" and enabled
// row-level security on "docs", which PostgreSQL refused, and a database the
// same file created directly compared as a different table, planning DROP
// TABLE.

// tableCaseRows are the engines and spellings the tests apply. quote wraps
// every name in the file the same way; a quoted name is the control, because
// every engine keeps its case. PostgreSQL with quoted names is left out: a
// quoted mixed-case index is dropped and created again on every PostgreSQL
// comparison, which is stokaro/ptah#3615 and not a matter of folding.
var tableCaseRows = []struct {
	name   string
	engine dbtarget.Engine
	quote  string
}{
	{name: "PostgreSQL/unquoted mixed case", engine: dbtarget.PostgreSQL, quote: ""},
	{name: "CockroachDB/unquoted mixed case", engine: dbtarget.CockroachDB, quote: ""},
	{name: "CockroachDB/quoted mixed case", engine: dbtarget.CockroachDB, quote: `"`},
	{name: "YugabyteDB/unquoted mixed case", engine: dbtarget.YugabyteDB, quote: ""},
	{name: "YugabyteDB/quoted mixed case", engine: dbtarget.YugabyteDB, quote: `"`},
}

// tableCaseFixture is one throwaway schema on one engine, and the schema file
// that declares objects in it.
type tableCaseFixture struct {
	conn    *dbschema.DatabaseConnection
	dialect string
	schema  string
	body    string
}

func newTableCaseFixture(c *qt.C, engine dbtarget.Engine, quote string) tableCaseFixture {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	f := tableCaseFixture{
		conn:    conn,
		dialect: conn.Info().Dialect,
		schema:  fmt.Sprintf("ptah_table_case_%d", time.Now().UnixNano()),
	}
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+pgx.Identifier{f.schema}.Sanitize()+" CASCADE")
		c.Check(dropErr, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	_, err = conn.ExecContext(ctx, "CREATE SCHEMA "+pgx.Identifier{f.schema}.Sanitize())
	c.Assert(err, qt.IsNil)
	f.body = strings.NewReplacer("SCHEMA", f.schema, "Q", quote).Replace(`CREATE TABLE SCHEMA.QParentQ (QIdQ bigint PRIMARY KEY, QTitleQ text NOT NULL);
CREATE TABLE SCHEMA.QChildQ (QIdQ bigint PRIMARY KEY, QParentIdQ bigint NOT NULL CONSTRAINT QChild_Parent_FkQ REFERENCES SCHEMA.QParentQ (QIdQ));
CREATE INDEX QChild_ParentQ ON SCHEMA.QChildQ (QParentIdQ);
ALTER TABLE SCHEMA.QParentQ ENABLE ROW LEVEL SECURITY;
CREATE POLICY QParent_ReadQ ON SCHEMA.QParentQ USING (true);
`)
	return f
}

func (f tableCaseFixture) load(c *qt.C) *schemamodel.Database {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(f.body), 0o600), qt.IsNil)
	desired, err := schemafile.LoadAll([]string{path}, schemafile.Options{Dialect: f.dialect})
	c.Assert(err, qt.IsNil)
	return desired
}

// execute runs statements one at a time, the way a client applies a file.
func (f tableCaseFixture) execute(c *qt.C, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

func (f tableCaseFixture) compare(c *qt.C, desired *schemamodel.Database) *difftypes.SchemaDiff {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	return schemadiff.CompareWithDialect(desired, live, f.dialect)
}

// assertNothingPlannedForTheFile checks the parts of a comparison the file
// declares. The whole diff also carries what the engine holds beyond the
// throwaway schema, such as an extension YugabyteDB installs, which is not what
// these tests are about.
func assertNothingPlannedForTheFile(c *qt.C, diff *difftypes.SchemaDiff) {
	c.Helper()
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesModified))
	c.Assert(diff.IndexesAdded, qt.HasLen, 0)
	c.Assert(diff.IndexesRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.IndexesRemoved))
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.ConstraintsAdded))
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.ConstraintsRemoved))
	c.Assert(diff.RLSEnabledTablesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSEnabledTablesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesAdded, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesRemoved, qt.HasLen, 0)
	c.Assert(diff.RLSPoliciesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.RLSPoliciesModified))
}

// fileStatements splits the fixture file into the statements a client sends.
func (f tableCaseFixture) fileStatements() []string {
	var statements []string
	for statement := range strings.SplitSeq(f.body, ";\n") {
		if strings.TrimSpace(statement) != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}

// TestTableCase_LiveRenderedFileApplies renders the file, applies the render
// and compares the file with what the server then holds. Nothing is left to
// plan.
func TestTableCase_LiveRenderedFileApplies(t *testing.T) {
	for _, row := range tableCaseRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTableCaseFixture(c, row.engine, row.quote)
			desired := f.load(c)
			statements, err := renderer.GetOrderedCreateStatements(desired, f.dialect)
			c.Assert(err, qt.IsNil)
			f.execute(c, statements)

			diff := f.compare(c, desired)

			assertNothingPlannedForTheFile(c, diff)
		})
	}
}

// TestTableCase_LiveFileTheServerAppliedComparesEqual sends the file to the
// server as written, the way psql or another migration tool applies it, and
// compares the same file with the result. The server's fold and Ptah's read
// have to agree, or the comparison replaces a table the file already
// describes.
func TestTableCase_LiveFileTheServerAppliedComparesEqual(t *testing.T) {
	for _, row := range tableCaseRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			f := newTableCaseFixture(c, row.engine, row.quote)
			f.execute(c, f.fileStatements())

			diff := f.compare(c, f.load(c))

			assertNothingPlannedForTheFile(c, diff)
		})
	}
}
