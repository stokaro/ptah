//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// These tests describe a live schema holding "Docs" and docs, two relations
// PostgreSQL keeps apart, whose names derive one struct name
// (stokaro/ptah#3647). Sharing it, the pair is one table to every consumer of
// the description: `schema inspect` writes each CREATE TABLE with both tables'
// columns, two primary keys included, which PostgreSQL refuses, and
// `introspect` writes both tables to one file, the second replacing the first,
// so the models compare against the database they came from as a DROP TABLE.

// liveTableIdentityColumns is what the fixture creates, read from pg_attribute.
var liveTableIdentityColumns = map[string][]string{
	"Docs": {"id", "a"},
	"docs": {"id", "b"},
}

// liveTableIdentityFixture is one throwaway schema holding the two tables.
type liveTableIdentityFixture struct {
	conn    *dbschema.DatabaseConnection
	dialect string
	schema  string
}

func newLiveTableIdentityFixture(c *qt.C) liveTableIdentityFixture {
	c.Helper()
	ctx := c.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	f := liveTableIdentityFixture{
		conn:    conn,
		dialect: conn.Info().Dialect,
		schema:  fmt.Sprintf("ptah_live_identity_%d", time.Now().UnixNano()),
	}
	c.Cleanup(func() {
		_, dropErr := conn.ExecContext(context.Background(), "DROP SCHEMA IF EXISTS "+f.quotedSchema()+" CASCADE")
		c.Check(dropErr, qt.IsNil)
		c.Check(conn.Close(), qt.IsNil)
	})
	f.execute(c, []string{
		"CREATE SCHEMA " + f.quotedSchema(),
		"CREATE TABLE " + f.quotedSchema() + `."Docs" (id bigint PRIMARY KEY, a text)`,
		"CREATE TABLE " + f.quotedSchema() + ".docs (id bigint PRIMARY KEY, b text)",
		"CREATE INDEX docs_b_idx ON " + f.quotedSchema() + ".docs (b)",
	})
	return f
}

func (f liveTableIdentityFixture) quotedSchema() string {
	return pgx.Identifier{f.schema}.Sanitize()
}

// execute runs statements one at a time, the way a client applies a file.
func (f liveTableIdentityFixture) execute(c *qt.C, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := f.conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// read reads the fixture schema from the catalog.
func (f liveTableIdentityFixture) read(c *qt.C) *catalog.Database {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), f.conn, []string{f.schema})
	c.Assert(err, qt.IsNil)
	return live
}

// columns reads each table of the fixture schema and its columns back from
// pg_attribute, in the order the server holds them.
func (f liveTableIdentityFixture) columns(c *qt.C) map[string][]string {
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

// indexTable names the table the index is on.
func (f liveTableIdentityFixture) indexTable(c *qt.C, index string) string {
	c.Helper()
	var table string
	err := f.conn.QueryRowContext(c.Context(), `
		SELECT tablename FROM pg_indexes WHERE schemaname = $1 AND indexname = $2`, f.schema, index).Scan(&table)
	c.Assert(err, qt.IsNil)
	return table
}

// assertNothingPlanned compares desired with what the server holds and checks
// the parts of the comparison the two tables make up.
func (f liveTableIdentityFixture) assertNothingPlanned(c *qt.C, desired *schemamodel.Database) {
	c.Helper()
	diff := schemadiff.CompareWithDialect(desired, f.read(c), f.dialect)
	c.Assert(diff.TablesAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesAdded))
	c.Assert(diff.TablesRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesRemoved))
	c.Assert(diff.TablesModified, qt.HasLen, 0, qt.Commentf("%+v", diff.TablesModified))
	c.Assert(diff.IndexesAdded, qt.HasLen, 0)
	c.Assert(diff.IndexesRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.IndexesRemoved))
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0, qt.Commentf("%+v", diff.ConstraintsAdded))
	c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0, qt.Commentf("%+v", diff.ConstraintsRemoved))
}

// TestLiveTableIdentity_DescriptionReplays describes the schema, drops it, and
// applies the rendered description: each table comes back with its own
// columns, the index comes back on docs, and the description compares equal to
// the result.
func TestLiveTableIdentity_DescriptionReplays(t *testing.T) {
	c := qt.New(t)
	f := newLiveTableIdentityFixture(c)
	description := dbschematogo.ConvertDBSchemaToGoSchema(f.read(c), f.dialect)
	statements, err := renderer.GetOrderedCreateStatements(description, f.dialect)
	c.Assert(err, qt.IsNil)
	f.execute(c, []string{"DROP SCHEMA " + f.quotedSchema() + " CASCADE", "CREATE SCHEMA " + f.quotedSchema()})

	f.execute(c, statements)

	c.Assert(f.columns(c), qt.DeepEquals, liveTableIdentityColumns)
	c.Assert(f.indexTable(c, "docs_b_idx"), qt.Equals, "docs")
	f.assertNothingPlanned(c, description)
}

// TestLiveTableIdentity_IntrospectedModelsCompareEqual writes the Go models
// `introspect` generates for the schema, reads them back as a desired schema,
// and compares them with the database they came from. Nothing is left to plan.
func TestLiveTableIdentity_IntrospectedModelsCompareEqual(t *testing.T) {
	c := qt.New(t)
	f := newLiveTableIdentityFixture(c)
	files, err := goschematogo.Render(dbschematogo.ConvertDBSchemaToGoSchema(f.read(c), f.dialect),
		goschematogo.Options{PackageName: "models", PerTable: true})
	c.Assert(err, qt.IsNil)
	dir := c.TempDir()
	c.Assert(goschematogo.WriteDir(dir, files), qt.IsNil)

	models, err := goschema.ParseDir(dir)

	c.Assert(err, qt.IsNil)
	f.assertNothingPlanned(c, models)
}
