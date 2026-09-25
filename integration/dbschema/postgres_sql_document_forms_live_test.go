//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
)

// These tests read a PostgreSQL SQL schema document through the path every CLI
// verb takes, apply what it plans against an empty schema, and plan again. The
// second plan has to be empty: a form the reader accepts but that does not
// survive the round trip through the server would plan the same statement on
// every run (stokaro/ptah#3562).

// newFormsSchema creates a schema of its own on the PostgreSQL target and
// drops it when the test ends.
func newFormsSchema(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	schemaName := fmt.Sprintf("ptah_forms_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(c.Context(), `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		dbschema.CloseAndWarn(conn)
	})
	return conn, schemaName
}

// writeFormsDocument writes the files of a schema document, each body with the
// schema name substituted for %[1]s, and returns the path to load: the file
// itself when there is one, the directory when there are several.
func writeFormsDocument(c *qt.C, schemaName string, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	path := dir
	for name, body := range files {
		path = filepath.Join(dir, name)
		c.Assert(os.WriteFile(path, []byte(fmt.Sprintf(body, schemaName)), 0o600), qt.IsNil)
	}
	if len(files) > 1 {
		return dir
	}
	return path
}

// planFormsDocument loads the document and plans it against the live schema.
func planFormsDocument(
	c *qt.C, conn *dbschema.DatabaseConnection, schemaName string, files map[string]string,
) []string {
	c.Helper()
	declared, err := schemafile.LoadSources(
		[]schemafile.Source{{URL: writeFormsDocument(c, schemaName, files)}},
		schemafile.Options{Dialect: platform.Postgres},
	)
	c.Assert(err, qt.IsNil)
	return planDocumentAgainstLive(c, conn, declared, schemaName)
}

// settleFormsDocument applies what the document plans, checks that planning it
// again finds nothing to do, and returns the schema the server now reports.
func settleFormsDocument(
	c *qt.C, conn *dbschema.DatabaseConnection, schemaName string, files map[string]string,
) *catalog.Database {
	c.Helper()
	statements := planFormsDocument(c, conn, schemaName, files)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
	c.Assert(planFormsDocument(c, conn, schemaName, files), qt.HasLen, 0,
		qt.Commentf("the document plans again after its own statements ran"))
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	return live
}

func liveColumnDefault(c *qt.C, live *catalog.Database, table, column string) string {
	c.Helper()
	for _, candidate := range live.Tables {
		if candidate.Name != table {
			continue
		}
		for _, col := range candidate.Columns {
			if col.Name == column && col.ColumnDefault != nil {
				return *col.ColumnDefault
			}
		}
	}
	return ""
}

// An array cast in a column default is read whole, element type, modifiers and
// brackets included, and compares clean against what pg_get_expr reports.
// '{}'::text[][] comes back as '{}'::text[]: the server keeps no dimensions.
func TestPostgresLiveSQLDocumentArrayCastDefaultsConverge(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)

	live := settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `
CREATE TABLE "%[1]s".casts (
    id       integer PRIMARY KEY,
    site_ids uuid[] NOT NULL DEFAULT '{}'::uuid[],
    grid     text[][] DEFAULT '{}'::text[][],
    amounts  numeric(5,2)[] DEFAULT '{1.5}'::numeric(5,2)[],
    doc      jsonb NOT NULL DEFAULT '{}'::jsonb
);
`})

	c.Assert(liveColumnDefault(c, live, "casts", "site_ids"), qt.Equals, "'{}'::uuid[]")
	c.Assert(liveColumnDefault(c, live, "casts", "grid"), qt.Equals, "'{}'::text[]")
	c.Assert(liveColumnDefault(c, live, "casts", "amounts"), qt.Equals, "'{1.5}'::numeric(5,2)[]")
}

// The control for the test above: the comparison still sees an array default
// that changed, so the clean second plan is agreement and not blindness.
func TestPostgresLiveSQLDocumentArrayCastDefaultChangePlans(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `
CREATE TABLE "%[1]s".casts (id integer PRIMARY KEY, site_ids uuid[] NOT NULL DEFAULT '{}'::uuid[]);
`})

	statements := planFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `
CREATE TABLE "%[1]s".casts (
    id       integer PRIMARY KEY,
    site_ids uuid[] NOT NULL DEFAULT '{00000000-0000-0000-0000-000000000001}'::uuid[]
);
`})

	c.Assert(strings.Join(statements, "\n"), qt.Contains, "00000000-0000-0000-0000-000000000001")
}

// A column added by ALTER TABLE ... ADD COLUMN [IF NOT EXISTS] reaches the
// table, in the order the document adds it, whether the statement is in the
// file that creates the table or in a later file of a schema directory. In the
// directory, the IF NOT EXISTS of a column the first file declared adds
// nothing.
func TestPostgresLiveSQLDocumentAddColumnConverges(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]string
		wantColumns []string
	}{
		{
			name: "one file",
			files: map[string]string{"schema.sql": `
CREATE TABLE "%[1]s".users (id integer PRIMARY KEY);
ALTER TABLE "%[1]s".users ADD COLUMN IF NOT EXISTS password_changed_at timestamptz;
ALTER TABLE "%[1]s".users ADD COLUMN note text;
`},
			wantColumns: []string{"id", "password_changed_at", "note"},
		},
		{
			name: "a later file of a directory",
			files: map[string]string{
				"1_users.sql": `CREATE TABLE "%[1]s".users (id integer PRIMARY KEY, note text);` + "\n",
				"2_users.sql": `ALTER TABLE "%[1]s".users ADD COLUMN IF NOT EXISTS password_changed_at timestamptz;` + "\n" +
					`ALTER TABLE "%[1]s".users ADD COLUMN IF NOT EXISTS note text;` + "\n",
			},
			wantColumns: []string{"id", "note", "password_changed_at"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)

			live := settleFormsDocument(c, conn, schemaName, test.files)

			c.Assert(liveColumnNames(c, live, "users"), qt.DeepEquals, test.wantColumns)
		})
	}
}

func liveColumnNames(c *qt.C, live *catalog.Database, table string) []string {
	c.Helper()
	var names []string
	for _, candidate := range live.Tables {
		if candidate.Name != table {
			continue
		}
		for _, column := range candidate.Columns {
			names = append(names, column.Name)
		}
	}
	return names
}

// STRICT and its long spelling RETURNS NULL ON NULL INPUT are one property, and
// the server reports it back as proisstrict. PARALLEL rides along because the
// function that raised the issue declares both.
func TestPostgresLiveSQLDocumentStrictFunctionConverges(t *testing.T) {
	tests := []struct {
		name   string
		clause string
	}{
		{name: "STRICT", clause: "STRICT"},
		{name: "RETURNS NULL ON NULL INPUT", clause: "RETURNS NULL ON NULL INPUT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)

			live := settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `
CREATE FUNCTION "%[1]s".add_arrays(a integer[], b integer[]) RETURNS integer[]
    LANGUAGE sql IMMUTABLE ` + test.clause + ` PARALLEL SAFE
    AS $$ SELECT a || b $$;
`})

			c.Assert(live.Functions, qt.HasLen, 1)
			c.Assert(live.Functions[0].Strict, qt.IsTrue)
			c.Assert(live.Functions[0].Parallel, qt.Equals, "SAFE")
		})
	}
}

// The control: CALLED ON NULL INPUT against a strict routine plans a change,
// so the comparison reads the property rather than folding it away.
func TestPostgresLiveSQLDocumentStrictnessChangePlans(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `
CREATE FUNCTION "%[1]s".twice(a integer) RETURNS integer LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT a * 2 $$;
`})

	statements := planFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": `
CREATE FUNCTION "%[1]s".twice(a integer) RETURNS integer LANGUAGE sql IMMUTABLE CALLED ON NULL INPUT AS $$ SELECT a * 2 $$;
`})

	joined := strings.Join(statements, "\n")
	c.Assert(joined, qt.Contains, "twice")
	c.Assert(joined, qt.Not(qt.Contains), "STRICT")
}
