//go:build integration

package dbschema_test

import (
	"fmt"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// alterTablesScript creates two tables with CREATE TABLE and then shapes them
// with every ALTER TABLE form a schema file reads: a primary key added later,
// a default and NOT NULL set, a type changed, a column dropped and one renamed,
// constraints added, renamed and dropped.
const alterTablesCreate = `
CREATE TABLE "%[1]s".parents (id integer, code text);
CREATE TABLE "%[1]s".items (
    id        integer NOT NULL,
    parent_id integer,
    qty       integer,
    note      text,
    legacy    text,
    label     varchar(20) DEFAULT 'x'
);
`

const alterTablesAlter = `
ALTER TABLE "%[1]s".parents ADD PRIMARY KEY (id);
ALTER TABLE "%[1]s".items ADD CONSTRAINT items_pk PRIMARY KEY (id);
ALTER TABLE "%[1]s".items ALTER COLUMN qty SET DEFAULT 1, ALTER COLUMN qty SET NOT NULL;
ALTER TABLE "%[1]s".items ALTER COLUMN note TYPE varchar(200);
ALTER TABLE "%[1]s".items ALTER label DROP DEFAULT;
ALTER TABLE "%[1]s".items DROP COLUMN legacy;
ALTER TABLE "%[1]s".items DROP COLUMN IF EXISTS never_declared;
ALTER TABLE "%[1]s".items RENAME COLUMN label TO title;
ALTER TABLE "%[1]s".items ADD CONSTRAINT items_qty_check CHECK (qty > 0);
ALTER TABLE "%[1]s".items RENAME CONSTRAINT items_qty_check TO items_qty_positive;
ALTER TABLE "%[1]s".items ADD CONSTRAINT items_parent_fk FOREIGN KEY (parent_id) REFERENCES "%[1]s".parents (id);
ALTER TABLE "%[1]s".items ADD CONSTRAINT items_note_uq UNIQUE (note);
ALTER TABLE "%[1]s".items DROP CONSTRAINT items_note_uq;
ALTER TABLE "%[1]s".items ALTER COLUMN parent_id SET NOT NULL;
ALTER TABLE "%[1]s".items ALTER COLUMN parent_id DROP NOT NULL;
`

// alterTablesDocuments are the same script as one file and as a directory
// whose later file alters what the earlier one created.
var alterTablesDocuments = []struct {
	name  string
	files map[string]string
}{
	{name: "one file", files: map[string]string{"schema.sql": alterTablesCreate + alterTablesAlter}},
	{name: "a directory", files: map[string]string{"1_tables.sql": alterTablesCreate, "2_alter.sql": alterTablesAlter}},
}

// runDocumentOnServer runs the document's files, in file-name order, as the
// server would run them, which is what the document is supposed to describe.
func runDocumentOnServer(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string, files map[string]string) {
	c.Helper()
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		_, err := conn.ExecContext(c.Context(), fmt.Sprintf(files[name], schemaName))
		c.Assert(err, qt.IsNil, qt.Commentf("file %s", name))
	}
}

// Read back against a schema where the server ran the same script, the
// document plans nothing: what Ptah reads from the ALTER statements is what the
// server built from them. An operation the reader dropped would plan it here.
func TestPostgresLiveSQLDocumentAlterTableDescribesWhatTheServerBuilds(t *testing.T) {
	for _, document := range alterTablesDocuments {
		t.Run(document.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			runDocumentOnServer(c, conn, schemaName, document.files)

			statements := planFormsDocument(c, conn, schemaName, document.files)

			c.Assert(statements, qt.HasLen, 0)
		})
	}
}

// Applied to an empty schema, the plan builds the same tables the script
// builds, and planning again finds nothing to do.
func TestPostgresLiveSQLDocumentAlterTableApplies(t *testing.T) {
	for _, document := range alterTablesDocuments {
		t.Run(document.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			_, expectedSchema := newFormsSchema(c)
			runDocumentOnServer(c, conn, expectedSchema, document.files)
			expected, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{expectedSchema})
			c.Assert(err, qt.IsNil)

			live := settleFormsDocument(c, conn, schemaName, document.files)

			c.Assert(liveColumnNames(c, live, "items"), qt.DeepEquals, liveColumnNames(c, expected, "items"))
			c.Assert(liveColumnDefault(c, live, "items", "qty"), qt.Equals, liveColumnDefault(c, expected, "items", "qty"))
			c.Assert(liveColumnDefault(c, live, "items", "title"), qt.Equals, "")
		})
	}
}
