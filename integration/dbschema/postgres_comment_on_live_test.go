//go:build integration

package dbschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
)

// commentedDocument declares each object with CREATE and describes it with a
// separate COMMENT ON, the way pg_dump writes a schema.
const commentedDocument = `
CREATE SCHEMA "%[1]s";
CREATE TABLE "%[1]s".notes (id integer PRIMARY KEY, body text);
CREATE INDEX notes_body_idx ON "%[1]s".notes (body);
COMMENT ON SCHEMA "%[1]s" IS 'the notes schema';
COMMENT ON TABLE "%[1]s".notes IS 'what users wrote';
COMMENT ON COLUMN "%[1]s".notes.body IS 'the text, as typed';
COMMENT ON INDEX "%[1]s".notes_body_idx IS 'search by body';
`

func liveTableComment(live *catalog.Database, table string) string {
	for _, candidate := range live.Tables {
		if candidate.Name == table {
			return candidate.Comment
		}
	}
	return ""
}

func liveColumnComment(live *catalog.Database, table, column string) string {
	for _, candidate := range live.Tables {
		for _, col := range candidate.Columns {
			if candidate.Name == table && col.Name == column {
				return col.Comment
			}
		}
	}
	return ""
}

// Each comment a separate COMMENT ON sets reaches the object, is applied, and
// plans nothing again (stokaro/ptah#3610). Dropped, the comments were missing
// from the desired schema and a plan removed every one the database had.
func TestPostgresLiveSQLDocumentCommentOnConverges(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)

	live := settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": commentedDocument})

	c.Assert(liveTableComment(live, "notes"), qt.Equals, "what users wrote")
	c.Assert(liveColumnComment(live, "notes", "body"), qt.Equals, "the text, as typed")
	// The rest are read from the server directly, so a kind whose comment the
	// plan never wrote cannot pass by comparing clean against itself: the kinds
	// stokaro/ptah#3627 owns are refused rather than read for that reason.
	rows, err := conn.QueryContext(c.Context(), `
		SELECT c.relname, COALESCE(obj_description(c.oid, 'pg_class'), '')
		FROM pg_class c WHERE c.relnamespace = $1::regnamespace
		UNION ALL
		SELECT '(schema)', COALESCE(obj_description($1::regnamespace, 'pg_namespace'), '')`, schemaName)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	comments := make(map[string]string)
	for rows.Next() {
		var name, comment string
		c.Assert(rows.Scan(&name, &comment), qt.IsNil)
		comments[name] = comment
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(comments["notes_body_idx"], qt.Equals, "search by body")
	c.Assert(comments["(schema)"], qt.Equals, "the notes schema")
}

// The control: against a database whose comments the document states, the
// plan is empty; a document that changes one plans it.
func TestPostgresLiveSQLDocumentCommentOnChangePlans(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": commentedDocument})

	statements := planFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": commentedDocument +
		`COMMENT ON COLUMN "%[1]s".notes.body IS 'the text, trimmed';`})

	c.Assert(statements, qt.Not(qt.HasLen), 0)
}
