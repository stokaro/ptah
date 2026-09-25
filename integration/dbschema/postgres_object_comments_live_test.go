//go:build integration

package dbschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// objectCommentsDocument declares a view, a sequence, a domain, a composite
// type, a range type and an extension, and describes each with a separate
// COMMENT ON, the way pg_dump writes a schema. The extension is installed in
// the test's own schema, so dropping the schema removes it.
const objectCommentsDocument = `
CREATE SCHEMA "%[1]s";
CREATE EXTENSION fuzzystrmatch WITH SCHEMA "%[1]s";
CREATE TABLE "%[1]s".notes (id integer PRIMARY KEY, body text);
CREATE VIEW "%[1]s".recent AS SELECT id FROM "%[1]s".notes;
CREATE SEQUENCE "%[1]s".ticket;
CREATE DOMAIN "%[1]s".positive AS integer CHECK (VALUE > 0);
CREATE TYPE "%[1]s".point2 AS (x integer, y integer);
CREATE TYPE "%[1]s".span AS RANGE (subtype = integer);
`

// objectComments is the COMMENT ON block for objectCommentsDocument, one
// statement per object, with text standing for the comment of each.
func objectComments(text string) string {
	return `
COMMENT ON VIEW "%[1]s".recent IS '` + text + ` view';
COMMENT ON SEQUENCE "%[1]s".ticket IS '` + text + ` sequence';
COMMENT ON DOMAIN "%[1]s".positive IS '` + text + ` domain';
COMMENT ON TYPE "%[1]s".point2 IS '` + text + ` composite';
COMMENT ON TYPE "%[1]s".span IS '` + text + ` range';
COMMENT ON EXTENSION fuzzystrmatch IS '` + text + ` extension';
`
}

// liveObjectComments reads the comment of every object the document declares
// straight from the server, so a comment the plan never wrote cannot pass by
// comparing clean against a read that does not look for it.
func liveObjectComments(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string) map[string]string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), `
		SELECT c.relname, COALESCE(obj_description(c.oid, 'pg_class'), '')
		FROM pg_class c
		WHERE c.relnamespace = $1::regnamespace AND c.relkind IN ('v', 'S')
		UNION ALL
		SELECT t.typname, COALESCE(obj_description(t.oid, 'pg_type'), '')
		FROM pg_type t
		LEFT JOIN pg_class k ON k.oid = t.typrelid
		WHERE t.typnamespace = $1::regnamespace
		  AND (t.typtype IN ('d', 'r') OR (t.typtype = 'c' AND k.relkind = 'c'))
		UNION ALL
		SELECT e.extname, COALESCE(obj_description(e.oid, 'pg_extension'), '')
		FROM pg_extension e
		WHERE e.extnamespace = $1::regnamespace`, schemaName)
	c.Assert(err, qt.IsNil)
	defer func() { _ = rows.Close() }()
	comments := make(map[string]string)
	for rows.Next() {
		var name, comment string
		c.Assert(rows.Scan(&name, &comment), qt.IsNil)
		comments[name] = comment
	}
	c.Assert(rows.Err(), qt.IsNil)
	return comments
}

// wantObjectComments is what liveObjectComments reads after
// objectComments(text) applied. The extension keeps the comment of the
// document that last stated one: no comment is compared where the
// declaration states none.
func wantObjectComments(text, extension string) map[string]string {
	comment := func(kind string) string {
		if text == "" {
			return ""
		}
		return text + " " + kind
	}
	return map[string]string{
		"recent":        comment("view"),
		"ticket":        comment("sequence"),
		"positive":      comment("domain"),
		"point2":        comment("composite"),
		"span":          comment("range"),
		"fuzzystrmatch": extension,
	}
}

// Every object's comment reaches the server with the object, and the
// document plans nothing once it is applied (stokaro/ptah#3627). Before the
// plan wrote them, each comment was a `-- text` line in the script or
// nothing at all, and the second plan was just as empty.
func TestPostgresLiveObjectCommentsAreWrittenWithTheObject(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)

	settleFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": objectCommentsDocument + objectComments("first"),
	})

	c.Assert(liveObjectComments(c, conn, schemaName), qt.DeepEquals, wantObjectComments("first", "first extension"))
}

// A changed comment is planned as COMMENT ON the object, not as a drop and a
// create, and the plan converges.
func TestPostgresLiveObjectCommentsChangeInPlace(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": objectCommentsDocument + objectComments("first"),
	})
	changed := map[string]string{"schema.sql": objectCommentsDocument + objectComments("second")}

	statements := planFormsDocument(c, conn, schemaName, changed)

	c.Assert(statements, qt.HasLen, 6, qt.Commentf("statements:\n%s", strings.Join(statements, "\n")))
	for _, statement := range statements {
		c.Assert(statement, qt.Matches, `(?s)COMMENT ON (VIEW|SEQUENCE|DOMAIN|TYPE|EXTENSION) .* IS 'second \w+';?\s*`)
	}
	settleFormsDocument(c, conn, schemaName, changed)
	c.Assert(liveObjectComments(c, conn, schemaName), qt.DeepEquals, wantObjectComments("second", "second extension"))
}

// A comment the document no longer states is removed, except an
// extension's: CREATE EXTENSION gives every extension the comment its
// control file carries, so a declaration without one is not read as asking
// for none.
func TestPostgresLiveObjectCommentsRemovedFromTheDocument(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": objectCommentsDocument + objectComments("first"),
	})
	uncommented := map[string]string{"schema.sql": objectCommentsDocument}

	statements := planFormsDocument(c, conn, schemaName, uncommented)

	c.Assert(statements, qt.HasLen, 5, qt.Commentf("statements:\n%s", strings.Join(statements, "\n")))
	for _, statement := range statements {
		c.Assert(statement, qt.Matches, `(?s)COMMENT ON (VIEW|SEQUENCE|DOMAIN|TYPE) .* IS NULL;?\s*`)
	}
	settleFormsDocument(c, conn, schemaName, uncommented)
	c.Assert(liveObjectComments(c, conn, schemaName), qt.DeepEquals, wantObjectComments("", "first extension"))
}

// A view replaced in place keeps the comment it had, and a domain dropped and
// created again loses it; the plan has to leave both with the comment the
// document states.
func TestPostgresLiveObjectCommentsSurviveReplaceAndRecreate(t *testing.T) {
	tests := []struct {
		name     string
		comments string
		want     string
	}{
		{name: "the comment kept", comments: objectComments("first"), want: "first"},
		{name: "the comment changed", comments: objectComments("second"), want: "second"},
		{name: "the comment removed", comments: "", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			settleFormsDocument(c, conn, schemaName, map[string]string{
				"schema.sql": objectCommentsDocument + objectComments("first"),
			})
			// The view gains a column, which PostgreSQL replaces in place,
			// and the domain changes its base type, which has no ALTER.
			reshaped := strings.NewReplacer(
				`AS SELECT id FROM`, `AS SELECT id, body FROM`,
				`positive AS integer`, `positive AS bigint`,
			).Replace(objectCommentsDocument)

			settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": reshaped + test.comments})

			comments := liveObjectComments(c, conn, schemaName)
			c.Assert(comments["recent"], qt.Equals, wantObjectComments(test.want, "")["recent"])
			c.Assert(comments["positive"], qt.Equals, wantObjectComments(test.want, "")["positive"])
		})
	}
}
