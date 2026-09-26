//go:build integration

package dbschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// routineCommentsDocument declares a function, a procedure, a materialized
// view, a trigger, a policy and an enum type, each of which carries its
// comment in a separate COMMENT ON, the way pg_dump writes a schema. The
// routines take no parameter default: stokaro/ptah#3673 keeps such a routine
// from converging at all.
const routineCommentsDocument = `
CREATE TABLE "%[1]s".notes (id integer PRIMARY KEY, body text);
CREATE FUNCTION "%[1]s".score(a integer, b text) RETURNS integer LANGUAGE sql AS 'SELECT 1';
CREATE PROCEDURE "%[1]s".tidy(a integer) LANGUAGE sql AS 'SELECT 1';
CREATE MATERIALIZED VIEW "%[1]s".totals AS SELECT count(*) AS n FROM "%[1]s".notes;
CREATE FUNCTION "%[1]s".stamp() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER stamp_notes BEFORE INSERT ON "%[1]s".notes FOR EACH ROW EXECUTE FUNCTION "%[1]s".stamp();
ALTER TABLE "%[1]s".notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY own_notes ON "%[1]s".notes USING (true);
CREATE TYPE "%[1]s".mood AS ENUM ('ok', 'bad');
`

// routineComments is the COMMENT ON block for routineCommentsDocument, one
// statement per object, with text standing for the comment of each.
func routineComments(text string) string {
	return `
COMMENT ON FUNCTION "%[1]s".score(integer, text) IS '` + text + ` function';
COMMENT ON PROCEDURE "%[1]s".tidy(integer) IS '` + text + ` procedure';
COMMENT ON MATERIALIZED VIEW "%[1]s".totals IS '` + text + ` matview';
COMMENT ON TRIGGER stamp_notes ON "%[1]s".notes IS '` + text + ` trigger';
COMMENT ON POLICY own_notes ON "%[1]s".notes IS '` + text + ` policy';
COMMENT ON TYPE "%[1]s".mood IS '` + text + ` enum';
`
}

// liveRoutineComments reads the comment of every object the document declares
// straight from the server.
func liveRoutineComments(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string) map[string]string {
	c.Helper()
	rows, err := conn.QueryContext(c.Context(), `
		SELECT p.proname, COALESCE(obj_description(p.oid, 'pg_proc'), '')
		FROM pg_proc p WHERE p.pronamespace = $1::regnamespace AND p.proname IN ('score', 'tidy')
		UNION ALL
		SELECT c.relname, COALESCE(obj_description(c.oid, 'pg_class'), '')
		FROM pg_class c WHERE c.relnamespace = $1::regnamespace AND c.relkind = 'm'
		UNION ALL
		SELECT t.tgname, COALESCE(obj_description(t.oid, 'pg_trigger'), '')
		FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid
		WHERE c.relnamespace = $1::regnamespace AND NOT t.tgisinternal
		UNION ALL
		SELECT p.polname, COALESCE(obj_description(p.oid, 'pg_policy'), '')
		FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid WHERE c.relnamespace = $1::regnamespace
		UNION ALL
		SELECT t.typname, COALESCE(obj_description(t.oid, 'pg_type'), '')
		FROM pg_type t WHERE t.typnamespace = $1::regnamespace AND t.typtype = 'e'`, schemaName)
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

// wantRoutineComments is what liveRoutineComments reads after
// routineComments(text) applied.
func wantRoutineComments(text string) map[string]string {
	return map[string]string{
		"score":       text + " function",
		"tidy":        text + " procedure",
		"totals":      text + " matview",
		"stamp_notes": text + " trigger",
		"own_notes":   text + " policy",
		"mood":        text + " enum",
	}
}

// noRoutineComments is what liveRoutineComments reads when the document states
// no comment: every object is there, and none carries one.
var noRoutineComments = map[string]string{
	"score": "", "tidy": "", "totals": "", "stamp_notes": "", "own_notes": "", "mood": "",
}

// Each comment reaches the server with its object, and the document plans
// nothing once applied (stokaro/ptah#3646). Without the COMMENT ON rendering
// each comment is a `-- text` line in the script, and without Enum.Comment the
// enum's comment has nowhere to live, so the second plan is never empty.
func TestPostgresLiveRoutineCommentsAreWrittenWithTheObject(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)

	settleFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": routineCommentsDocument + routineComments("first"),
	})

	c.Assert(liveRoutineComments(c, conn, schemaName), qt.DeepEquals, wantRoutineComments("first"))
}

// A changed comment is planned as COMMENT ON the object, not as a drop and a
// create, and the plan converges.
func TestPostgresLiveRoutineCommentsChangeInPlace(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": routineCommentsDocument + routineComments("first"),
	})
	changed := map[string]string{"schema.sql": routineCommentsDocument + routineComments("second")}

	statements := planFormsDocument(c, conn, schemaName, changed)

	c.Assert(statements, qt.HasLen, 6, qt.Commentf("statements:\n%s", strings.Join(statements, "\n")))
	for _, statement := range statements {
		c.Assert(statement, qt.Matches,
			`(?s)COMMENT ON (FUNCTION|PROCEDURE|MATERIALIZED VIEW|TRIGGER|POLICY|TYPE) .* IS 'second \w+';?\s*`)
	}
	settleFormsDocument(c, conn, schemaName, changed)
	c.Assert(liveRoutineComments(c, conn, schemaName), qt.DeepEquals, wantRoutineComments("second"))
}

// A comment the document no longer states is removed.
func TestPostgresLiveRoutineCommentsRemovedFromTheDocument(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	settleFormsDocument(c, conn, schemaName, map[string]string{
		"schema.sql": routineCommentsDocument + routineComments("first"),
	})
	uncommented := map[string]string{"schema.sql": routineCommentsDocument}

	statements := planFormsDocument(c, conn, schemaName, uncommented)

	c.Assert(statements, qt.HasLen, 6, qt.Commentf("statements:\n%s", strings.Join(statements, "\n")))
	for _, statement := range statements {
		c.Assert(statement, qt.Matches, `(?s)COMMENT ON .* IS NULL;?\s*`)
	}
	settleFormsDocument(c, conn, schemaName, uncommented)
	c.Assert(liveRoutineComments(c, conn, schemaName), qt.DeepEquals, noRoutineComments)
}

// Each object is written again by a change beside its comment: the function
// replaced in place, the materialized view, the policy and the enum dropped
// and created, the trigger replaced. Whatever the statement does to the
// comment it had, the object ends with the comment the document states.
func TestPostgresLiveRoutineCommentsSurviveTheirObjectBeingWrittenAgain(t *testing.T) {
	tests := []struct {
		name     string
		comments string
		want     map[string]string
	}{
		{name: "the comment kept", comments: routineComments("first"), want: wantRoutineComments("first")},
		{name: "the comment changed", comments: routineComments("second"), want: wantRoutineComments("second")},
		{name: "the comment removed", comments: "", want: noRoutineComments},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newFormsSchema(c)
			settleFormsDocument(c, conn, schemaName, map[string]string{
				"schema.sql": routineCommentsDocument + routineComments("first"),
			})
			reshaped := strings.NewReplacer(
				`AS 'SELECT 1';
CREATE PROCEDURE`, `AS 'SELECT 7';
CREATE PROCEDURE`,
				`SELECT count(*) AS n FROM`, `SELECT count(*) AS n, max(id) AS top FROM`,
				`USING (true)`, `USING (id > 0)`,
				`BEFORE INSERT ON`, `AFTER INSERT ON`,
				`('ok', 'bad')`, `('ok')`,
			).Replace(routineCommentsDocument)

			settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": reshaped + test.comments})

			c.Assert(liveRoutineComments(c, conn, schemaName), qt.DeepEquals, test.want)
		})
	}
}

// A table constraint's comment, stated with COMMENT ON CONSTRAINT ... ON the
// way pg_dump writes it, is written after the table, changed in place and
// removed, and the document plans nothing after each apply.
func TestPostgresLiveConstraintCommentFollowsTheDocument(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newFormsSchema(c)
	table := `
CREATE TABLE "%[1]s".notes (id integer PRIMARY KEY, CONSTRAINT positive CHECK (id > 0));
`
	steps := []struct {
		comment string
		want    string
	}{
		{comment: `COMMENT ON CONSTRAINT positive ON "%[1]s".notes IS 'ids start at one';`, want: "ids start at one"},
		{comment: `COMMENT ON CONSTRAINT positive ON "%[1]s".notes IS 'ids are positive';`, want: "ids are positive"},
		{comment: "", want: ""},
	}
	for _, step := range steps {
		settleFormsDocument(c, conn, schemaName, map[string]string{"schema.sql": table + step.comment + "\n"})

		var comment string
		c.Assert(conn.QueryRowContext(c.Context(), `
			SELECT COALESCE(obj_description(con.oid, 'pg_constraint'), '')
			FROM pg_constraint con WHERE con.connamespace = $1::regnamespace AND con.conname = 'positive'`,
			schemaName,
		).Scan(&comment), qt.IsNil)
		c.Assert(comment, qt.Equals, step.want)
	}
}
