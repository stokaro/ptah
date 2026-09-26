//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// quotedRoutineBodies write each body as a string literal, in every form
// PostgreSQL takes. The plan writes a body back between dollar quotes, where a
// doubled quote and a backslash escape are not escapes, so a body read with
// its quoting kept failed to apply, was cut at its first doubled quote, or,
// for a procedure written as an escape string, was created empty and applied
// without an error. A body holding `$$` ended the plan's own dollar quote
// (stokaro/ptah#3691).
const quotedRoutineBodies = `CREATE TABLE notes (id integer PRIMARY KEY, note text);
CREATE FUNCTION doubled() RETURNS text LANGUAGE sql AS 'SELECT ''x''';
CREATE FUNCTION escaped() RETURNS text LANGUAGE sql AS E'SELECT \'x\'';
CREATE FUNCTION dollars() RETURNS text LANGUAGE sql AS $f$SELECT $$x$$$f$;
CREATE PROCEDURE add_doubled() LANGUAGE sql AS 'INSERT INTO notes VALUES (1, ''x'')';
CREATE PROCEDURE add_escaped() LANGUAGE sql AS E'INSERT INTO notes VALUES (2, \'x\')';
CREATE FUNCTION stamp() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN NEW.note := ''stamped''; RETURN NEW; END;';
CREATE TRIGGER notes_stamp BEFORE INSERT ON notes FOR EACH ROW EXECUTE FUNCTION stamp();`

// TestSchemaApplyCreatesQuotedRoutineBodiesAsWrittenE2E applies the file to an
// empty database, reads each stored body back, and plans the file again: the
// bodies are the text between the quotes, and nothing is planned.
func TestSchemaApplyCreatesQuotedRoutineBodiesAsWrittenE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, "", quotedRoutineBodies)
	target, _ := scratchReplayDatabase(c)

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	bodies := make(map[string]string)
	rows, err := conn.QueryContext(c.Context(),
		"SELECT proname, prosrc FROM pg_proc WHERE pronamespace = 'public'::regnamespace")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	for rows.Next() {
		var name, body string
		c.Assert(rows.Scan(&name, &body), qt.IsNil)
		bodies[name] = body
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(bodies["doubled"], qt.Equals, "\nSELECT 'x'\n")
	c.Assert(bodies["escaped"], qt.Equals, "\nSELECT 'x'\n")
	c.Assert(bodies["dollars"], qt.Equals, "\nSELECT $$x$$\n")
	c.Assert(bodies["add_doubled"], qt.Equals, "\nINSERT INTO notes VALUES (1, 'x')\n")
	c.Assert(bodies["add_escaped"], qt.Equals, "\nINSERT INTO notes VALUES (2, 'x')\n")
	_, err = conn.ExecContext(c.Context(), "CALL add_doubled(); CALL add_escaped()")
	c.Assert(err, qt.IsNil)
	var stamped int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM notes WHERE note = 'stamped'").Scan(&stamped), qt.IsNil)
	c.Assert(stamped, qt.Equals, 2)

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestMigrateDiffFindsQuotedRoutineBodiesSyncedE2E replays a directory whose
// one migration is the schema file, so the server stores each body from the
// SQL itself, and compares the file with it: the file reads each body as the
// server stored it. The replay creates routines, which a dev database takes
// only when it is declared disposable, as this scratch database is.
func TestMigrateDiffFindsQuotedRoutineBodiesSyncedE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	dir, schema := writeRewrittenMigrationProject(c, quotedRoutineBodies, quotedRoutineBodies)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}
