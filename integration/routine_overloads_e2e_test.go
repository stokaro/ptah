//go:build integration

package integration_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// routineOverloads declares two overloads of a function and two of a
// procedure, which is what pg_dump writes for a schema that overloads them.
// The model kept one routine per name, so the file described a database with
// one of each: an apply created the first overload only, and a plan against a
// database holding both dropped the second (stokaro/ptah#3672).
const routineOverloads = `CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS $$SELECT 1$$;
CREATE FUNCTION f(a int, b text) RETURNS int LANGUAGE sql AS $$SELECT 2$$;
CREATE PROCEDURE p(a int) LANGUAGE sql AS $$SELECT 1$$;
CREATE PROCEDURE p(a int, b text) LANGUAGE sql AS $$SELECT 2$$;`

// routinesIn answers the routines of the public schema the way PostgreSQL
// names them, overload by overload.
func routinesIn(c *qt.C, url string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(),
		"SELECT p.oid::regprocedure::text FROM pg_proc p WHERE p.pronamespace = 'public'::regnamespace ORDER BY 1")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var routines []string
	for rows.Next() {
		var routine string
		c.Assert(rows.Scan(&routine), qt.IsNil)
		routines = append(routines, routine)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return routines
}

// dropStatements answers the DROP statements a command printed, in order.
func dropStatements(out string) []string {
	var drops []string
	for line := range strings.Lines(out) {
		if strings.HasPrefix(line, "DROP ") {
			drops = append(drops, strings.TrimSpace(line))
		}
	}
	return drops
}

// TestSchemaApplyCreatesEveryOverloadE2E applies the file to an empty database
// and plans it again: every overload is created, and nothing is planned after.
func TestSchemaApplyCreatesEveryOverloadE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, "", routineOverloads)
	target, _ := scratchReplayDatabase(c)

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(routinesIn(c, target), qt.DeepEquals, []string{"f(integer)", "f(integer,text)", "p(integer)", "p(integer,text)"})
	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")
	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestSchemaDiffFromADatabaseFindsEveryOverloadSyncedE2E diffs the database
// the file built against the file.
func TestSchemaDiffFromADatabaseFindsEveryOverloadSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, routineOverloads, routineOverloads)
	source := databaseBuiltFrom(c, routineOverloads)

	out := runPtahNative(c, "schema", "diff", "--from", source, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
}

// TestMigrateDiffFindsEveryOverloadSyncedE2E replays a directory whose one
// migration is the file and diffs it against the file. The replay creates
// routines, which a dev database takes only when it is declared disposable, as
// this scratch database is.
func TestMigrateDiffFindsEveryOverloadSyncedE2E(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
	dir, schema := writeRewrittenMigrationProject(c, routineOverloads, routineOverloads)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}

// TestSchemaApplyDropsOnlyTheOverloadTheFileLeavesOutE2E removes one overload
// of each routine from the file and keeps two of the function's: the plan drops
// exactly the two left out, by the argument list that names each, and keeps
// the rest. Read one routine per name, the file kept one overload of f and the
// plan dropped the other it declares.
func TestSchemaApplyDropsOnlyTheOverloadTheFileLeavesOutE2E(t *testing.T) {
	c := qt.New(t)
	built := routineOverloads + "\nCREATE FUNCTION f(a text) RETURNS int LANGUAGE sql AS $$SELECT 3$$;"
	kept := `CREATE FUNCTION f(a int) RETURNS int LANGUAGE sql AS $$SELECT 1$$;
CREATE FUNCTION f(a int, b text) RETURNS int LANGUAGE sql AS $$SELECT 2$$;
CREATE PROCEDURE p(a int, b text) LANGUAGE sql AS $$SELECT 2$$;`
	_, schema := writeRewrittenMigrationProject(c, built, kept)
	target := databaseBuiltFrom(c, built)

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	c.Assert(dropStatements(out), qt.DeepEquals, []string{
		`DROP FUNCTION IF EXISTS "f"(a text);`,
		`DROP PROCEDURE IF EXISTS "p"(IN a integer);`,
	})
	c.Assert(routinesIn(c, target), qt.DeepEquals, []string{"f(integer)", "f(integer,text)", "p(integer,text)"})
}
