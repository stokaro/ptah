//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/envbool/envbooltest"
)

// routinesWithArgumentDefaults are routines the server prints back differently
// from their declaration: a default gains a cast, `=` becomes DEFAULT, a type
// loses its modifier. Compared as text, each is dropped and created again on
// every plan, and a declaration lower-cased whole creates a default written
// `'X'` as `'x'` (stokaro/ptah#3673).
const routinesWithArgumentDefaults = `CREATE FUNCTION score(a integer, b text DEFAULT 'X') RETURNS integer LANGUAGE sql AS $$SELECT 1$$;
CREATE FUNCTION sized(a varchar(50), b numeric(10,2) = 1.5, "Label" text DEFAULT 'A,b', d int[] DEFAULT ARRAY[1, 2], e text DEFAULT NULL)
  RETURNS integer LANGUAGE sql AS $$SELECT 1$$;
CREATE PROCEDURE touch(a integer, b text DEFAULT 'Y') LANGUAGE sql AS $$SELECT 1$$;`

// routineArgumentDefaultControls change one default for real, so a comparison
// that stopped seeing differences would pass the synced tests and fail here.
var routineArgumentDefaultControls = []struct {
	name       string
	migration  string
	schema     string
	wantInPlan string
}{
	{
		// The literal's case is the value. Lower-cased, the declaration plans
		// nothing against a routine created with `'x'`.
		name:       "a function's default changes case",
		migration:  `CREATE FUNCTION score(a integer, b text DEFAULT 'x') RETURNS integer LANGUAGE sql AS $$SELECT 1$$;`,
		schema:     `CREATE FUNCTION score(a integer, b text DEFAULT 'X') RETURNS integer LANGUAGE sql AS $$SELECT 1$$;`,
		wantInPlan: "'X'",
	},
	{
		name:       "a procedure's default changes",
		migration:  `CREATE PROCEDURE touch(a integer, b text DEFAULT 'Y') LANGUAGE sql AS $$SELECT 1$$;`,
		schema:     `CREATE PROCEDURE touch(a integer, b text DEFAULT 'Z') LANGUAGE sql AS $$SELECT 1$$;`,
		wantInPlan: "'Z'",
	},
}

// replayingRoutines lets a migration replay create routines. The dev database
// is a scratch database on a test server nothing else depends on, which is
// what the setting declares; without it the replay refuses CREATE FUNCTION,
// because a routine's effects cannot be confined to the dev database.
func replayingRoutines(c *qt.C) {
	c.Helper()
	envbooltest.Set(devdocker.DisposableServerEnvVar, "1")(c)
}

// TestSchemaApplyFindsRoutineArgumentDefaultsSyncedE2E plans the schema file
// against the database the same SQL built: nothing is planned.
func TestSchemaApplyFindsRoutineArgumentDefaultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, routinesWithArgumentDefaults, routinesWithArgumentDefaults)
	target := databaseBuiltFrom(c, routinesWithArgumentDefaults)

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestSchemaDiffFromADatabaseFindsRoutineArgumentDefaultsSyncedE2E is the same
// comparison through schema diff, which asks the --from database.
func TestSchemaDiffFromADatabaseFindsRoutineArgumentDefaultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, routinesWithArgumentDefaults, routinesWithArgumentDefaults)
	source := databaseBuiltFrom(c, routinesWithArgumentDefaults)

	out := runPtahNative(c, "schema", "diff", "--from", source, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
}

// TestMigrateDiffFindsRoutineArgumentDefaultsSyncedE2E replays a directory
// whose one migration is the schema file and diffs: the directory is synced.
func TestMigrateDiffFindsRoutineArgumentDefaultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	replayingRoutines(c)
	dir, schema := writeRewrittenMigrationProject(c, routinesWithArgumentDefaults, routinesWithArgumentDefaults)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}

// TestMigrationsGenerateReplayFindsRoutineArgumentDefaultsSyncedE2E is the
// native replay: no migration is written.
func TestMigrationsGenerateReplayFindsRoutineArgumentDefaultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	replayingRoutines(c)
	dir, schema := writeRewrittenMigrationProject(c, routinesWithArgumentDefaults, routinesWithArgumentDefaults)
	dev, _ := scratchReplayDatabase(c)

	out := runPtahNative(c, "migrations", "generate", "--schema-file", schema,
		"--migrations-dir", dir, "--replay", "--dev-url", dev, "--dir-format", "atlas")

	c.Assert(out, qt.Contains, "no migration files generated")
}

// TestSchemaApplyPlansARoutineArgumentDefaultThatChangedE2E is the control for
// the schema apply row.
func TestSchemaApplyPlansARoutineArgumentDefaultThatChangedE2E(t *testing.T) {
	for _, test := range routineArgumentDefaultControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			target := databaseBuiltFrom(c, test.migration)

			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}

// TestMigrateDiffPlansARoutineArgumentDefaultThatChangedE2E is the control for
// the migrate diff row.
func TestMigrateDiffPlansARoutineArgumentDefaultThatChangedE2E(t *testing.T) {
	for _, test := range routineArgumentDefaultControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			replayingRoutines(c)
			dir, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
				"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}

// TestSchemaApplyCreatesARoutineWithTheDeclaredDefaultE2E applies the schema
// file to an empty database and reads back what the server stored: the
// default the author wrote, not a lower-cased one.
func TestSchemaApplyCreatesARoutineWithTheDeclaredDefaultE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, "", routinesWithArgumentDefaults)
	target, _ := scratchReplayDatabase(c)

	runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--auto-approve")

	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var score, touch string
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT pg_get_function_arguments('score'::regproc), pg_get_function_arguments('touch'::regproc)",
	).Scan(&score, &touch), qt.IsNil)
	c.Assert(score, qt.Equals, "a integer, b text DEFAULT 'X'::text")
	c.Assert(touch, qt.Equals, "IN a integer, IN b text DEFAULT 'Y'::text")
}
