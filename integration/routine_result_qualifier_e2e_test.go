//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// routinesReturningQualifiedTypes declare results whose type is qualified with
// its schema. pg_get_function_result prints the schema only where the search
// path does not reach the type, so the first three read back without `public.`
// and the fourth keeps `other.`. Compared as text, each of the first three was
// dropped and created again on every plan, and the policy that uses
// all_items() made that drop fail (stokaro/ptah#4038).
const routinesReturningQualifiedTypes = `CREATE TABLE public.items (id bigint PRIMARY KEY, title text);
CREATE SCHEMA other;
CREATE TABLE other.parts (id bigint PRIMARY KEY);
CREATE FUNCTION public.all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE FUNCTION public.first_item() RETURNS public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items LIMIT 1 $$;
CREATE FUNCTION public.item_rows() RETURNS TABLE (r public.items) LANGUAGE sql STABLE AS $$ SELECT i FROM public.items i $$;
CREATE FUNCTION public.all_parts() RETURNS SETOF other.parts LANGUAGE sql STABLE AS $$ SELECT * FROM other.parts $$;
CREATE TABLE public.notes (id bigint PRIMARY KEY, item_id bigint);
ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;
CREATE POLICY notes_of_items ON public.notes USING (item_id IN (SELECT id FROM public.all_items()));`

// routineResultControls change a result for real, so a comparison that
// dropped every qualifier would pass the synced tests and fail here. The body
// is the same on both sides and fits either type, so the result is the only
// difference. The second row moves the result to a type of the same name in
// another schema, which only the schema tells apart.
var routineResultControls = []struct {
	name       string
	migration  string
	schema     string
	wantInPlan string
}{
	{
		name: "the result moves to another table",
		migration: `CREATE TABLE public.items (id bigint PRIMARY KEY);
CREATE TABLE public.archived (id bigint PRIMARY KEY);
CREATE FUNCTION public.all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT 1::bigint WHERE false $$;`,
		schema: `CREATE TABLE public.items (id bigint PRIMARY KEY);
CREATE TABLE public.archived (id bigint PRIMARY KEY);
CREATE FUNCTION public.all_items() RETURNS SETOF public.archived LANGUAGE sql STABLE AS $$ SELECT 1::bigint WHERE false $$;`,
		wantInPlan: "RETURNS SETOF public.archived",
	},
	{
		name: "the result moves to a same-named table in another schema",
		migration: `CREATE TABLE public.parts (id bigint PRIMARY KEY);
CREATE SCHEMA other;
CREATE TABLE other.parts (id bigint PRIMARY KEY);
CREATE FUNCTION public.all_parts() RETURNS SETOF other.parts LANGUAGE sql STABLE AS $$ SELECT 1::bigint WHERE false $$;`,
		schema: `CREATE TABLE public.parts (id bigint PRIMARY KEY);
CREATE SCHEMA other;
CREATE TABLE other.parts (id bigint PRIMARY KEY);
CREATE FUNCTION public.all_parts() RETURNS SETOF public.parts LANGUAGE sql STABLE AS $$ SELECT 1::bigint WHERE false $$;`,
		wantInPlan: "RETURNS SETOF public.parts",
	},
}

// TestSchemaApplyFindsQualifiedRoutineResultsSyncedE2E plans the schema file
// against the database the same SQL built: nothing is planned.
func TestSchemaApplyFindsQualifiedRoutineResultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, routinesReturningQualifiedTypes, routinesReturningQualifiedTypes)
	target := databaseBuiltFrom(c, routinesReturningQualifiedTypes)

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestSchemaDiffFromADatabaseFindsQualifiedRoutineResultsSyncedE2E is the same
// comparison through schema diff, which asks the --from database.
func TestSchemaDiffFromADatabaseFindsQualifiedRoutineResultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, routinesReturningQualifiedTypes, routinesReturningQualifiedTypes)
	source := databaseBuiltFrom(c, routinesReturningQualifiedTypes)

	out := runPtahNative(c, "schema", "diff", "--from", source, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
}

// TestCompatSchemaApplyWithADevDatabaseFindsQualifiedRoutineResultsSyncedE2E
// is the command the report ran. With a plan to rehearse, the rehearsal on the
// dev database refused the drop of all_items(), which the policy depends on.
func TestCompatSchemaApplyWithADevDatabaseFindsQualifiedRoutineResultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, routinesReturningQualifiedTypes, routinesReturningQualifiedTypes)
	target := databaseBuiltFrom(c, routinesReturningQualifiedTypes)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("schema", "apply", "-u", target, "--to", "file://"+schema,
		"--dev-url", dev, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "DROP FUNCTION")
}

// TestMigrateDiffFindsQualifiedRoutineResultsSyncedE2E replays a directory
// whose one migration is the schema file and diffs: the directory is synced.
func TestMigrateDiffFindsQualifiedRoutineResultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	replayingRoutines(c)
	dir, schema := writeRewrittenMigrationProject(c, routinesReturningQualifiedTypes, routinesReturningQualifiedTypes)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}

// TestMigrationsGenerateReplayFindsQualifiedRoutineResultsSyncedE2E is the
// native replay: no migration is written. The replay reads the public schema
// unless --schemas names more, and the fixture keeps a table in other.
func TestMigrationsGenerateReplayFindsQualifiedRoutineResultsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	replayingRoutines(c)
	dir, schema := writeRewrittenMigrationProject(c, routinesReturningQualifiedTypes, routinesReturningQualifiedTypes)
	dev, _ := scratchReplayDatabase(c)

	out := runPtahNative(c, "migrations", "generate", "--schema-file", schema,
		"--migrations-dir", dir, "--replay", "--dev-url", dev, "--dir-format", "atlas", "--schemas", "public,other")

	c.Assert(out, qt.Contains, "no migration files generated")
}

// TestSchemaApplyPlansARoutineResultThatChangedE2E is the control for the
// schema apply row.
func TestSchemaApplyPlansARoutineResultThatChangedE2E(t *testing.T) {
	for _, test := range routineResultControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			target := databaseBuiltFrom(c, test.migration)

			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}

// TestMigrateDiffPlansARoutineResultThatChangedE2E is the control for the
// migrate diff row.
func TestMigrateDiffPlansARoutineResultThatChangedE2E(t *testing.T) {
	for _, test := range routineResultControls {
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

// routinesProbedOnYugabyteDB are routines whose argument list or result the
// comparison asks the server to spell, each naming a table's row type.
const routinesProbedOnYugabyteDB = `CREATE TABLE items (id bigint PRIMARY KEY, title text);
CREATE FUNCTION all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE FUNCTION item_rows() RETURNS TABLE (r public.items) LANGUAGE sql STABLE AS $$ SELECT i FROM public.items i $$;
CREATE FUNCTION item_title(i public.items) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT i.title $$;`

// TestSchemaDiffLeavesNoProbeRoutineOnYugabyteDBE2E compares on YugabyteDB,
// where a routine created in pg_temp outlives the rollback that undoes it on
// PostgreSQL, and outlives the session too. Left behind, a probe routine
// whose signature names items keeps anyone from dropping items. The probe
// drops the routine itself, so the database holds none afterwards and the
// table can be dropped.
func TestSchemaDiffLeavesNoProbeRoutineOnYugabyteDBE2E(t *testing.T) {
	c := qt.New(t)
	db := yugabyteDevDatabase(c)
	_, err := db.conn.ExecContext(c.Context(), routinesProbedOnYugabyteDB)
	c.Assert(err, qt.IsNil)
	_, schema := writeRewrittenMigrationProject(c, routinesProbedOnYugabyteDB, routinesProbedOnYugabyteDB)

	out := runPtahNative(c, "schema", "diff", "--from", db.url, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
	var probes int
	c.Assert(db.conn.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_proc WHERE proname LIKE 'ptah\_routine\_probe\_%'`).Scan(&probes), qt.IsNil)
	c.Assert(probes, qt.Equals, 0)
	_, err = db.conn.ExecContext(c.Context(), "DROP FUNCTION all_items(), item_rows(), item_title(items); DROP TABLE items")
	c.Assert(err, qt.IsNil)
}
