//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// viewsOverFunctions are views that read a function in FROM. PostgreSQL stores
// each with the function's whole result as a column alias list -- `FROM
// all_items()` reads back as `FROM all_items() all_items(id, title, tags)` --
// and compared as text every one was dropped with CASCADE and created again on
// every plan (stokaro/ptah#4057). The set covers a qualified call, an
// unqualified one, an alias, a function in another schema, a scalar
// set-returning function, LATERAL, and a materialized view.
const viewsOverFunctions = `CREATE TABLE public.items (id bigint PRIMARY KEY, title text, tags text[]);
CREATE SCHEMA other;
CREATE FUNCTION public.all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE FUNCTION other.more_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE VIEW public.item_list AS SELECT id, title FROM public.all_items();
CREATE VIEW public.item_list2 AS SELECT id, title FROM all_items();
CREATE VIEW public.item_list3 AS SELECT i.id, i.title FROM all_items() i;
CREATE VIEW public.more_list AS SELECT id FROM other.more_items();
CREATE VIEW public.numbers AS SELECT g FROM generate_series(1, 3) g;
CREATE VIEW public.item_tags AS SELECT items.id, t FROM public.items, LATERAL unnest(items.tags) t;
CREATE MATERIALIZED VIEW public.item_snapshot AS SELECT id FROM public.all_items() WHERE title IS NOT NULL;`

// viewOverFunctionControls change a view for real. The first changes the
// select list of a view over a function. The second adds a column to a table a
// view reads through `*`: the server would spell that star with the columns
// the database has now, so a comparison that asked it would match and miss the
// column the plan adds.
var viewOverFunctionControls = []struct {
	name       string
	migration  string
	schema     string
	wantInPlan string
}{
	{
		name: "the select list of a view over a function changes",
		migration: `CREATE TABLE public.items (id bigint PRIMARY KEY, title text);
CREATE FUNCTION public.all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE VIEW public.item_list AS SELECT id FROM public.all_items();`,
		schema: `CREATE TABLE public.items (id bigint PRIMARY KEY, title text);
CREATE FUNCTION public.all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE VIEW public.item_list AS SELECT id, title FROM public.all_items();`,
		wantInPlan: "SELECT id, title FROM public.all_items()",
	},
	{
		name: "a table a star view reads gains a column",
		migration: `CREATE TABLE public.items (id bigint PRIMARY KEY);
CREATE VIEW public.all_rows AS SELECT * FROM public.items;`,
		schema: `CREATE TABLE public.items (id bigint PRIMARY KEY, title text);
CREATE VIEW public.all_rows AS SELECT * FROM public.items;`,
		wantInPlan: `"all_rows"`,
	},
}

// TestSchemaApplyFindsViewsOverFunctionsSyncedE2E plans the schema file against
// the database the same SQL built: nothing is planned.
func TestSchemaApplyFindsViewsOverFunctionsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, viewsOverFunctions, viewsOverFunctions)
	target := databaseBuiltFrom(c, viewsOverFunctions)

	out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

	c.Assert(out, qt.Contains, "Schema is synced")
}

// TestSchemaDiffFromADatabaseFindsViewsOverFunctionsSyncedE2E is the same
// comparison through schema diff, which asks the --from database.
func TestSchemaDiffFromADatabaseFindsViewsOverFunctionsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, viewsOverFunctions, viewsOverFunctions)
	source := databaseBuiltFrom(c, viewsOverFunctions)

	out := runPtahNative(c, "schema", "diff", "--from", source, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
}

// TestCompatSchemaApplyWithADevDatabaseFindsViewsOverFunctionsSyncedE2E is the
// command the report ran: no view is dropped.
func TestCompatSchemaApplyWithADevDatabaseFindsViewsOverFunctionsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	_, schema := writeRewrittenMigrationProject(c, viewsOverFunctions, viewsOverFunctions)
	target := databaseBuiltFrom(c, viewsOverFunctions)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("schema", "apply", "-u", target, "--to", "file://"+schema,
		"--dev-url", dev, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Not(qt.Contains), "DROP VIEW")
	c.Assert(out, qt.Not(qt.Contains), "DROP MATERIALIZED VIEW")
}

// TestMigrateDiffFindsViewsOverFunctionsSyncedE2E replays a directory whose one
// migration is the schema file and diffs: the directory is synced.
func TestMigrateDiffFindsViewsOverFunctionsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	replayingRoutines(c)
	dir, schema := writeRewrittenMigrationProject(c, viewsOverFunctions, viewsOverFunctions)
	dev, _ := scratchReplayDatabase(c)

	out, err := runCompatVerb("migrate", "diff", "--dir", "file://"+dir,
		"--to", "file://"+schema, "--dev-url", dev, "--dry-run")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains, "The migration directory is synced with the desired state")
}

// TestMigrationsGenerateReplayFindsViewsOverFunctionsSyncedE2E is the native
// replay: no migration is written. The replay reads the public schema unless
// --schemas names more, and the fixture keeps a function in other.
func TestMigrationsGenerateReplayFindsViewsOverFunctionsSyncedE2E(t *testing.T) {
	c := qt.New(t)
	replayingRoutines(c)
	dir, schema := writeRewrittenMigrationProject(c, viewsOverFunctions, viewsOverFunctions)
	dev, _ := scratchReplayDatabase(c)

	out := runPtahNative(c, "migrations", "generate", "--schema-file", schema,
		"--migrations-dir", dir, "--replay", "--dev-url", dev, "--dir-format", "atlas", "--schemas", "public,other")

	c.Assert(out, qt.Contains, "no migration files generated")
}

// TestSchemaApplyPlansAViewThatChangedE2E is the control for the schema apply
// row.
func TestSchemaApplyPlansAViewThatChangedE2E(t *testing.T) {
	for _, test := range viewOverFunctionControls {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, schema := writeRewrittenMigrationProject(c, test.migration, test.schema)
			target := databaseBuiltFrom(c, test.migration)

			out := runPtahNative(c, "schema", "apply", "--db-url", target, "--schema-file", schema, "--dry-run")

			c.Assert(out, qt.Contains, test.wantInPlan)
		})
	}
}

// TestMigrateDiffPlansAViewThatChangedE2E is the control for the migrate diff
// row.
func TestMigrateDiffPlansAViewThatChangedE2E(t *testing.T) {
	for _, test := range viewOverFunctionControls {
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

// viewsOverFunctionsOnYugabyteDB is the fixture's core on YugabyteDB.
const viewsOverFunctionsOnYugabyteDB = `CREATE TABLE items (id bigint PRIMARY KEY, title text);
CREATE FUNCTION all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE VIEW item_list AS SELECT id, title FROM public.all_items();
CREATE VIEW numbers AS SELECT g FROM generate_series(1, 3) g;
CREATE MATERIALIZED VIEW item_snapshot AS SELECT id FROM public.all_items() WHERE title IS NOT NULL;`

// TestSchemaDiffLeavesNoProbeViewOnYugabyteDBE2E compares on YugabyteDB, where
// a view created in pg_temp outlives the rollback that undoes it on PostgreSQL,
// and outlives the session too. The views match, and the database holds no
// probe view afterwards.
func TestSchemaDiffLeavesNoProbeViewOnYugabyteDBE2E(t *testing.T) {
	c := qt.New(t)
	db := yugabyteDevDatabase(c)
	_, err := db.conn.ExecContext(c.Context(), viewsOverFunctionsOnYugabyteDB)
	c.Assert(err, qt.IsNil)
	_, schema := writeRewrittenMigrationProject(c, viewsOverFunctionsOnYugabyteDB, viewsOverFunctionsOnYugabyteDB)

	out := runPtahNative(c, "schema", "diff", "--from", db.url, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "Schemas are synced")
	var probes int
	c.Assert(db.conn.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'ptah\_view\_probe\_%'`).Scan(&probes), qt.IsNil)
	c.Assert(probes, qt.Equals, 0)
}

// viewsOverFunctionsOnCockroachDB is a CockroachDB fixture. v26.3.2 stores the
// function call as `ROWS FROM (public.all_items())` and a table as
// `<database>.public.items`, so both views were planned again on every diff.
const viewsOverFunctionsOnCockroachDB = `CREATE TABLE items (id INT8 PRIMARY KEY, title STRING);
CREATE FUNCTION all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE VIEW item_list AS SELECT id, title FROM public.all_items();
CREATE VIEW recent AS SELECT id, title FROM items WHERE id > 10;`

// TestSchemaDiffDoesNotReplanViewsOnCockroachDBE2E compares on CockroachDB,
// which refuses temporary views, so the probe view is created in the current
// schema inside the rolled-back transaction. Neither view is planned, and
// the database holds no probe view afterwards. The fixture's function and
// column are planned for reasons outside this comparison, so the assertion
// reads the views alone.
func TestSchemaDiffDoesNotReplanViewsOnCockroachDBE2E(t *testing.T) {
	c := qt.New(t)
	db := cockroachDevDatabase(c)
	_, err := db.conn.ExecContext(c.Context(), viewsOverFunctionsOnCockroachDB)
	c.Assert(err, qt.IsNil)
	_, schema := writeRewrittenMigrationProject(c, viewsOverFunctionsOnCockroachDB, viewsOverFunctionsOnCockroachDB)

	out := runPtahNative(c, "schema", "diff", "--from", db.url, "--to", "file://"+schema)

	c.Assert(out, qt.Not(qt.Contains), "DROP VIEW")
	c.Assert(out, qt.Not(qt.Contains), "CREATE VIEW")
	var probes int
	c.Assert(db.conn.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_class WHERE relname LIKE 'ptah\_view\_probe\_%'`).Scan(&probes), qt.IsNil)
	c.Assert(probes, qt.Equals, 0)
}

// TestSchemaDiffPlansAViewThatChangedOnCockroachDBE2E is the CockroachDB
// control: a select list that changes is planned.
func TestSchemaDiffPlansAViewThatChangedOnCockroachDBE2E(t *testing.T) {
	c := qt.New(t)
	db := cockroachDevDatabase(c)
	_, err := db.conn.ExecContext(c.Context(), viewsOverFunctionsOnCockroachDB)
	c.Assert(err, qt.IsNil)
	changed := `CREATE TABLE items (id INT8 PRIMARY KEY, title STRING);
CREATE FUNCTION all_items() RETURNS SETOF public.items LANGUAGE sql STABLE AS $$ SELECT * FROM public.items $$;
CREATE VIEW item_list AS SELECT id FROM public.all_items();
CREATE VIEW recent AS SELECT id, title FROM items WHERE id > 10;`
	_, schema := writeRewrittenMigrationProject(c, viewsOverFunctionsOnCockroachDB, changed)

	out := runPtahNative(c, "schema", "diff", "--from", db.url, "--to", "file://"+schema)

	c.Assert(out, qt.Contains, "SELECT id FROM public.all_items()")
	c.Assert(out, qt.Not(qt.Contains), `"recent"`)
}
