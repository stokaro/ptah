//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// excludeUnmatchedSeed is the fixture every row below runs against. It holds one
// object of each kind the exclusion clones, in the connection's own schema, plus
// a table with a named column so a child selector has something real to name.
const excludeUnmatchedSeed = `
CREATE TABLE users (id serial PRIMARY KEY, name text NOT NULL);
CREATE INDEX users_name_idx ON users (name);
CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy');
CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0);
CREATE SEQUENCE order_seq;
CREATE TYPE addr AS (street text, city text);
CREATE TYPE intrange AS RANGE (subtype = integer);
`

// excludeUnmatchedCase is one --exclude selection and what the executing verb
// must do with it.
type excludeUnmatchedCase struct {
	name string
	// exclude is the selector list, one --exclude per element.
	exclude []string
	// accepted is true when `schema apply --dry-run` must exit 0 because every
	// selector named a real object.
	accepted bool
	// why records the measurement behind the row.
	why string
}

// TestAtlasCompatExcludeUnmatchedE2E pins that `schema apply` refuses an
// --exclude selector only when the filter actually ASKED it.
//
// The refusal added for stokaro/ptah#933 fired on selectors that name real
// objects, on a live PostgreSQL database, in two shapes:
//
//	--exclude users --exclude users.id
//	  exit 1: the --exclude selection matched no objects: "users.id"
//	  filterTables `continue`s on a table match, so filterColumns is never
//	  reached for an excluded table and the column pattern is never asked.
//
//	--exclude positive_int   (the domain exists)
//	  exit 1: the --exclude selection matched no objects: "positive_int"
//	  sequences, domains, composite types and range types were cloned into the
//	  filtered schema and never offered to a pattern, so no selector naming one
//	  could ever set a match mark.
//
// Both exit 0 on the pinned Atlas community binary v1.3.0 and on the tree before
// the refusal existed. This test runs against a live database because a filter
// unit test cannot see the introspection that fills these four collections.
//
// Every row also asserts the negative direction through the last two cases: a
// selector that names nothing must still refuse, or the fix would be "stop
// reporting" rather than "start asking".
func TestAtlasCompatExcludeUnmatchedE2E(t *testing.T) {
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []excludeUnmatchedCase{
		{
			name:     "a column of a table another selector removed",
			exclude:  []string{"users", "users.id"},
			accepted: true,
			why:      "both objects exist; the parent short-circuit skipped the column pass entirely",
		},
		{
			name:     "an index of a table another selector removed",
			exclude:  []string{"users", "users.users_name_idx"},
			accepted: true,
			why:      "the control for the reorder the child filters already carried",
		},
		{
			name:     "a domain",
			exclude:  []string{"positive_int"},
			accepted: true,
			why:      "the domain is in pg_type; the exclusion cloned domains without filtering them",
		},
		{
			name:     "a sequence",
			exclude:  []string{"order_seq"},
			accepted: true,
			why:      "same gap, sequence arm",
		},
		{
			name:     "a composite type",
			exclude:  []string{"addr"},
			accepted: true,
			why:      "same gap, composite arm",
		},
		{
			name:     "a range type",
			exclude:  []string{"intrange"},
			accepted: true,
			why:      "same gap, range arm",
		},
		{
			name:     "an enum, the kind that already worked",
			exclude:  []string{"mood"},
			accepted: true,
			why:      "control: a kind the filter always asked",
		},
		{
			name:     "a selector naming nothing still refuses",
			exclude:  []string{"nosuchobject_zzz"},
			accepted: false,
			why:      "the inverse mutant: the fix must be to ask, not to stop reporting",
		},
		{
			name:     "a column that does not exist still refuses",
			exclude:  []string{"users", "users.nosuchcolumn"},
			accepted: false,
			why:      "asking has to stay a name test rather than a blanket mark",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("pgx", adminURL)
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()

			stamp := time.Now().UnixNano()
			sourceName := fmt.Sprintf("ptah_excl_src_%d", stamp)
			devName := fmt.Sprintf("ptah_excl_dev_%d", stamp)
			createE2EDatabase(c.TB, ctx, adminDB, sourceName)
			defer dropE2EDatabase(c.TB, context.Background(), adminDB, sourceName)
			createE2EDatabase(c.TB, ctx, adminDB, devName)
			defer dropE2EDatabase(c.TB, context.Background(), adminDB, devName)

			sourceURL := replaceDatabaseName(c.TB, adminURL, sourceName)
			devURL := replaceDatabaseName(c.TB, adminURL, devName)
			seedExcludeUnmatchedDB(c.TB, ctx, sourceURL)

			err = runExcludeUnmatchedApply(c.TB, sourceURL, devURL, test.exclude)

			assertExcludeUnmatchedOutcome(c.TB, test, err)
		})
	}
}

// TestAtlasCompatExcludeProtectsATypeObjectE2E is the safety half. Reporting the
// selector correctly is not the point on its own: the selector is written to
// keep the object out of the plan, and before this change the plan dropped it.
//
// Measured before: `--exclude positive_int` with the permissive opt-in set still
// emitted `DROP DOMAIN IF EXISTS "positive_int" CASCADE`.
func TestAtlasCompatExcludeProtectsATypeObjectE2E(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	stamp := time.Now().UnixNano()
	sourceName := fmt.Sprintf("ptah_exclp_src_%d", stamp)
	devName := fmt.Sprintf("ptah_exclp_dev_%d", stamp)
	createE2EDatabase(c.TB, ctx, adminDB, sourceName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, sourceName)
	createE2EDatabase(c.TB, ctx, adminDB, devName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, devName)

	sourceURL := replaceDatabaseName(c.TB, adminURL, sourceName)
	devURL := replaceDatabaseName(c.TB, adminURL, devName)
	seedExcludeUnmatchedDB(c.TB, ctx, sourceURL)

	plan := runExcludeUnmatchedDiff(c.TB, sourceURL, devURL,
		[]string{"positive_int", "order_seq", "addr", "intrange"})

	c.Assert(plan, qt.Not(qt.Contains), `DROP DOMAIN IF EXISTS "positive_int"`)
	c.Assert(plan, qt.Not(qt.Contains), `DROP SEQUENCE IF EXISTS "order_seq"`)
	c.Assert(plan, qt.Not(qt.Contains), `DROP TYPE IF EXISTS "addr"`)
	c.Assert(plan, qt.Not(qt.Contains), `DROP TYPE IF EXISTS "intrange"`)
	// The control: an object no selector named is still planned for removal, so
	// the assertions above measure the exclusion rather than an empty plan.
	c.Assert(plan, qt.Contains, `DROP TYPE IF EXISTS "mood"`)
}

// excludeSchemaSeed adds a second schema holding objects of its own, so a
// selector naming that schema has both a schema entry and contents to reach.
const excludeSchemaSeed = `
CREATE SCHEMA app;
CREATE TABLE app.orders (id serial PRIMARY KEY, total integer);
CREATE TYPE app.color AS ENUM ('red', 'blue');
`

// TestAtlasCompatExcludeSchemaSelectorE2E is the live half of the last
// collection cloneDatabase copied without ever offering it to the patterns.
//
// Schemas were cloned, rendered as `schema "app"` by `schema inspect`, and never
// filtered. That made `--exclude app` two wrong things at once on a live
// database: `schema apply` exited 1 reporting that the selector matched no
// objects, and with the permissive opt-in set the plan still dropped
// `app.orders` and `app.color` -- the objects the selector was written to
// protect.
//
// Both halves are asserted here because they fail independently: marking the
// selector matched without filtering fixes the exit code and leaves the drops.
//
// Measured on the pinned Atlas community binary v1.3.0 against the same fixture
// and the same `-s public -s app` scope: `--exclude app` exits 0 and plans drops
// for the public objects only. Removing `--exclude app` from that run adds
// exactly one line, `DROP SCHEMA "app" CASCADE;`, which is what pins the
// selector to the schema rather than to something else in the command.
func TestAtlasCompatExcludeSchemaSelectorE2E(t *testing.T) {
	c := qt.New(t)
	adminURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	stamp := time.Now().UnixNano()
	sourceName := fmt.Sprintf("ptah_exclsch_src_%d", stamp)
	devName := fmt.Sprintf("ptah_exclsch_dev_%d", stamp)
	createE2EDatabase(c.TB, ctx, adminDB, sourceName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, sourceName)
	createE2EDatabase(c.TB, ctx, adminDB, devName)
	defer dropE2EDatabase(c.TB, context.Background(), adminDB, devName)

	sourceURL := replaceDatabaseName(c.TB, adminURL, sourceName)
	devURL := replaceDatabaseName(c.TB, adminURL, devName)
	seedExcludeUnmatchedDB(c.TB, ctx, sourceURL)
	seedExcludeSchemaDB(c.TB, ctx, sourceURL)

	scope := []string{"public", "app"}

	// The report half: the selector names a schema the description renders, so
	// the executing verb must not refuse it.
	c.Assert(runExcludeScopedApply(c.TB, sourceURL, devURL, scope, []string{"app"}), qt.IsNil)

	// The protection half: the plan must leave the excluded schema's objects
	// alone.
	plan := runExcludeScopedDiff(c.TB, sourceURL, devURL, scope, []string{"app"})
	c.Assert(plan, qt.Not(qt.Contains), `"app"."orders"`)
	c.Assert(plan, qt.Not(qt.Contains), `DROP TYPE IF EXISTS "color"`)
	// Controls in the same run: objects no selector named are still planned for
	// removal, so the assertions above measure the exclusion rather than an
	// empty plan.
	c.Assert(plan, qt.Contains, `DROP TABLE IF EXISTS "users"`)
	c.Assert(plan, qt.Contains, `DROP TYPE IF EXISTS "mood"`)

	// The negative direction, in the same scope: a selector naming no schema and
	// no object still refuses, so "ask the schemas" did not become "mark
	// everything".
	c.Assert(runExcludeScopedApply(c.TB, sourceURL, devURL, scope, []string{"nosuchschema_zzz"}),
		qt.ErrorMatches, `the --exclude selection matched no objects:.*`)
}

func seedExcludeSchemaDB(tb testing.TB, ctx context.Context, dbURL string) {
	c := qt.New(tb)
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, excludeSchemaSeed)
	c.Assert(err, qt.IsNil)

	var objects int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT count(*)
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = 'app' AND c.relname = 'orders'`,
	).Scan(&objects), qt.IsNil)
	c.Assert(objects, qt.Equals, 1,
		qt.Commentf("the seed did not create app.orders, so the protection assertions measure nothing"))
}

func seedExcludeUnmatchedDB(tb testing.TB, ctx context.Context, dbURL string) {
	c := qt.New(tb)
	c.Helper()

	db, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, err = db.ExecContext(ctx, excludeUnmatchedSeed)
	c.Assert(err, qt.IsNil)

	var kinds int
	c.Assert(db.QueryRowContext(ctx, `
		SELECT count(DISTINCT t.typtype)
		  FROM pg_type t
		  JOIN pg_namespace n ON n.oid = t.typnamespace
		 WHERE n.nspname = 'public'
		   AND t.typname IN ('positive_int', 'addr', 'intrange', 'mood')`,
	).Scan(&kinds), qt.IsNil)
	c.Assert(kinds, qt.Equals, 4,
		qt.Commentf("the seed did not create a domain, composite, range and enum, so the rows assert nothing"))
}

// runExcludeUnmatchedApply runs `schema apply --dry-run` against an empty
// desired schema and returns the command's error, which is the exit status the
// blocker is about.
func runExcludeUnmatchedApply(tb testing.TB, sourceURL, devURL string, exclude []string) error {
	c := qt.New(tb)
	c.Helper()

	return runExcludeScopedApply(c.TB, sourceURL, devURL, []string{"public"}, exclude)
}

// runExcludeScopedApply is runExcludeUnmatchedApply with the schema universe
// spelled out, so a row can run in the multi-schema scope where a one-part
// selector names a schema.
func runExcludeScopedApply(tb testing.TB, sourceURL, devURL string, schemas, exclude []string) error {
	c := qt.New(tb)
	c.Helper()

	args := []string{
		"schema", "apply",
		"--url", sourceURL,
		"--to", "file://" + writeExcludeUnmatchedEmptySchema(c.TB),
		"--dev-url", devURL,
		"--dry-run",
	}
	args = append(args, schemaArgs(schemas)...)
	args = append(args, excludeArgs(exclude)...)

	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	return cmd.Execute()
}

// runExcludeUnmatchedDiff returns the planned statements for the same
// comparison, so the exclusion can be measured on what it protects rather than
// only on what it reports.
func runExcludeUnmatchedDiff(tb testing.TB, sourceURL, devURL string, exclude []string) string {
	c := qt.New(tb)
	c.Helper()

	return runExcludeScopedDiff(c.TB, sourceURL, devURL, []string{"public"}, exclude)
}

// runExcludeScopedDiff is runExcludeUnmatchedDiff with the schema universe
// spelled out.
func runExcludeScopedDiff(tb testing.TB, sourceURL, devURL string, schemas, exclude []string) string {
	c := qt.New(tb)
	c.Helper()

	args := []string{
		"schema", "diff",
		"--from", sourceURL,
		"--to", "file://" + writeExcludeUnmatchedEmptySchema(c.TB),
		"--dev-url", devURL,
	}
	args = append(args, schemaArgs(schemas)...)
	args = append(args, excludeArgs(exclude)...)

	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", errOut.String()))

	return out.String()
}

func writeExcludeUnmatchedEmptySchema(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()

	path := filepath.Join(c.TempDir(), "empty.sql")
	c.Assert(os.WriteFile(path, []byte("-- intentionally empty\n"), 0o600), qt.IsNil)
	return path
}

// excludeArgs expands one --exclude per selector, keeping the caller's test body
// free of branching.
func excludeArgs(exclude []string) []string {
	args := make([]string, 0, 2*len(exclude))
	for _, selector := range exclude {
		args = append(args, "--exclude", selector)
	}
	return args
}

// schemaArgs expands one --schema per name, the flag spelling that puts
// ptah-compat in the multi-schema scope.
func schemaArgs(schemas []string) []string {
	args := make([]string, 0, 2*len(schemas))
	for _, schema := range schemas {
		args = append(args, "--schema", schema)
	}
	return args
}

// assertExcludeUnmatchedOutcome turns the row's expectation into the assertion,
// so the loop body carries no branch.
func assertExcludeUnmatchedOutcome(tb testing.TB, test excludeUnmatchedCase, err error) {
	c := qt.New(tb)
	c.Helper()

	if test.accepted {
		c.Assert(err, qt.IsNil, qt.Commentf("%s", test.why))
		return
	}
	c.Assert(err, qt.ErrorMatches, `the --exclude selection matched no objects:.*`,
		qt.Commentf("%s", test.why))
}
