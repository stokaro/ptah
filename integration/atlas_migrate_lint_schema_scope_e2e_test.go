//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// withLintE2ESearchPath returns dbURL carrying a `search_path` query parameter,
// which is how a dev URL declares the one schema a lint run reviews.
func withLintE2ESearchPath(c *qt.C, dbURL, schema string) string {
	c.Helper()
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

// TestAtlasMigrateLintSchemaScopeE2E covers issue #1074 S1 against a live
// PostgreSQL dev database: which objects `migrate lint` puts under review.
//
// The dev database is what a lint run diffs against, so it also decides what is
// being analyzed. With `?search_path=public`, an object a migration creates and
// destroys in schema `app` was never in the before-state, so its destruction is
// not a covered change -- no diagnostic, no schema change, exit 0. Ptah used to
// report it and exit 1, which is stricter but means the two tools disagreed
// about what was even under review.
//
// The rows below are a 2x2 plus its boundary controls, and every axis is
// separated by a row that moves only that axis:
//
//   - target count -- the single-target `app` row answers the same as the
//     two-target one, which is what proves this is not the multi-target defect
//     closed by #1112;
//   - object kind -- the `app` DROP COLUMN row shows the boundary is the schema,
//     not the DROP TABLE grammar;
//   - schema -- the `public` rows must stay reported and keep exiting 1;
//   - qualification -- `public.users` written out in full is in scope, so the
//     filter reads the qualifier rather than merely preferring bare names;
//   - mixed statement -- one statement dropping `users` and `app.audit_log`
//     reports exactly one table and counts exactly one schema change, so the
//     filter is per object rather than per statement;
//   - the boundary itself -- the same `app` fixture through a dev URL with NO
//     `search_path` reports both drops and exits 1, which is the row that fails
//     if the filter ever stops depending on the dev URL.
//
// Verified against the pinned community binary v1.3.0 on PostgreSQL 16: every
// `want` below is that binary's report byte for byte, with only the two elapsed
// durations redacted, and every `wantExitOne` its exit status.
func TestAtlasMigrateLintSchemaScopeE2E(t *testing.T) {
	dbURL := requirePostgresE2EDatabaseURL(t)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	const appBase = "CREATE SCHEMA app;\nCREATE TABLE app.\"Users\" (id int);\nCREATE TABLE app.audit_log (id int);\n"
	const publicBase = "CREATE TABLE users (id int);\nCREATE TABLE audit_log (id int);\n"
	const silent = "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
		"\n" +
		"  -- analyzing version 2\n" +
		"    -- no diagnostics found\n" +
		"  -- ok (DUR)\n" +
		"\n" +
		"  -------------------------\n" +
		"  -- DUR\n" +
		"  -- 1 version ok\n"

	tests := []struct {
		name       string
		base       string
		next       string
		searchPath string
		// setupSQL runs on the fresh test database before the migrations do. Only
		// the row that reviews a schema other than public needs it: the dev
		// database has to already carry that schema for the snapshot to succeed.
		setupSQL    string
		wantExitOne bool
		want        string
	}{
		{
			name:       "two targets outside the reviewed schema",
			base:       appBase,
			next:       "DROP TABLE app.\"Users\", app.audit_log;\n",
			searchPath: "public",
			want:       silent,
		},
		{
			name:       "one target outside the reviewed schema",
			base:       appBase,
			next:       "DROP TABLE app.\"Users\";\n",
			searchPath: "public",
			want:       silent,
		},
		{
			name:       "a column dropped outside the reviewed schema",
			base:       "CREATE SCHEMA app;\nCREATE TABLE app.users (id int, nick text);\n",
			next:       "ALTER TABLE app.users DROP COLUMN nick;\n",
			searchPath: "public",
			want:       silent,
		},
		{
			name:       "objects created outside the reviewed schema count no change",
			base:       "CREATE TABLE seed (id int);\n",
			next:       "CREATE SCHEMA app2;\nCREATE TABLE app2.t (id int);\nCREATE TABLE keep (id int);\n",
			searchPath: "public",
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- no diagnostics found\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version ok\n" +
				"  -- 1 schema change\n",
		},
		{
			name:        "control: two targets inside the reviewed schema",
			base:        publicBase,
			next:        "DROP TABLE users, audit_log;\n",
			searchPath:  "public",
			wantExitOne: true,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"audit_log\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure table \"audit_log\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
		{
			name:        "control: one target inside the reviewed schema",
			base:        publicBase,
			next:        "DROP TABLE users;\n",
			searchPath:  "public",
			wantExitOne: true,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:        "control: targets qualified with the reviewed schema itself",
			base:        "CREATE TABLE public.users (id int);\nCREATE TABLE public.audit_log (id int);\n",
			next:        "DROP TABLE public.users, public.audit_log;\n",
			searchPath:  "public",
			wantExitOne: true,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"audit_log\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure table \"audit_log\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
		{
			name:        "one statement across both schemas reports only the in-scope table",
			base:        "CREATE SCHEMA app;\nCREATE TABLE users (id int);\nCREATE TABLE app.audit_log (id int);\n",
			next:        "DROP TABLE users, app.audit_log;\n",
			searchPath:  "public",
			wantExitOne: true,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:        "control: a dev URL naming no schema reviews the whole database",
			base:        appBase,
			next:        "DROP TABLE app.\"Users\", app.audit_log;\n",
			searchPath:  "",
			wantExitOne: true,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"Users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"audit_log\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure table \"Users\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"audit_log\" is empty before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
		{
			// The row that proves the scope is the URL's VALUE and not the
			// constant "public". Every other row reviews public, so substituting
			// that constant for whatever the URL names leaves them all green --
			// measured, that mutant survives the entire suite without this cell.
			//
			// Here the dev URL reviews `app` while the migration creates and drops
			// tables explicitly qualified as `public`, so the objects are OUT of
			// scope and the run is silent. Under the constant they would be in
			// scope and the run would report two DS102 and exit 1.
			//
			// The pinned community binary v1.3.0 answers exactly this: rc=0,
			// `-- no diagnostics found`, `-- 1 version ok`.
			name:       "the reviewed schema is the URL's value, not a constant",
			setupSQL:   "CREATE SCHEMA app;",
			base:       "CREATE TABLE public.\"Users\" (id int);\nCREATE TABLE public.audit_log (id int);\n",
			next:       "DROP TABLE public.\"Users\", public.audit_log;\n",
			searchPath: "app",
			want:       silent,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			testDBName := fmt.Sprintf("ptah_lint_scope_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)
			runLintE2ESetupSQL(c, ctx, dbURL, testDBName, test.setupSQL)

			migrationsDir := c.TempDir()
			writeLintE2EFile(c, migrationsDir, "1.sql", test.base)
			writeLintE2EFile(c, migrationsDir, "2.sql", test.next)

			devURL := replaceDatabaseName(c, dbURL, testDBName)
			assertLintE2EScopedReport(c, ctx, binaryPath, migrationsDir,
				scopedLintE2EDevURL(c, devURL, test.searchPath), test.wantExitOne, test.want)
		})
	}
}

// runLintE2ESetupSQL runs a row's setup statement on its own fresh database. A
// row that names no setup is a no-op, so the ordinary rows keep meeting an
// untouched database.
func runLintE2ESetupSQL(c *qt.C, ctx context.Context, dbURL, testDBName, statement string) {
	c.Helper()
	if statement == "" {
		return
	}
	db, err := sql.Open("pgx", replaceDatabaseName(c, dbURL, testDBName))
	c.Assert(err, qt.IsNil)
	defer db.Close()
	_, err = db.ExecContext(ctx, statement)
	c.Assert(err, qt.IsNil)
}

// scopedLintE2EDevURL applies the row's search_path, if it names one.
func scopedLintE2EDevURL(c *qt.C, devURL, searchPath string) string {
	c.Helper()
	scoped := map[bool]func() string{
		true:  func() string { return devURL },
		false: func() string { return withLintE2ESearchPath(c, devURL, searchPath) },
	}
	return scoped[searchPath == ""]()
}

// assertLintE2EScopedReport runs the compatibility binary and asserts its
// report and exit status together, so a matching report cannot hide a
// divergent exit code.
func assertLintE2EScopedReport(
	c *qt.C,
	ctx context.Context,
	binaryPath, migrationsDir, devURL string,
	wantExitOne bool,
	want string,
) {
	c.Helper()
	stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
		"migrate", "lint",
		"--dir", "file://"+migrationsDir,
		"--dev-url", devURL,
		"--latest", "1",
	)

	assertExit := map[bool]func(){
		true:  func() { c.Assert(err, qt.ErrorMatches, "exit status 1") },
		false: func() { c.Assert(err, qt.IsNil) },
	}
	assertExit[wantExitOne]()
	c.Assert(stderr, qt.Equals, "")
	c.Assert(redactLintE2EDurations(stdout), qt.Equals, want)
}

// TestNativeMigrationsLintIgnoresTheDevURLSchemaScopeE2E pins the half of the
// #1074 S1 decision that has no comparison binary: the schema boundary belongs
// to the Atlas-compatible surface only.
//
// The argument for scoping is that an object outside the dev URL's reach was
// never in the before-state the run compares against. That describes a
// diff-based analyzer. Ptah's native linter reads SQL text, which is why it
// reports TRUNCATE and DROP SCHEMA -- neither of which produces a diff. Scoping
// it turned two error-grade DS101 findings into "No lint findings." and exit 1
// into exit 0, silently, on the surface where no binary can be consulted about
// whether that is right.
//
// Removing the profile gate in migrationlintreport leaves every other test in
// the repository green, so without this row the split is unpinned.
func TestNativeMigrationsLintIgnoresTheDevURLSchemaScopeE2E(t *testing.T) {
	dbURL := requirePostgresE2EDatabaseURL(t)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, nativeBinary)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_lint_native_scope_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	migrationsDir := c.TempDir()
	writeLintE2EFile(c, migrationsDir, "1.sql",
		"CREATE SCHEMA app;\nCREATE TABLE app.\"Users\" (id int);\nCREATE TABLE app.audit_log (id int);\n")
	writeLintE2EFile(c, migrationsDir, "2.sql", "DROP TABLE app.\"Users\", app.audit_log;\n")

	devURL := withLintE2ESearchPath(c, replaceDatabaseName(c, dbURL, testDBName), "public")

	// The same directory and the same dev URL the compatibility surface answers
	// with "no diagnostics found" and exit 0.
	stdout, stderr, err := runLintE2EBinary(ctx, nativeBinary,
		"migrations", "lint",
		"--dir", migrationsDir,
		"--dir-format", "atlas",
		"--dev-url", devURL,
		"--latest", "1",
	)

	// The native report goes to stderr, so both streams are joined rather than
	// asserted on one: reading only stdout here made this test pass against an
	// empty string.
	report := stdout + stderr

	c.Assert(err, qt.ErrorMatches, "exit status 1")
	c.Assert(report, qt.Contains, `DROP TABLE permanently deletes table app."Users"`)
	c.Assert(report, qt.Contains, "DROP TABLE permanently deletes table app.audit_log")
	c.Assert(report, qt.Contains, "2 finding(s).")
	c.Assert(report, qt.Not(qt.Contains), "No lint findings.")
}
