//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestAtlasMigrateLintRenameAddSideE2E covers the last open scenario of
// stokaro/ptah#1074 against a live PostgreSQL dev database.
//
// A rename retires one name and introduces another. The retirement is reachable
// from the statement text and has been reported since #1120; the introduction is
// not, because the retired column's type, nullability and default live in an
// earlier migration file. Reaching it needs the dev database: the linter reads
// the schema state the version starts from mid-replay, while the retired column
// still exists.
//
// Every want below is the pinned community binary's output on the same fixture,
// byte for byte apart from the elapsed durations. That includes the `integer`
// spelling, which is the state's and not the statement's -- the migration says
// `int`.
//
// The rows are chosen so each one moves a single axis away from the reporting
// case, which is what makes them able to fail independently:
//
//   - nullable retired column: the add cannot fail on existing rows.
//   - retired column with a DEFAULT: same, for the other reason.
//   - table rename: retires a name but introduces no column, so no add side.
//   - same-file CREATE: the table is empty, so neither half is reportable.
//
// A rule that reported the add side of every rename would pass the first row and
// fail all four; a rule that reported none of them would fail only the first.
func TestAtlasMigrateLintRenameAddSideE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, repoRoot, binaryPath)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	tests := []struct {
		name     string
		base     string
		rename   string
		exitCode int
		want     string
	}{
		{
			name:     "not null column rename reports both halves",
			base:     "CREATE TABLE users (id int NOT NULL);\n",
			rename:   "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"integer\" column \"oid\" will fail in case table \"users\" is not\n" +
				"         empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 2 diagnostics\n",
		},
		{
			// The type spelling comes from the state, not the statement:
			// `varchar(20)` reads back as `character varying(20)`, which no
			// analyzer working from the migration text could produce.
			name:     "sized column rename carries the catalog type spelling",
			base:     "CREATE TABLE users (id varchar(20) NOT NULL);\n",
			rename:   "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"character varying(20)\" column \"oid\" will fail in case table\n" +
				"         \"users\" is not empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 2 diagnostics\n",
		},
		{
			// Two renames in one file: one diagnostic per introduced column, in
			// statement order, under one group heading, with the fix header
			// pluralized. A single-rename fixture cannot show any of that.
			name:   "two renames report one add side each",
			base:   "CREATE TABLE users (zeta int NOT NULL, alpha varchar(5) NOT NULL);\n",
			rename: "ALTER TABLE users RENAME COLUMN zeta TO zz;\nALTER TABLE users RENAME COLUMN alpha TO aa;\n",

			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"zeta\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"      -- L2: Dropping non-virtual column \"alpha\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"integer\" column \"zz\" will fail in case table \"users\" is not\n" +
				"         empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"      -- L2: Adding a non-nullable \"character varying(5)\" column \"aa\" will fail in case table\n" +
				"         \"users\" is not empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure column \"zeta\" is NULL before dropping it\n" +
				"      -> Add a pre-migration check to ensure column \"alpha\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 4 diagnostics\n",
		},
		{
			// A migration is free to qualify the table it alters. The scoped read
			// does not repeat the schema name on every table, so this is the row
			// that fails if the qualifier is compared literally.
			name:     "control: qualified table reference resolves",
			base:     "CREATE TABLE users (id int NOT NULL);\n",
			rename:   "ALTER TABLE public.users RENAME COLUMN id TO oid;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"integer\" column \"oid\" will fail in case table \"users\" is not\n" +
				"         empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 2 diagnostics\n",
		},
		{
			name:     "control: nullable retired column has no add side",
			base:     "CREATE TABLE users (id int);\n",
			rename:   "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:     "control: retired column with a default has no add side",
			base:     "CREATE TABLE users (id int NOT NULL DEFAULT 7);\n",
			rename:   "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			// The two halves of one rename belong to different analyzers, and each
			// selector silences exactly its own half. Measured on both tools: this
			// one leaves the add side reported and exits 0.
			name:     "control: the destructive selector silences only the retirement",
			base:     "CREATE TABLE users (id int NOT NULL);\n",
			rename:   "-- atlas:nolint destructive\nALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 0,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L2: Adding a non-nullable \"integer\" column \"oid\" will fail in case table \"users\" is not\n" +
				"         empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with warnings\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			// The mirror of the row above. An add side carrying a DS code would be
			// silenced by `destructive` and survive `data_depend`, which is exactly
			// the pair of results these two rows rule out.
			name:     "control: the data dependent selector silences only the add side",
			base:     "CREATE TABLE users (id int NOT NULL);\n",
			rename:   "-- atlas:nolint data_depend\nALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L2: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:     "control: table rename introduces no column",
			base:     "CREATE TABLE users (id int NOT NULL, nick varchar(20) NOT NULL);\n",
			rename:   "ALTER TABLE users RENAME TO accounts;\n",
			exitCode: 1,
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
				"  -- 2 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:     "control: rename of a column the same file created",
			base:     "CREATE TABLE seed (x int);\n",
			rename:   "CREATE TABLE users (id int NOT NULL);\nALTER TABLE users RENAME COLUMN id TO oid;\n",
			exitCode: 0,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- no diagnostics found\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 version ok\n" +
				"  -- 2 schema changes\n",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			testDBName := fmt.Sprintf("ptah_lint_rename_add_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

			migrationsDir := c.TempDir()
			writeLintE2EFile(c, migrationsDir, "1.sql", test.base)
			writeLintE2EFile(c, migrationsDir, "2.sql", test.rename)

			stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
				"migrate", "lint",
				"--dir", "file://"+migrationsDir,
				"--dev-url", replaceDatabaseName(c, dbURL, testDBName),
				"--latest", "1",
			)

			c.Assert(exitStatusOf(c, err), qt.Equals, test.exitCode)
			c.Assert(stderr, qt.Equals, "")
			c.Assert(redactLintE2EDurations(stdout), qt.Equals, test.want)
		})
	}
}

// TestNativeMigrationsLintRenameHasNoAddSideE2E pins the surface split for the
// half of #1074 that has no comparison binary.
//
// The compatibility surface says a rename "will fail in case table ... is not
// empty" because it models the rename as a drop plus an add. Native `ptah
// migrations lint` models it as a rename, and a rename does not fail on a
// populated table -- so the same claim there would be false of the statement the
// native analyzer describes. BC101 stays the single finding.
//
// Dropping the profile gate on the rename's add side leaves every other test in
// the repository green, so without this row the split is unpinned.
func TestNativeMigrationsLintRenameHasNoAddSideE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	repoRoot := e2eRepoRoot(t)
	nativeBinary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(c, ctx, repoRoot, nativeBinary)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	defer adminDB.Close()

	testDBName := fmt.Sprintf("ptah_lint_native_rename_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, testDBName)
	defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

	migrationsDir := c.TempDir()
	writeLintE2EFile(c, migrationsDir, "1.sql", "CREATE TABLE users (id int NOT NULL);\n")
	writeLintE2EFile(c, migrationsDir, "2.sql", "ALTER TABLE users RENAME COLUMN id TO oid;\n")

	// The same directory and the same dev database the compatibility surface
	// answers with two diagnostics.
	stdout, stderr, err := runLintE2EBinary(ctx, nativeBinary,
		"migrations", "lint",
		"--dir", migrationsDir,
		"--dir-format", "atlas",
		"--dev-url", replaceDatabaseName(c, dbURL, testDBName),
		"--latest", "1",
	)

	// The native report goes to stderr, so both streams are joined rather than
	// asserted on one: reading only stdout here made a neighboring test pass
	// against an empty string.
	report := stdout + stderr

	c.Assert(exitStatusOf(c, err), qt.Equals, 0)
	c.Assert(report, qt.Contains, "BC101: renames are not backwards compatible")
	c.Assert(report, qt.Contains, "1 finding(s).")
	c.Assert(report, qt.Not(qt.Contains), "without a DEFAULT")
}
