//go:build integration

package integration_test

import (
	"context"
	"maps"
	"path/filepath"
	"slices"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// applyTextReportRow is one `ptah-compat migrate apply` run without --format
// over a fresh SQLite database.
type applyTextReportRow struct {
	name   string
	files  map[string]string
	format string
	args   []string
	// wantStdout is the report with each duration written as DUR.
	wantStdout string
	wantStderr string
}

// runApplyTextReportRow writes the row's directory, hashes it with the binary
// under test and applies it, returning stdout with its durations redacted.
func runApplyTextReportRow(c *qt.C, ctx context.Context, binaryPath string, row applyTextReportRow) (stdout, stderr string, code int) {
	c.Helper()
	migrations := c.TempDir()
	for _, name := range slices.Sorted(maps.Keys(row.files)) {
		writeLintE2EFile(c, migrations, name, row.files[name])
	}
	dirURL := "file://" + migrations + row.format
	_, hashErr, err := runLintE2EBinary(ctx, binaryPath, "migrate", "hash", "--dir", dirURL)
	c.Assert(err, qt.IsNil, qt.Commentf("migrate hash: %s", hashErr))
	args := append([]string{
		"migrate", "apply",
		"--dir", dirURL,
		"--url", "sqlite://" + filepath.Join(c.TempDir(), "app.db"),
	}, row.args...)
	stdout, stderr, err = runLintE2EBinary(ctx, binaryPath, args...)
	return redactLintE2EDurations(stdout), stderr, exitStatusOf(c, err)
}

// TestAtlasMigrateApplyTextReportE2E_HappyPath drives the built binary over
// directories the pinned community binary v1.3.0 was measured on, on SQLite.
// Each report is that binary's byte for byte once the durations are redacted:
// every migration with the statements it ran, and a summary.
func TestAtlasMigrateApplyTextReportE2E_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)

	tests := []applyTextReportRow{
		{
			name: "every migration, one statement over several lines",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n",
				"2.sql": "ALTER TABLE users ADD COLUMN name text;\nCREATE INDEX users_name ON users (name);\n",
				"3.sql": "-- a comment above\nCREATE TABLE t3 (\n  id int,\n  v text\n);\nINSERT INTO t3 VALUES (1, 'a;b');\n",
			},
			wantStdout: "Migrating to version 3 (3 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> ALTER TABLE users ADD COLUMN name text;\n" +
				"    -> CREATE INDEX users_name ON users (name);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 3\n" +
				"    -> CREATE TABLE t3 (\n" +
				"         id int,\n" +
				"         v text\n" +
				"       );\n" +
				"    -> INSERT INTO t3 VALUES (1, 'a;b');\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 3 migrations\n" +
				"  -- 5 sql statements\n",
		},
		{
			name: "a dry run",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n",
				"2.sql": "ALTER TABLE users ADD COLUMN name text;\nCREATE INDEX users_name ON users (name);\n",
			},
			args: []string{"--dry-run"},
			wantStdout: "Migrating to version 2 (2 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> ALTER TABLE users ADD COLUMN name text;\n" +
				"    -> CREATE INDEX users_name ON users (name);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 2 migrations\n" +
				"  -- 3 sql statements\n",
		},
		{
			name: "a file outside a transaction",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n",
				"2.sql": "-- atlas:txmode none\n\nCREATE TABLE t2 (id int);\nVACUUM;\n",
			},
			wantStdout: "Migrating to version 2 (2 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> CREATE TABLE t2 (id int);\n" +
				"    -> VACUUM;\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 2 migrations\n" +
				"  -- 3 sql statements\n",
		},
		{
			name: "a Flyway directory names its own versions",
			files: map[string]string{
				"V1__init.sql":  "CREATE TABLE users (id int);\n",
				"V1.5__b_c.sql": "CREATE TABLE b (id int);\nCREATE TABLE b2 (id int);\n",
				"V2__two.sql":   "CREATE TABLE t2 (id int);\n",
			},
			format: "?format=flyway",
			wantStdout: "Migrating to version 2 (3 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 1.5\n" +
				"    -> CREATE TABLE b (id int);\n" +
				"    -> CREATE TABLE b2 (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> CREATE TABLE t2 (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 3 migrations\n" +
				"  -- 4 sql statements\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runApplyTextReportRow(c, ctx, binaryPath, test)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stderr: %s", stderr))
			c.Assert(stderr, qt.Equals, test.wantStderr)
			c.Assert(stdout, qt.Equals, test.wantStdout)
		})
	}

	t.Run("nothing pending", func(t *testing.T) {
		c := qt.New(t)
		migrations := c.TempDir()
		writeLintE2EFile(c, migrations, "1.sql", "CREATE TABLE users (id int);\n")
		_, hashErr, err := runLintE2EBinary(ctx, binaryPath, "migrate", "hash", "--dir", "file://"+migrations)
		c.Assert(err, qt.IsNil, qt.Commentf("migrate hash: %s", hashErr))
		args := []string{"migrate", "apply", "--dir", "file://" + migrations, "--url", "sqlite://" + filepath.Join(c.TempDir(), "app.db")}
		_, firstErr, err := runLintE2EBinary(ctx, binaryPath, args...)
		c.Assert(err, qt.IsNil, qt.Commentf("first apply: %s", firstErr))

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath, args...)

		c.Assert(exitStatusOf(c, err), qt.Equals, 0)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Equals, "No migration files to execute\n")
	})
}

// TestAtlasMigrateApplyTextReportE2E_FailurePath drives a run the database
// refuses. The report goes to stdout and the error to stderr, as the pinned
// community binary writes them; both differ from that binary's only in the
// SQLite driver's wording, where its driver prints `no such table: nosuch`.
func TestAtlasMigrateApplyTextReportE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)
	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)

	tests := []applyTextReportRow{
		{
			name: "a statement in the middle of a file",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n",
				"2.sql": "ALTER TABLE users ADD COLUMN name text;\nINSERT INTO nosuch VALUES (1);\nCREATE TABLE after (id int);\n",
				"3.sql": "CREATE TABLE t3 (id int);\n",
			},
			wantStdout: "Migrating to version 3 (3 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 2\n" +
				"    -> ALTER TABLE users ADD COLUMN name text;\n" +
				"    -> INSERT INTO nosuch VALUES (1);\n" +
				"    SQL logic error: no such table: nosuch (1)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 migration ok, 1 with errors\n" +
				"  -- 2 sql statements ok, 1 with errors\n",
			wantStderr: `Error: sql/migrate: executing statement "INSERT INTO nosuch VALUES (1);" from version "2": ` +
				"SQL logic error: no such table: nosuch (1)\n",
		},
		{
			name: "the first statement of a Flyway file",
			files: map[string]string{
				"V1__init.sql":  "CREATE TABLE users (id int);\n",
				"V1.5__b_c.sql": "INSERT INTO nosuch VALUES (1);\nCREATE TABLE b (id int);\n",
			},
			format: "?format=flyway",
			wantStdout: "Migrating to version 1.5 (2 migrations in total):\n" +
				"\n" +
				"  -- migrating version 1\n" +
				"    -> CREATE TABLE users (id int);\n" +
				"  -- ok (DUR)\n" +
				"\n" +
				"  -- migrating version 1.5\n" +
				"    -> INSERT INTO nosuch VALUES (1);\n" +
				"    SQL logic error: no such table: nosuch (1)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- DUR\n" +
				"  -- 1 migration ok, 1 with errors\n" +
				"  -- 1 sql statement ok, 1 with errors\n",
			wantStderr: `Error: sql/migrate: executing statement "INSERT INTO nosuch VALUES (1);" from version "1.5": ` +
				"SQL logic error: no such table: nosuch (1)\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runApplyTextReportRow(c, ctx, binaryPath, test)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stderr, qt.Equals, test.wantStderr)
			c.Assert(stdout, qt.Equals, test.wantStdout)
		})
	}
}
