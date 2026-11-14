//go:build integration

package integration_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// writeExecutingStatementDirectory writes a migration directory whose second
// file fails at a statement spanning two lines with a double quote in it, and
// hashes it with the binary under test.
func writeExecutingStatementDirectory(c *qt.C, ctx context.Context, binaryPath, failing string) string {
	c.Helper()
	migrations := c.TempDir()
	writeLintE2EFile(c, migrations, "1.sql", "CREATE TABLE t1 (id int);\n")
	writeLintE2EFile(c, migrations, "2.sql", failing)
	_, stderr, err := runLintE2EBinary(ctx, binaryPath, "migrate", "hash", "--dir", "file://"+migrations)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	return migrations
}

// TestAtlasMigrateExecutingStatementE2E drives `ptah-compat migrate apply` and
// `migrate validate --dev-url` on SQLite to a statement the database refuses.
// Both print the community binary's one line, which names the statement as the
// file spells it and the version it belongs to. Measured on v1.3.0 with the
// same directory, each line differs from that binary's only in the driver's
// message, where its driver prints `no such table: nosuch` and Ptah's prints
// `SQL logic error: no such table: nosuch (1)`.
func TestAtlasMigrateExecutingStatementE2E(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	t.Cleanup(cancel)

	binaryPath := filepath.Join(t.TempDir(), "ptah-compat")
	buildPtahCompat(c, ctx, e2eRepoRoot(t), binaryPath)

	const failing = "CREATE TABLE t2 (id int);\n-- above\nINSERT INTO \"nosuch\"\n\tVALUES ('a\"b');\n"
	const line = `sql/migrate: executing statement "INSERT INTO \"nosuch\"\n\tVALUES ('a\"b');" from version "2": ` +
		`SQL logic error: no such table: nosuch (1)`

	t.Run("apply", func(t *testing.T) {
		c := qt.New(t)
		migrations := writeExecutingStatementDirectory(c, ctx, binaryPath, failing)

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "apply",
			"--dir", "file://"+migrations,
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "app.db"),
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "Error: "+line+"\n")
		c.Assert(stdout, qt.Contains, "Migrating to version 2")
	})

	t.Run("apply with a format", func(t *testing.T) {
		c := qt.New(t)
		migrations := writeExecutingStatementDirectory(c, ctx, binaryPath, failing)

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "apply",
			"--dir", "file://"+migrations,
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "app.db"),
			"--format", "{{ json .Error }}|{{ range .Applied }}{{ with .Error }}{{ json . }}{{ end }}{{ end }}",
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(stdout, qt.Equals,
			`"sql/migrate: executing statement \"INSERT INTO \\\"nosuch\\\"\\n\\tVALUES ('a\\\"b');\" from version \"2\": `+
				`SQL logic error: no such table: nosuch (1)"|`+
				`{"Stmt":"INSERT INTO \"nosuch\"\n\tVALUES ('a\"b');","Text":"SQL logic error: no such table: nosuch (1)"}`)
	})

	t.Run("validate", func(t *testing.T) {
		c := qt.New(t)
		migrations := writeExecutingStatementDirectory(c, ctx, binaryPath, failing)

		stdout, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "validate",
			"--dir", "file://"+migrations,
			"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals,
			"Error: replaying the migration directory: sql/migrate: read migration directory state: "+line+"\n")
	})

	// A statement the replay refuses before the database sees it was never
	// executed, so the line that says one was is not printed.
	t.Run("a statement refused before it ran keeps Ptah's reason", func(t *testing.T) {
		c := qt.New(t)
		migrations := writeExecutingStatementDirectory(c, ctx, binaryPath, "ATTACH DATABASE 'other.db' AS other;\n")

		_, stderr, err := runLintE2EBinary(ctx, binaryPath,
			"migrate", "validate",
			"--dir", "file://"+migrations,
			"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dev.db"),
		)

		c.Assert(exitStatusOf(c, err), qt.Equals, 1)
		c.Assert(stderr, qt.Matches, `(?s)Error: error validating migration SQL on dev database: replay migration 2 on dev database: .*rejects ATTACH.*`)
	})
}
