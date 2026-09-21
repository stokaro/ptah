//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// A lint policy that declares a server version Ptah has not measured plans
// against the nearest release line below it. The apply still runs its gate,
// and an apply that said nothing would leave an operator unable to tell "the
// gate passed on your server" from "the gate passed on a release nobody
// named" (stokaro/ptah#3420).
func TestMigrationsUpReportsThePolicyVersionFallbackE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { adminDB.Close() })
	databaseName := fmt.Sprintf("ptah_lint_note_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, databaseName)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, databaseName) })
	scopedURL := replaceDatabaseName(c, dbURL, databaseName)

	workDir := c.TempDir()
	migrations := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	writeLintNoteFixture(c, migrations, "server-version: \"99\"\n")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
		"migrations", "up", "--migrations-dir", migrations, "--db-url", scopedURL)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	c.Assert(applied.Stdout+applied.Stderr, qt.Contains, "Migration lint policy:")
}

// The control. A policy that names a measured release line resolves onto it
// exactly, so there is nothing to report; without this row an apply that
// printed the note unconditionally would satisfy the test above.
func TestMigrationsUpReportsNoNoteForAMeasuredVersionE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() { adminDB.Close() })
	databaseName := fmt.Sprintf("ptah_lint_nonote_e2e_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, databaseName)
	t.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, databaseName) })
	scopedURL := replaceDatabaseName(c, dbURL, databaseName)

	workDir := c.TempDir()
	migrations := filepath.Join(workDir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o750), qt.IsNil)
	writeLintNoteFixture(c, migrations, "server-version: \"16\"\n")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: workDir},
		"migrations", "up", "--migrations-dir", migrations, "--db-url", scopedURL)

	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))
	c.Assert(applied.Stdout+applied.Stderr, qt.Not(qt.Contains), "Migration lint policy:")
}

// writeLintNoteFixture writes one additive migration and a lint policy whose
// version line is the only thing that varies between the two runs.
func writeLintNoteFixture(c *qt.C, migrations, versionLine string) {
	c.Helper()
	c.Assert(os.WriteFile(
		filepath.Join(migrations, "0000000001_create_notes.up.sql"),
		[]byte("CREATE TABLE ptah_lint_notes (id BIGINT PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrations, "0000000001_create_notes.down.sql"),
		[]byte("DROP TABLE ptah_lint_notes;\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrations, ".ptah-lint.yaml"),
		[]byte("dialect: postgres\n"+versionLine), 0o600), qt.IsNil)
}
