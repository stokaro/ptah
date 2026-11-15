package migratetests_test

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/migration/shadow"
)

// TestCompatCommand_MigrateDownPlanUnmatchedExclude_FailurePath mistypes the
// selector of an atlas.hcl env that keeps audit_events, a table another tool
// created after version 1. `migrate down --plan` forwards to the native verb,
// which refuses the selector because it protects nothing. The refusal names
// the command the operator typed rather than the native one, and the table
// and the column version 2 added are still there.
func TestCompatCommand_MigrateDownPlanUnmatchedExclude_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	migrationsDir := writeDownableMigrationsDir(c, dir)
	dbURL := "sqlite://" + filepath.Join(dir, "down.db")
	out, err := atlastest.RunCompatOutput("migrate", "apply", "--url", dbURL, "--dir", "file://"+migrationsDir)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE audit_events (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.hcl"), []byte("env \"local\" {\n  exclude = [\"audit_event\"]\n}\n"), 0o600), qt.IsNil)
	c.Chdir(dir)

	out, err = atlastest.RunCompatOutput("migrate", "down", "--env", "local", "--url", dbURL,
		"--dir", "file://"+migrationsDir, "--to-version", "20260801000001", "--plan",
		"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"))

	c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(`error running down migrations: `+
		`the exclude selection from atlas.hcl matched no objects: "audit_event"; `+
		`migrate down --plan refuses a selection that protects nothing, `+
		`set PTAH_ATLAS_ALLOW_UNMATCHED_EXCLUDE=1 to keep the permissive behavior`), qt.Commentf("%s", out))
	c.Assert(err, qt.ErrorIs, shadow.ErrUnmatchedExclude)
	var tables, emailColumns int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name IN ('audit_events', 'users')").Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 2)
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM pragma_table_info('users') WHERE name = 'email'").Scan(&emailColumns), qt.IsNil)
	c.Assert(emailColumns, qt.Equals, 1)
}
