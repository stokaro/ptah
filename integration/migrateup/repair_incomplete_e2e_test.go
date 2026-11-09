//go:build integration

package migrateup_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// A metadata-only repair cannot turn a partial up run into an applied revision
// while the rest of its migration has not run.
func TestPlainRepairRefusesUnrunSuffixE2E_FailurePath(t *testing.T) {
	c := qt.New(t)
	address := "sqlite://" + filepath.Join(c.TempDir(), "repair.db")
	dir := migrationsDir(c, "DROP TABLE users;\n",
		"CREATE TABLE users (id INTEGER PRIMARY KEY);\nINSERT INTO absent (id) VALUES (1);\n")
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "up", "--db-url", address, "--migrations-dir", dir, "--tx-mode", "none")
	c.Assert(applied.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

	repair := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"migrations", "repair", "--db-url", address, "--migrations-dir", dir, "--version", "1")
	c.Assert(repair.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", repair.Stdout, repair.Stderr))
	c.Assert(repair.Stderr, qt.Contains, "recorded 1 of 2 applied statements")
	c.Assert(repair.Stderr, qt.Contains, "--resume-from 2")

	conn, err := dbschema.ConnectToDatabase(c.Context(), address)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	var state string
	var completed, total int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT state, applied, total FROM schema_migrations WHERE version = 1").
		Scan(&state, &completed, &total), qt.IsNil)
	c.Assert(state, qt.Equals, "failed")
	c.Assert(completed, qt.Equals, 1)
	c.Assert(total, qt.Equals, 2)
}
