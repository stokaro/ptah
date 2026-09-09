//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// The migration lifecycle, driven through the shipped binary.
//
// These verbs had no end-to-end coverage at all: `create`, `ls`, `show` and
// `status` appeared nowhere in the integration tree, and `up` and `down` were
// only ever driven in process (stokaro/ptah#3151). Their packages have unit
// tests, so what was missing is the assembled article -- flag parsing, the exit
// code, which stream carries which sentence, and the files a run leaves on
// disk.
//
// SQLite is the engine here because it is compiled in: the lifecycle is the
// subject, and a server would add a failure mode that has nothing to do with
// it.

const lifecycleMigration = `CREATE TABLE widgets (id INTEGER PRIMARY KEY, name TEXT NOT NULL);`

// TestMigrationsLifecycleE2E walks the route an operator actually walks.
//
// One test rather than six, because the steps are not independent: `status`
// after `up` is only meaningful on the database `up` wrote, and `down` has
// nothing to undo without it. Splitting them would mean rebuilding the same
// state three times and asserting on a fixture rather than on a sequence.
func TestMigrationsLifecycleE2E(t *testing.T) {
	c := qt.New(t)

	dir := c.TempDir()
	migrations := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrations, 0o755), qt.IsNil)
	databaseURL := "sqlite://" + filepath.ToSlash(filepath.Join(dir, "lifecycle.db"))

	created := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "create", "--migrations-dir", migrations, "--name", "add_widgets")
	c.Assert(created.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", created.Stderr))

	// `create` writes an empty pair for the operator to fill in, so the SQL is
	// this test's to supply. Globbing rather than reading the directory keeps
	// the test declarative: which file is which is in the name the command
	// chose, not in a branch here.
	ups, err := filepath.Glob(filepath.Join(migrations, "*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(ups, qt.HasLen, 1, qt.Commentf("create wrote:\n%s", created.Stdout))
	downs, err := filepath.Glob(filepath.Join(migrations, "*.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(downs, qt.HasLen, 1, qt.Commentf("create wrote:\n%s", created.Stdout))

	c.Assert(os.WriteFile(ups[0], []byte(lifecycleMigration+"\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(downs[0], []byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)

	listed := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "ls", "--migrations-dir", migrations)
	c.Assert(listed.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", listed.Stderr))
	c.Assert(listed.Stdout, qt.Contains, "add_widgets")

	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "up", "--migrations-dir", migrations, "--db-url", databaseURL)
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", applied.Stderr))

	// `status` reports the state of the database rather than the names in the
	// directory: the applied count and the pending count are what an operator
	// decides on, and they are what a broken `up` would get wrong while still
	// exiting 0.
	status := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "status", "--migrations-dir", migrations, "--db-url", databaseURL)
	c.Assert(status.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", status.Stderr))
	c.Assert(status.Stdout, qt.Contains, "Applied Migrations: 1")
	c.Assert(status.Stdout, qt.Contains, "Pending Migrations: 0")

	// The address is a credential-bearing string, so the command has to keep it
	// out of its own output. A status line printing the URL back would put a
	// password into every log that captured it.
	c.Assert(status.Stdout, qt.Not(qt.Contains), databaseURL)

	rolled := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "down", "--migrations-dir", migrations, "--db-url", databaseURL,
		"--target", "0", "--confirm")
	c.Assert(rolled.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", rolled.Stderr))

	after := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "status", "--migrations-dir", migrations, "--db-url", databaseURL)
	c.Assert(after.ExitCode, qt.Equals, 0, qt.Commentf("stderr:\n%s", after.Stderr))
	c.Assert(after.Stdout, qt.Contains, "Applied Migrations: 0")
}

// TestMigrationsUpFailurePathE2E measures the refusal rather than the success.
//
// An exit code is only observable through a process, and this is the shape an
// operator meets most: a directory that is not there. The diagnostic belongs on
// stderr, and a run that printed it on stdout would look identical to a result.
func TestMigrationsUpFailurePathE2E(t *testing.T) {
	c := qt.New(t)

	dir := c.TempDir()
	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
		"migrations", "up",
		"--migrations-dir", filepath.Join(dir, "absent"),
		"--db-url", "sqlite://"+filepath.ToSlash(filepath.Join(dir, "unused.db")))

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Not(qt.Equals), "")
}

// TestVersionE2E pins the shape a script reads.
//
// `version` is parsed by installers and by users reporting a defect, so the
// field names are a contract even though nothing else in the binary depends on
// them.
func TestVersionE2E(t *testing.T) {
	c := qt.New(t)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{}, "version")

	c.Assert(got.ExitCode, qt.Equals, 0)
	c.Assert(got.Stdout, qt.Contains, "Version:")
	c.Assert(got.Stdout, qt.Contains, "Commit:")
	c.Assert(got.Stdout, qt.Contains, "Platform:")
	c.Assert(got.Stderr, qt.Equals, "")
}

// TestDBReadFailurePathE2E covers the verb an operator reaches for first.
//
// `db read` had no end-to-end coverage, and its failure path is the half that
// matters here: an address Ptah cannot use has to be refused by name rather
// than reported as an empty schema, which would read as a database with no
// tables in it.
func TestDBReadFailurePathE2E(t *testing.T) {
	c := qt.New(t)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{},
		"db", "read", "--db-url", "nonsense://localhost/whatever")

	c.Assert(got.ExitCode, qt.Not(qt.Equals), 0)
	c.Assert(got.Stderr, qt.Not(qt.Equals), "")
	c.Assert(got.Stdout, qt.Equals, "")
}
