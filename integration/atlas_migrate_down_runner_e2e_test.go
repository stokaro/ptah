//go:build integration

package integration_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/clirun"
)

// `ptah-compat migrate down` takes the runner from PTAH_RUNNER, the
// environment twin of native --runner, and takes no flag for it. Without
// --format it forwards to `migrations down`, which reads the variable through
// its flag binding. With --format it builds the rollback itself and reads the
// variable there, so the output flag does not change what the rollback runs.

// compatDownFormat is a --format template that names the versions only, so
// the report says nothing a row asserts on.
const compatDownFormat = "{{ .Current }}->{{ .Target }}"

// compatOwnTransactionDirectory writes an Atlas directory whose second
// version rolls back in a transaction of its own, applies it to a scratch
// database, and returns the project root and the database.
func compatOwnTransactionDirectory(c *qt.C) (root, target string) {
	c.Helper()
	root = c.TempDir()
	dir := filepath.Join(root, "m")
	c.Assert(os.MkdirAll(dir, 0o750), qt.IsNil)
	for name, sql := range map[string]string{
		"1_init.sql":          "CREATE TABLE base (id int);\n",
		"1_init.down.sql":     "DROP TABLE base;\n",
		"2_backfill.sql":      "CREATE TABLE t2 (id int);\nCREATE TABLE t3 (id int);\n",
		"2_backfill.down.sql": downOwnTransactionsBody,
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil) // #nosec G703 -- the path is c.TempDir() plus a constant name
	}
	target, _ = scratchReplayDatabase(c)
	options := clirun.Options{Dir: root, Env: environmentWithoutPtahVariables()}
	hashed := clirun.Run(c, clirun.Compat, options, "migrate", "hash", "--dir", "file://m")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("%s", hashed.Stderr))
	applied := clirun.Run(c, clirun.Compat, options, "migrate", "apply", "--url", target, "--dir", "file://m")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", applied.Stdout, applied.Stderr))
	return root, target
}

// compatRollBack rolls the second version back with `ptah-compat migrate
// down`, in an environment that carries no PTAH_ variable but env.
func compatRollBack(c *qt.C, root, target string, env []string, flags ...string) clirun.Result {
	c.Helper()
	return clirun.Run(c, clirun.Compat, clirun.Options{Dir: root, Env: append(environmentWithoutPtahVariables(), env...)},
		append([]string{"migrate", "down", "--url", target, "--dir", "file://m", "--to-version", "1"}, flags...)...)
}

// compatRevisionCensus is the Atlas revision rows of version, read from the
// database at url, and which of tables exist.
func compatRevisionCensus(c *qt.C, url, version string, tables ...string) (int, []string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var rows int
	c.Assert(conn.QueryRowContext(c.Context(),
		"SELECT count(*) FROM atlas_schema_revisions.atlas_schema_revisions WHERE version = $1", version).Scan(&rows),
		qt.IsNil)
	existing := make([]string, 0, len(tables))
	for _, table := range tables {
		var exists bool
		c.Assert(conn.QueryRowContext(c.Context(), "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists), qt.IsNil)
		existing = append(existing, fmt.Sprintf("%s=%t", table, exists))
	}
	return rows, existing
}

func TestAtlasMigrateDownRunnerRollsBackTheFileOwnTransactionsE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
	}{
		{name: "forwarded to migrations down"},
		{name: "with --format", flags: []string{"--format", compatDownFormat}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root, target := compatOwnTransactionDirectory(c)

			got := compatRollBack(c, root, target, []string{"PTAH_RUNNER=golang-migrate"}, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, tables := compatRevisionCensus(c, target, "2", "base", "t2", "t3")
			c.Assert(rows, qt.Equals, 0)
			c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=false", "t3=false"})
		})
	}
}

// --dev-url replays the rollback on the dev database the way the target runs
// it, on both paths, so the down file is verified there before the target is
// touched.
func TestAtlasMigrateDownRunnerVerifiesOnTheDevDatabaseE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
	}{
		{name: "forwarded to migrations down"},
		{name: "with --format", flags: []string{"--format", compatDownFormat}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root, target := compatOwnTransactionDirectory(c)
			dev, _ := scratchReplayDatabase(c)

			got := compatRollBack(c, root, target, []string{"PTAH_RUNNER=golang-migrate"},
				append([]string{"--dev-url", dev}, test.flags...)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			rows, tables := compatRevisionCensus(c, target, "2", "base", "t2", "t3")
			c.Assert(rows, qt.Equals, 0)
			c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=false", "t3=false"})
		})
	}
}

// Without PTAH_RUNNER the migrator wraps the down file and refuses its
// transaction control before the file runs, on both paths: both tables stay,
// and so does the revision.
func TestAtlasMigrateDownRunnerRefusesWrappedTransactionControlE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
	}{
		{name: "forwarded to migrations down"},
		{name: "with --format", flags: []string{"--format", compatDownFormat}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			root, target := compatOwnTransactionDirectory(c)

			got := compatRollBack(c, root, target, nil, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout+got.Stderr, qt.Contains, "controls transaction state")
			rows, tables := compatRevisionCensus(c, target, "2", "base", "t2", "t3")
			c.Assert(rows, qt.Equals, 1)
			c.Assert(tables, qt.DeepEquals, []string{"base=true", "t2=true", "t3=true"})
		})
	}
}

// A PTAH_RUNNER that names no runner is refused on both paths with the same
// words, before the rollback reads the directory: the directory here does not
// exist.
func TestAtlasMigrateDownRunnerRefusesAnUnknownRunnerE2E(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags []string
	}{
		{name: "forwarded to migrations down"},
		{name: "with --format", flags: []string{"--format", compatDownFormat}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := compatRollBack(c, c.TempDir(), unreachableDatabaseURL, []string{"PTAH_RUNNER=flyway"}, test.flags...)

			c.Assert(got.ExitCode, qt.Equals, 1, qt.Commentf("%s%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, "Error: invalid runner \"flyway\": expected ptah or golang-migrate\n")
		})
	}
}
