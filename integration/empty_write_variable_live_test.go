//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
)

// The tests in this file run the PostgreSQL half of
// empty_write_variable_e2e_test.go: what an empty PTAH_EXCLUDE and an empty
// PTAH_TARGET would widen is decided by a server's catalog, so a server
// reads the result back.

// postgresKeepAndExtra applies writeVariableInitialSchema to a PostgreSQL
// database of this test's own, writes desired.sql beside it, and returns the
// directory and the database URL.
func postgresKeepAndExtra(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir = c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "initial.sql"), []byte(writeVariableInitialSchema), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "desired.sql"), []byte(writeVariableDesiredSchema), 0o600), qt.IsNil)
	targetURL = postgresScratchURLOn(c, dbtarget.PostgreSQL, "ptah_empty_write")
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"schema", "apply", "--db-url", targetURL, "--schema-file", "initial.sql", "--auto-approve")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(readPostgresKeepAndExtra(c, targetURL), qt.DeepEquals, keepAndExtraUntouched)
	return dir, targetURL
}

// readPostgresKeepAndExtra reads the state of keep and extra back from the
// public schema.
func readPostgresKeepAndExtra(c *qt.C, targetURL string) keepAndExtraState {
	c.Helper()
	return keepAndExtraState{
		Tables: limitHistoryColumn(c, targetURL, "SELECT table_name FROM information_schema.tables "+
			"WHERE table_schema = 'public' AND table_name IN ('keep', 'extra') ORDER BY table_name"),
		Columns: limitHistoryColumn(c, targetURL, "SELECT column_name FROM information_schema.columns "+
			"WHERE table_schema = 'public' AND table_name = 'keep' ORDER BY ordinal_position"),
	}
}

// readPostgresHistoryTables lists which of a, b and c the public schema holds.
func readPostgresHistoryTables(c *qt.C, targetURL string) []string {
	c.Helper()
	return limitHistoryColumn(c, targetURL, "SELECT table_name FROM information_schema.tables "+
		"WHERE table_schema = 'public' AND table_name IN ('a', 'b', 'c') ORDER BY table_name")
}

func TestSchemaApplyExcludeVariableLive_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want keepAndExtraState
	}{
		{name: "unset, the apply changes every object", want: keepAndExtraWhole},
		{name: "PTAH_EXCLUDE keeps the table it names", env: []string{"PTAH_EXCLUDE=extra"}, want: keepAndExtraNarrowed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, targetURL := postgresKeepAndExtra(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				"schema", "apply", "--db-url", targetURL, "--schema-file", "desired.sql", "--auto-approve")

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(readPostgresKeepAndExtra(c, targetURL), qt.DeepEquals, tc.want)
		})
	}
}

func TestSchemaApplyExcludeVariableLive_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, targetURL := postgresKeepAndExtra(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables("PTAH_EXCLUDE=")},
		"schema", "apply", "--db-url", targetURL, "--schema-file", "desired.sql", "--auto-approve")

	c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(got.Stderr, qt.Equals,
		"error: PTAH_EXCLUDE is set but empty: give it a value, or unset it and leave --exclude out to exclude nothing\n")
	c.Assert(readPostgresKeepAndExtra(c, targetURL), qt.DeepEquals, keepAndExtraUntouched)
}

// appliedPostgresStopHistory applies the directory [nativeStopHistory] writes
// to a PostgreSQL database of this test's own.
func appliedPostgresStopHistory(c *qt.C) (dir, targetURL string) {
	c.Helper()
	dir, _ = nativeStopHistory(c)
	targetURL = postgresScratchURLOn(c, dbtarget.PostgreSQL, "ptah_empty_stop")
	applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables()},
		"migrations", "up", "--db-url", targetURL, "--migrations-dir", "migrations")
	c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))
	c.Assert(readPostgresHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
	return dir, targetURL
}

func TestMigrationsDownStopVariableLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, targetURL := appliedPostgresStopHistory(c)

	got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables("PTAH_TARGET=1")},
		"migrations", "down", "--db-url", targetURL, "--migrations-dir", "migrations", "--confirm")

	c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
	c.Assert(readPostgresHistoryTables(c, targetURL), qt.DeepEquals, []string{"a"})
}

func TestMigrationsDownStopVariableLive_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		env        []string
		wantStderr string
	}{
		{
			name: "no stop",
			wantStderr: `error: name where the rollback stops: pass --target <version> (--target 0 rolls back every ` +
				`applied migration), --to-tag <tag> or --limit <n>; "ptah migrations status" lists the applied versions` + "\n",
		},
		{
			name: "empty PTAH_TARGET",
			env:  []string{"PTAH_TARGET="},
			wantStderr: "error: PTAH_TARGET is set but empty: give it a value, or unset it and leave --target out to " +
				"stop at --to-tag or --limit\n",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			dir, targetURL := appliedPostgresStopHistory(c)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir, Env: withVariables(tc.env...)},
				"migrations", "down", "--db-url", targetURL, "--migrations-dir", "migrations", "--confirm")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Equals, tc.wantStderr)
			c.Assert(readPostgresHistoryTables(c, targetURL), qt.DeepEquals, []string{"a", "b", "c"})
		})
	}
}
