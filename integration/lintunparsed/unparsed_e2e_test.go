//go:build integration

package lintunparsed_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/clirun"
)

// These tests hold what a lint run says about a statement the SQL parser could not
// read. They run the shipped binaries because the exit code and the stream are the
// claim, and because a dev database decides the answer, which only the command
// wires. SQLite is enough: it is a real server for the replay, and it is the engine
// the reproduction names.

const (
	// garbage is no SQL at all, and the parser refuses it.
	garbage = "CREATE TABLE b BOGUS;\n"
	// validUnmodeled is SQL SQLite runs, and the parser refuses its last statement.
	validUnmodeled = "CREATE TABLE ok (id INT PRIMARY KEY);\nCREATE VIEW v AS SELECT id FROM ok;\nDROP VIEW v;\n"
	// parsed is a statement the parser reads.
	parsed = "CREATE TABLE ok (id INT PRIMARY KEY);\n"
)

// directory writes one migration in Ptah's layout under a fresh project.
func directory(c *qt.C, up, down string) string {
	c.Helper()
	project := c.TempDir()
	dir := filepath.Join(project, "d")
	c.Assert(os.Mkdir(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_x.up.sql"), []byte(up), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_x.down.sql"), []byte(down), 0o600), qt.IsNil)
	return project
}

func devURL(c *qt.C) string {
	c.Helper()
	return atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "dev.db"))
}

// TestNativeLint_WithNoDevDatabase is the reproduction and its controls. A parse
// failure keeps the exit code, because the parser refuses valid SQL too, and it is
// never reported as a clean run.
func TestNativeLint_WithNoDevDatabase(t *testing.T) {
	tests := []struct {
		name     string
		up       string
		down     string
		wantLine string
	}{
		{
			name: "text that is no SQL", up: garbage, down: "SELECT 1;\n",
			wantLine: "d/1_x.up.sql:1 [info] AC102: the SQL parser could not read this statement (unsupported table option: BOGUS)",
		},
		{
			name: "SQL the parser does not model", up: validUnmodeled, down: "SELECT 1;\n",
			wantLine: "d/1_x.up.sql:3 [info] AC102: the SQL parser could not read this statement (unsupported DROP target: VIEW)",
		},
		{
			name: "a down file", up: parsed, down: "DROP TABEL ok;\n",
			wantLine: "d/1_x.down.sql:1 [info] AC102: the SQL parser could not read this statement (unsupported DROP target: TABEL)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: directory(c, test.up, test.down)},
				"migrations", "lint", "--dir", "d", "--dialect", "sqlite",
			)
			c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.wantLine)
			c.Assert(run.Stdout, qt.Not(qt.Contains), "No lint findings.")
		})
	}
}

// TestNativeLint_ACleanRunStaysClean is the control for the report above: a file the
// parser reads has nothing to report.
func TestNativeLint_ACleanRunStaysClean(t *testing.T) {
	c := qt.New(t)
	run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: directory(c, parsed, "SELECT 1;\n")},
		"migrations", "lint", "--dir", "d", "--dialect", "sqlite",
	)
	c.Assert(run.ExitCode, qt.Equals, 0)
	c.Assert(run.Stdout, qt.Contains, "No lint findings.")
}

// TestNativeLint_AGateOnUnparsedStatementsIsOptIn says how a team that has no dev
// database turns the report into a gate, and that nothing else does.
func TestNativeLint_AGateOnUnparsedStatementsIsOptIn(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		args   []string
		want   int
	}{
		{name: "default threshold", want: 0},
		{name: "every finding fails", args: []string{"--fail-on", "any"}, want: 1},
		{name: "the rule raised to error", policy: "rules:\n  AC102:\n    severity: error\n", want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			project := directory(c, validUnmodeled, "SELECT 1;\n")
			c.Assert(os.WriteFile(filepath.Join(project, "d", ".ptah-lint.yaml"), []byte(test.policy), 0o600), qt.IsNil)

			args := append([]string{"migrations", "lint", "--dir", "d", "--dialect", "sqlite"}, test.args...)
			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: project}, args...)

			c.Assert(run.ExitCode, qt.Equals, test.want, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, "AC102")
		})
	}
}

// TestNativeLint_WithADevDatabase holds the other half of the answer. A server runs
// the up files, so it refuses text that is no SQL and its acceptance replaces the
// parser's opinion, and it never runs a down file, so those stay reported.
func TestNativeLint_WithADevDatabase(t *testing.T) {
	tests := []struct {
		name       string
		up         string
		down       string
		wantExit   int
		wantStdout string
		wantStderr string
	}{
		{
			name: "the server refuses text that is no SQL", up: garbage, down: "SELECT 1;\n",
			wantExit: 2, wantStderr: "near \"BOGUS\": syntax error",
		},
		{
			name: "the server accepts SQL the parser does not model", up: validUnmodeled, down: "SELECT 1;\n",
			wantStdout: "No lint findings.",
		},
		{
			name: "no server runs a down file", up: parsed, down: "DROP TABEL ok;\n",
			wantStdout: "d/1_x.down.sql:1 [info] AC102",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: directory(c, test.up, test.down)},
				"migrations", "lint", "--dir", "d", "--dialect", "sqlite", "--dev-url", devURL(c),
			)
			c.Assert(run.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.wantStdout)
			c.Assert(run.Stderr, qt.Contains, test.wantStderr)
		})
	}
}

// atlasDirectory writes one migration in Atlas's layout and hashes it, which
// `migrate lint` asks for.
func atlasDirectory(c *qt.C, sql string) string {
	c.Helper()
	project := c.TempDir()
	dir := filepath.Join(project, "d")
	c.Assert(os.Mkdir(dir, 0o750), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_x.sql"), []byte(sql), 0o600), qt.IsNil)
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: project}, "migrate", "hash", "--dir", "file://d")
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr: %s", hashed.Stderr))
	return project
}

// TestCompatLint_WithADevDatabase holds the parity half. The community binary is the
// contract here, so a statement the server runs reports nothing and a statement it
// refuses ends the run with the exit code it always had.
func TestCompatLint_WithADevDatabase(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantExit   int
		wantStdout string
		wantStderr string
	}{
		{
			name: "the server accepts SQL the parser does not model", sql: validUnmodeled,
			wantStdout: "no diagnostics found",
		},
		{
			name: "the server refuses text that is no SQL", sql: garbage,
			wantExit: 1, wantStderr: "near \"BOGUS\": syntax error",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			run := clirun.Run(c, clirun.Compat, clirun.Options{Dir: atlasDirectory(c, test.sql)},
				"migrate", "lint", "--dir", "file://d", "--latest", "1", "--dev-url", devURL(c),
			)
			c.Assert(run.ExitCode, qt.Equals, test.wantExit, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
			c.Assert(run.Stdout, qt.Contains, test.wantStdout)
			c.Assert(run.Stderr, qt.Contains, test.wantStderr)
			c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), "AC102")
		})
	}
}

// TestCompatLint_ADownFileStaysOutOfTheReport keeps the native report of a down file
// off this surface. A dev database never runs one on either binary, and the community
// binary says nothing about it, so neither does this one.
func TestCompatLint_ADownFileStaysOutOfTheReport(t *testing.T) {
	c := qt.New(t)
	project := directory(c, parsed, "DROP TABEL ok;\n")
	hashed := clirun.Run(c, clirun.Compat, clirun.Options{Dir: project},
		"migrate", "hash", "--dir", "file://d", "--dir-format", "golang-migrate",
	)
	c.Assert(hashed.ExitCode, qt.Equals, 0, qt.Commentf("stderr: %s", hashed.Stderr))

	run := clirun.Run(c, clirun.Compat, clirun.Options{Dir: project},
		"migrate", "lint", "--dir", "file://d", "--dir-format", "golang-migrate", "--latest", "1", "--dev-url", devURL(c),
	)

	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, "no diagnostics found")
	c.Assert(run.Stdout+run.Stderr, qt.Not(qt.Contains), "AC102")
}

// TestCompatLint_WithNoDevDatabase is the capability the community binary does not
// have, so no parity binds it. It reports the statement and keeps the exit code, as
// the native surface does, and strict mode refuses the variable that opens it.
func TestCompatLint_WithNoDevDatabase(t *testing.T) {
	c := qt.New(t)
	project := atlasDirectory(c, garbage)

	run := clirun.Run(c, clirun.Compat,
		clirun.Options{Dir: project, Env: append(os.Environ(), "PTAH_ATLAS_LINT_WITHOUT_DEV_URL=1")},
		"migrate", "lint", "--dir", "file://d", "--latest", "1",
	)
	c.Assert(run.ExitCode, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, "[AC102]")

	strict := clirun.Run(c, clirun.Compat,
		clirun.Options{Dir: project, Env: append(os.Environ(), "PTAH_ATLAS_LINT_WITHOUT_DEV_URL=1", "PTAH_ATLAS_STRICT_COMPAT=1")},
		"migrate", "lint", "--dir", "file://d", "--latest", "1",
	)
	c.Assert(strict.ExitCode, qt.Equals, 1)
	c.Assert(strict.Stderr, qt.Contains, "PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_LINT_WITHOUT_DEV_URL")
}

// TestSQLLint_RefusesAStatementItCouldNotRead pins the neighbor this change leaves
// alone: `ptah sql lint` reports a parse failure at error severity. It has no replay
// to defer to, and its --disable refuses to hide the finding.
func TestSQLLint_RefusesAStatementItCouldNotRead(t *testing.T) {
	c := qt.New(t)
	project := directory(c, garbage, "SELECT 1;\n")

	run := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: project}, "sql", "lint", "--dialect", "sqlite", "d/1_x.up.sql")

	c.Assert(run.ExitCode, qt.Equals, 1, qt.Commentf("stdout: %s\nstderr: %s", run.Stdout, run.Stderr))
	c.Assert(run.Stdout, qt.Contains, "error SQL002: unsupported table option: BOGUS")
}
