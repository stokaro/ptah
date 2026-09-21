package lint_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
)

// serverVersionReport is the part of the JSON report this file reads.
type serverVersionReport struct {
	Dialect           string `json:"dialect"`
	ServerVersion     string `json:"server_version"`
	ServerVersionNote string `json:"server_version_note"`
	Error             string `json:"error"`
}

// writeLintableDirectory writes a migration pair with nothing in it for a rule
// to find, so what a test reads is the target the run planned against.
func writeLintableDirectory(c *qt.C) string {
	c.Helper()
	dir := c.TB.(*testing.T).TempDir()
	writeLintTestFile(c, dir, "0000000001_users.up.sql", "CREATE TABLE users (id BIGINT PRIMARY KEY);\n")
	writeLintTestFile(c, dir, "0000000001_users.down.sql", "DROP TABLE users;\n")
	return dir
}

func decodeServerVersionReport(c *qt.C, out string) serverVersionReport {
	c.Helper()
	var report serverVersionReport
	c.Assert(json.Unmarshal([]byte(out), &report), qt.IsNil, qt.Commentf("%s", out))
	return report
}

func TestLintServerVersion_HappyPath(t *testing.T) {
	t.Run("the flag names the server the run plans against", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)

		stdout, _, err := execute("--dir", dir, "--dialect", "postgres", "--server-version", "16", "--format", "json")

		c.Assert(err, qt.IsNil)
		report := decodeServerVersionReport(c, stdout)
		c.Assert(report.ServerVersion, qt.Equals, "16")
		c.Assert(report.ServerVersionNote, qt.Equals, "")
	})

	// A declaration a project checks in is a declaration, so a run with no flag
	// plans against it.
	t.Run("the configuration names it when the flag does not", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)
		writeLintTestFile(c, dir, ".ptah-lint.yaml", "dialect: postgres\nserver-version: \"16\"\n")

		stdout, _, err := execute("--dir", dir, "--format", "json")

		c.Assert(err, qt.IsNil)
		c.Assert(decodeServerVersionReport(c, stdout).ServerVersion, qt.Equals, "16")
	})

	// The flag is what the operator typed for this run, so it outranks the
	// file. Without the precedence a project that pinned a version could not
	// ask what a different one would say.
	t.Run("the flag outranks the configuration", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)
		writeLintTestFile(c, dir, ".ptah-lint.yaml", "dialect: postgres\nserver-version: \"16\"\n")

		stdout, _, err := execute("--dir", dir, "--server-version", "18", "--format", "json")

		c.Assert(err, qt.IsNil)
		c.Assert(decodeServerVersionReport(c, stdout).ServerVersion, qt.Equals, "18")
	})

	// A run that named no server says so by carrying no version rather than by
	// naming one it picked.
	t.Run("no declaration names no server", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)

		stdout, _, err := execute("--dir", dir, "--dialect", "postgres", "--format", "json")

		c.Assert(err, qt.IsNil)
		report := decodeServerVersionReport(c, stdout)
		c.Assert(report.Dialect, qt.Equals, "postgres")
		c.Assert(report.ServerVersion, qt.Equals, "")
	})

	// A recognized version Ptah has not measured plans against a release line
	// the operator did not name, and a clean run is exactly when that has to
	// be said out loud.
	t.Run("an unmeasured version says what was planned instead", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)

		stdout, stderr, err := execute("--dir", dir, "--dialect", "postgres", "--server-version", "99")

		c.Assert(err, qt.IsNil)
		c.Assert(stdout, qt.Contains, "No lint findings.")
		c.Assert(stderr, qt.Contains, "warning: postgres 99 is newer than the newest measured release line")
	})

	// JSON carries the note as a field, so printing the sentence as well would
	// put prose beside a document a consumer decodes.
	t.Run("json carries the note as a field and prints no prose", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)

		stdout, stderr, err := execute("--dir", dir, "--dialect", "postgres", "--server-version", "99", "--format", "json")

		c.Assert(err, qt.IsNil)
		c.Assert(stderr, qt.Equals, "")
		c.Assert(decodeServerVersionReport(c, stdout).ServerVersionNote,
			qt.Matches, `postgres 99 is newer than the newest measured release line.*`)
	})

	// Every other machine format renders findings and nothing else, so a run
	// against a preset the operator did not name would otherwise say nothing
	// about it. The sentence goes to the stream the document did not take.
	t.Run("a format that carries no note still says what it planned against", func(t *testing.T) {
		tests := []struct{ name, format string }{
			{name: "github actions", format: "github-actions"},
			{name: "sarif", format: "sarif"},
			{name: "gitlab", format: "gitlab"},
		}

		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				c := qt.New(t)
				dir := writeLintableDirectory(c)

				_, stderr, err := execute(
					"--dir", dir, "--dialect", "postgres", "--server-version", "99", "--format", test.format)

				c.Assert(err, qt.IsNil)
				c.Assert(stderr, qt.Contains,
					"warning: postgres 99 is newer than the newest measured release line")
			})
		}
	})
}

func TestLintServerVersion_FailurePath(t *testing.T) {
	// A typo that resolved to the dialect default would silently change which
	// rules can fire, so it stops the run at a usage exit code.
	t.Run("a value that names no server", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)

		_, stderr, err := execute("--dir", dir, "--dialect", "postgres", "--server-version", "seventeen", "--format", "json")

		c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
		c.Assert(decodeServerVersionReport(c, stderr).Error,
			qt.Matches, `"seventeen" is not a recognized postgres server version.*`)
	})

	t.Run("a version with no dialect", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)

		_, stderr, err := execute("--dir", dir, "--server-version", "16", "--format", "json")

		c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
		c.Assert(decodeServerVersionReport(c, stderr).Error,
			qt.Matches, `server version "16" needs a dialect.*`)
	})

	t.Run("a configuration version that names no server", func(t *testing.T) {
		c := qt.New(t)
		dir := writeLintableDirectory(c)
		writeLintTestFile(c, dir, ".ptah-lint.yaml", "dialect: postgres\nserver-version: seventeen\n")

		_, stderr, err := execute("--dir", dir, "--format", "json")

		c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
		c.Assert(decodeServerVersionReport(c, stderr).Error, qt.Contains, "is not a recognized postgres server version")
	})
}
