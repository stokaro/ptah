package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
)

// TestRunLint_AnExtensionWithoutADevDatabaseNamesBothRules pins the offline
// run of a directory that installs an extension: it exits 0 with no finding of
// either extension rule, and stderr names both as having run without the
// assessment, so the clean result is not read as a clean extension.
func TestRunLint_AnExtensionWithoutADevDatabaseNamesBothRules(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeLintTestFile(c, dir, "0000000001_crypto.up.sql", "CREATE EXTENSION pgcrypto;\n")
	writeLintTestFile(c, dir, "0000000001_crypto.down.sql", "DROP EXTENSION pgcrypto;\n")

	stdout, stderr, err := execute("--dir", dir, "--dialect", "postgres", "--advisory-source", "bundled")

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "No lint findings.")
	c.Assert(stderr, qt.Contains, "warning: coverage/unassessed-extension, security/vulnerable-extension ran without the "+
		"extension advisory assessment it reads")
}

// TestRunLint_AMalformedAdvisorySourceIsRefused pins that the flag is read
// before any work, with the reason the advisory source gives.
func TestRunLint_AMalformedAdvisorySourceIsRefused(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeLintTestFile(c, dir, "0000000001_crypto.up.sql", "CREATE EXTENSION pgcrypto;\n")

	stdout, stderr, err := execute("--dir", dir, "--dialect", "postgres", "--advisory-source", "http://mirror.example.org/")

	c.Assert(exitcode.Code(err, 0), qt.Equals, 2)
	c.Assert(stderr, qt.Contains, "--advisory-source: advisory source http://mirror.example.org/: http is accepted only for a loopback host; use https")
	c.Assert(stdout, qt.Equals, "")
}
