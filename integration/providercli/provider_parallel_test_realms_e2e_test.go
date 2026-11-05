//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// parallelCases is a migration test directory of cases that each migrate,
// write one row and count the rows. Cases that shared a database would count
// each other's rows.
func parallelCases(c *qt.C, names ...string) string {
	c.Helper()
	dir := c.TempDir()
	var cases strings.Builder
	for _, name := range names {
		cases.WriteString("test \"migrate\" \"" + name + "\" {\n" +
			"  parallel = true\n" +
			"  migrate { to = \"0000000001\" }\n" +
			"  exec { sql = \"INSERT INTO records (id) VALUES (1)\" }\n" +
			"  assert { sql = \"SELECT count(*) = 1 FROM records\" }\n" +
			"}\n")
	}
	c.Assert(os.WriteFile(filepath.Join(dir, "parallel.test.hcl"), []byte(cases.String()), 0o600), qt.IsNil)
	return dir
}

// besides lists the files next to path whose names start with its name: the
// database itself and every realm the fixture created beside it. The
// fixture's lock files are left out: it keeps one beside each database a
// migration locked, and a release removes the database and not that file.
func besides(c *qt.C, path string) []string {
	c.Helper()
	entries, err := os.ReadDir(filepath.Dir(path))
	c.Assert(err, qt.IsNil)
	var names []string
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), filepath.Base(path)) && !strings.Contains(entry.Name(), ".lock-") {
			names = append(names, entry.Name())
		}
	}
	slices.Sort(names)
	return names
}

// Parallel migration test cases on a provider's engine each run in a realm
// the provider creates inside the run's dev database, and each realm is
// removed when its case ends. Neither case sees the other's row, and the
// operator's database is left as it was.
func TestProviderRunsParallelMigrationTestCasesInRealms_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	operator := filepath.Join(c.TempDir(), "operator.db")
	c.Assert(os.WriteFile(operator, nil, 0o600), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"migrations", "test", "--provider-config", f.manifestPath,
		"--dir", parallelCases(c, "first", "second", "third"), "--migrations-dir", hashedMigrations(c),
		"--db-url", fixtureAddress("localhost", operator)})
	f.assertStopped(c)

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(out, qt.Contains, "3 cases, 3 passed")
	c.Assert(besides(c, operator), qt.DeepEquals, []string{"operator.db"})
	c.Assert(tables(c, operator), qt.HasLen, 0)
}

// A provider's engine whose dev database is the URL's own, as a fixture
// address that names no host is, gives a case no database of its own, so
// parallel cases are refused rather than run against one shared database.
func TestProviderRunsParallelMigrationTestCasesInRealms_FailurePath(t *testing.T) {
	c := qt.New(t)
	f := newDevCLI(c)
	operator := filepath.Join(c.TempDir(), "operator.db")
	c.Assert(os.WriteFile(operator, nil, 0o600), qt.IsNil)

	out, diagnostics, code := executeProviderCLI(c.Context(), []string{"migrations", "test", "--provider-config", f.manifestPath,
		"--dir", parallelCases(c, "first", "second"), "--migrations-dir", hashedMigrations(c),
		"--db-url", fixtureAddress("", operator)})
	f.assertStopped(c)

	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("%s\n%s", out, diagnostics))
	c.Assert(diagnostics, qt.Contains, "this dialect has no per-case database isolation")
	c.Assert(tables(c, operator), qt.HasLen, 0)
}
