//go:build integration

package schemadir_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// oracleEnv names the environment variable holding the path to the pinned
// Atlas CE binary this conformance run compares against.
const oracleEnv = "PTAH_ATLAS_ORACLE"

// oracleVersion is the only build this run trusts. A different build may have
// changed the very rule under test, so comparing against it would report
// version drift as a divergence.
const oracleVersion = "atlas community version v1.3.0"

func writeSchemaSourceDir(tb testing.TB, files map[string]string) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	for name, contents := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600), qt.IsNil)
	}
	return dir
}

// TestOracleAgreesOnADirectoryThatRedeclaresAnObject is the oracle row for the
// shape a merge cannot represent: a schema DIRECTORY whose files declare the
// same object twice.
//
// The pinned binary reads such a directory by executing every file in filename
// order against the dev database, so the second declaration is an engine error:
//
//	read state from "2_b.sql": executing statement: "CREATE TABLE users (...)":
//	table users already exists
//
// The first cut of Ptah's directory source merged the parsed files instead. It
// exited 0 on `schema diff` with a table that appears in NEITHER file, and
// exited 0 on `schema apply` having really written it. The rule this row holds
// is the compatibility rule itself: ptah-compat must never exit 0 where the
// pinned community binary exits 1.
//
// The rows are a set, and only the set is a measurement. `redeclared` alone
// would pass on a build that refused every directory; `distinct` and `guarded`
// are the two layouts that binary accepts, and they are what says the refusal
// separates a conflict from a multi-file schema and from an idempotent script.
//
// The `hcl redeclared` row records the ONE deliberate divergence, with the
// measurement that justifies it in the same place: that binary renders two
// `CREATE TABLE users` statements and exits 0 on `schema diff`, then exits 1 on
// `schema apply` executing the plan it just printed. Ptah refuses at read time
// on both verbs, which can never accept a source that binary rejects.
func TestOracleAgreesOnADirectoryThatRedeclaresAnObject(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c.TB)

	tests := []struct {
		name   string
		files  map[string]string
		assert func(c *qt.C, verdict dirVerdict)
	}{
		{
			name: "redeclared",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY, extra TEXT);\n",
			},
			assert: func(c *qt.C, verdict dirVerdict) {
				c.Assert(verdict.oracleDiff, qt.Equals, 1)
				c.Assert(verdict.ptahDiff, qt.Equals, verdict.oracleDiff)
				c.Assert(verdict.oracleApply, qt.Equals, 1)
				c.Assert(verdict.ptahApply, qt.Equals, verdict.oracleApply)
			},
		},
		{
			name: "distinct",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE posts (id INTEGER PRIMARY KEY);\n",
			},
			assert: func(c *qt.C, verdict dirVerdict) {
				c.Assert(verdict.oracleDiff, qt.Equals, 0)
				c.Assert(verdict.ptahDiff, qt.Equals, verdict.oracleDiff)
				c.Assert(verdict.oracleApply, qt.Equals, 0)
				c.Assert(verdict.ptahApply, qt.Equals, verdict.oracleApply)
			},
		},
		{
			name: "guarded",
			files: map[string]string{
				"1_a.sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY);\n",
				"2_b.sql": "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, extra TEXT);\n",
			},
			assert: func(c *qt.C, verdict dirVerdict) {
				c.Assert(verdict.oracleDiff, qt.Equals, 0)
				c.Assert(verdict.ptahDiff, qt.Equals, verdict.oracleDiff)
				c.Assert(verdict.oracleApply, qt.Equals, 0)
				c.Assert(verdict.ptahApply, qt.Equals, verdict.oracleApply)
			},
		},
		{
			name: "hcl redeclared",
			files: map[string]string{
				"a.hcl": "schema \"main\" {}\ntable \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
				"b.hcl": "table \"users\" {\n  schema = schema.main\n  column \"id\" {\n    type = int\n  }\n}\n",
			},
			assert: func(c *qt.C, verdict dirVerdict) {
				// The divergence, and the reason it is safe: the plan that
				// binary prints at exit 0 is the plan it then fails to apply.
				c.Assert(verdict.oracleDiff, qt.Equals, 0)
				c.Assert(verdict.oracleApply, qt.Equals, 1)
				c.Assert(verdict.ptahDiff, qt.Equals, 1)
				c.Assert(verdict.ptahApply, qt.Equals, 1)
			},
		},
	}
	c.Assert(tests, qt.HasLen, 4,
		qt.Commentf("all measured directory layouts must remain in the oracle matrix"))

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dir := writeSchemaSourceDir(c.TB, test.files)
			emptyHCL := filepath.Join(c.TempDir(), "empty.hcl")
			c.Assert(os.WriteFile(emptyHCL, []byte("schema \"main\" {}\n"), 0o600), qt.IsNil)

			test.assert(c, measureDirVerdict(c.TB, oracle, compat, dir, emptyHCL))
		})
	}
}

// dirVerdict is one fixture's four exit codes: two binaries times two verbs.
type dirVerdict struct {
	oracleDiff  int
	ptahDiff    int
	oracleApply int
	ptahApply   int
}

// measureDirVerdict runs `schema diff` and `schema apply` on one directory with
// both binaries.
//
// Every invocation gets its OWN dev database and its own target file. A shared
// dev database is not merely untidy here: the pinned binary refuses one that is
// not clean, so a reused dev database would report a refusal that belongs to the
// previous run rather than to the fixture.
func measureDirVerdict(tb testing.TB, oracle, compat, dir, emptyHCL string) dirVerdict {
	c := qt.New(tb)
	c.Helper()
	work := c.TempDir()

	diffArgs := func(devDB string) []string {
		return []string{
			"schema", "diff",
			"--from", "file://" + emptyHCL,
			"--to", "file://" + dir,
			"--dev-url", "sqlite://" + filepath.Join(work, devDB),
		}
	}
	applyArgs := func(targetDB, devDB string) []string {
		return []string{
			"schema", "apply",
			"--url", "sqlite://" + filepath.Join(work, targetDB),
			"--to", "file://" + dir,
			"--dev-url", "sqlite://" + filepath.Join(work, devDB),
			"--auto-approve",
		}
	}

	return dirVerdict{
		oracleDiff:  runForExitCode(c.TB, oracle, diffArgs("oracle-diff-dev.db")...),
		ptahDiff:    runForExitCode(c.TB, compat, diffArgs("ptah-diff-dev.db")...),
		oracleApply: runForExitCode(c.TB, oracle, applyArgs("oracle-target.db", "oracle-apply-dev.db")...),
		ptahApply:   runForExitCode(c.TB, compat, applyArgs("ptah-target.db", "ptah-apply-dev.db")...),
	}
}

// runForExitCode runs one command and returns its process exit code.
func runForExitCode(tb testing.TB, binary string, args ...string) int {
	c := qt.New(tb)
	c.Helper()

	out, err := exec.Command(binary, args...).CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	c.Assert(err, qt.IsNil, qt.Commentf("%s %s\n%s", binary, strings.Join(args, " "), out))
	return 0
}

// buildCompatBinary builds ptah-compat from this tree.
//
// The subject of this run is a process EXIT CODE, so the measurement has to be
// a process. Calling the command in this package would compare an error value
// against an exit status and could not see a regression in how one becomes the
// other.
func buildCompatBinary(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()

	path := filepath.Join(c.TempDir(), "ptah-compat")
	out, err := exec.Command("go", "build", "-o", path, "go.5x5.cz/ptah/cmd/ptah-compat").CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("build ptah-compat: %s", out))
	return path
}

// requireAtlasOracle resolves the pinned binary and refuses a different build.
func requireAtlasOracle(t *testing.T) string {
	t.Helper()

	oracle := os.Getenv(oracleEnv)
	if oracle == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run the schema-directory conformance run",
			oracleEnv, oracleVersion)
	}

	out, err := exec.Command(oracle, "version").Output() //nolint:gosec // the oracle path is operator-provided via PTAH_ATLAS_ORACLE
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", oracleEnv, oracle, err)
	}
	got, _, _ := strings.Cut(string(out), "\n")
	if strings.TrimSpace(got) != oracleVersion {
		t.Fatalf("%s=%s reports %q, want %q; a different build may have changed the rule under test",
			oracleEnv, oracle, strings.TrimSpace(got), oracleVersion)
	}
	return oracle
}
