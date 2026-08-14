//go:build integration

// Package migratedirquery_test compares the migration-directory URL query
// contract with the pinned Atlas CE binary through both command-line processes.
package migratedirquery_test

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

const (
	oracleEnv     = "PTAH_ATLAS_ORACLE"
	oracleVersion = "atlas community version v1.3.0"
	ignoredKey    = `ignoring migration directory URL query key "nonsense"`
)

type commandResult struct {
	code   int
	stdout string
	stderr string
}

type verbRow struct {
	name            string
	args            func(c *qt.C, dir, query string) []string
	writesDirectory bool
	assertSelected  func(c *qt.C, dir string)
	assertNative    func(c *qt.C, dir string)
	controlCode     int
	assertControl   func(c *qt.C, dir string)
}

func TestDirQueryContract_IgnoresUnknownKeysOnEveryCEVerb(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, row := range verbRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			assertUnknownKeyParity(c, row, oracle, oracle)
			assertUnknownKeyParity(c, row, compat, oracle)
		})
	}
}

func TestDirQueryContract_FormatSelectsTheLayoutOnEveryCEVerb(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, row := range verbRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			assertFormatSelection(c, row, oracle, oracle)
			assertFormatSelection(c, row, compat, oracle)
		})
	}
}

func TestDirQueryContract_UnknownFormatFailsOnEveryCEVerb(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, row := range verbRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			assertUnknownFormat(c, row, oracle, oracle)
			assertUnknownFormat(c, row, compat, oracle)
		})
	}
}

func TestDirQueryContract_QueryOutranksTheFlag(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, row := range flagVerbRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			assertQueryPrecedence(c, row, oracle, oracle)
			assertQueryPrecedence(c, row, compat, oracle)
		})
	}
}

func TestDirQueryContract_KeepsExtensionOnlyVerbsFailClosed(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, row := range extensionOnlyVerbRows() {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeNativeDir(c, oracle)
			result := runCommand(c, compat, row.args(c, dir, "?format=atlas&nonsense=1")...)

			c.Assert(result.code, qt.Equals, 1, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
			c.Assert(result.stderr, qt.Contains, "migration directory URL query parameters are not supported for this command")
		})
	}
}

func TestWithoutPtahEnvironment(t *testing.T) {
	c := qt.New(t)

	got := withoutPtahEnvironment([]string{
		"PATH=/bin",
		"PTAH_ATLAS_STRICT_COMPAT=1",
		"ptah_token=secret",
		"PtaH_Migrations_Dir=elsewhere",
		"NOT_PTAH=value",
		"PTAHWITHOUTUNDERSCORE=value",
	})

	c.Assert(got, qt.DeepEquals, []string{
		"PATH=/bin",
		"NOT_PTAH=value",
		"PTAHWITHOUTUNDERSCORE=value",
	})
}

func assertUnknownKeyParity(c *qt.C, row verbRow, binary, oracle string) {
	c.Helper()
	dir := writeNativeDir(c, oracle)
	before := directoryEntries(c, dir)
	result := runCommand(c, binary, row.args(c, dir, "?nonsense=1")...)

	c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	assertWriterChangedDirectory(c, row.writesDirectory, before, directoryEntries(c, dir))
	row.assertNative(c, dir)
	assertIgnoredKeyReport(c, binary, result.stderr)
}

func assertFormatSelection(c *qt.C, row verbRow, binary, oracle string) {
	c.Helper()
	dir := writeGolangMigrateDir(c, oracle)
	before := directoryEntries(c, dir)
	selected := runCommand(c, binary, row.args(c, dir, "?format=golang-migrate&nonsense=1")...)

	c.Assert(selected.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", selected.stdout, selected.stderr))
	assertWriterChangedDirectory(c, row.writesDirectory, before, directoryEntries(c, dir))
	row.assertSelected(c, dir)
	assertIgnoredKeyReport(c, binary, selected.stderr)

	controlDir := writeGolangMigrateDir(c, oracle)
	control := runCommand(c, binary, row.args(c, controlDir, "")...)
	c.Assert(control.code, qt.Equals, row.controlCode,
		qt.Commentf("control stdout: %s\ncontrol stderr: %s", control.stdout, control.stderr))
	row.assertControl(c, controlDir)
}

func assertUnknownFormat(c *qt.C, row verbRow, binary, oracle string) {
	c.Helper()
	dir := writeNativeDir(c, oracle)
	before := directoryContents(c, dir)
	result := runCommand(c, binary, row.args(c, dir, "?format=bogus&nonsense=1")...)

	c.Assert(result.code, qt.Equals, 1, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	c.Assert(result.stdout, qt.Equals, "")
	c.Assert(result.stderr, qt.Equals, "Error: unknown dir format \"bogus\"\n")
	c.Assert(directoryContents(c, dir), qt.DeepEquals, before)
}

func assertQueryPrecedence(c *qt.C, row verbRow, binary, oracle string) {
	c.Helper()

	golangMigrateDir := writeGolangMigrateDir(c, oracle)
	queryWins := runCommand(c, binary,
		append(row.args(c, golangMigrateDir, "?format=golang-migrate"), "--dir-format", "atlas")...)
	c.Assert(queryWins.code, qt.Equals, 0,
		qt.Commentf("query-wins stdout: %s\nquery-wins stderr: %s", queryWins.stdout, queryWins.stderr))
	row.assertSelected(c, golangMigrateDir)

	nativeDir := writeNativeDir(c, oracle)
	emptyQueryWins := runCommand(c, binary,
		append(row.args(c, nativeDir, "?format="), "--dir-format", "golang-migrate")...)
	c.Assert(emptyQueryWins.code, qt.Equals, 0,
		qt.Commentf("empty-query stdout: %s\nempty-query stderr: %s", emptyQueryWins.stdout, emptyQueryWins.stderr))
	row.assertNative(c, nativeDir)

	flagDir := writeGolangMigrateDir(c, oracle)
	ignoredKeyLeavesFlag := runCommand(c, binary,
		append(row.args(c, flagDir, "?nonsense=1"), "--dir-format", "golang-migrate")...)
	c.Assert(ignoredKeyLeavesFlag.code, qt.Equals, 0,
		qt.Commentf("flag stdout: %s\nflag stderr: %s", ignoredKeyLeavesFlag.stdout, ignoredKeyLeavesFlag.stderr))
	row.assertSelected(c, flagDir)
	assertIgnoredKeyReport(c, binary, ignoredKeyLeavesFlag.stderr)
}

func verbRows() []verbRow {
	noAssertion := func(_ *qt.C, _ string) {}
	return []verbRow{
		{
			name: "apply",
			args: func(c *qt.C, dir, query string) []string {
				return []string{"migrate", "apply", "--dir", fileURL(dir, query),
					"--url", sqliteURL(c, "apply.db")}
			},
			assertSelected: noAssertion,
			assertNative:   noAssertion,
			controlCode:    1,
			assertControl:  noAssertion,
		},
		{
			name: "hash",
			args: func(_ *qt.C, dir, query string) []string {
				return []string{"migrate", "hash", "--dir", fileURL(dir, query)}
			},
			assertSelected: assertGolangMigrateSum,
			assertNative:   assertAtlasSum,
			controlCode:    0,
			assertControl:  assertNativeSum,
		},
		{
			name: "validate",
			args: func(_ *qt.C, dir, query string) []string {
				return []string{"migrate", "validate", "--dir", fileURL(dir, query)}
			},
			assertSelected: noAssertion,
			assertNative:   noAssertion,
			controlCode:    1,
			assertControl:  noAssertion,
		},
		{
			name: "lint",
			args: func(c *qt.C, dir, query string) []string {
				return []string{"migrate", "lint", "--dir", fileURL(dir, query),
					"--dev-url", sqliteURL(c, "lint.db"), "--latest", "1"}
			},
			assertSelected: noAssertion,
			assertNative:   noAssertion,
			controlCode:    1,
			assertControl:  noAssertion,
		},
		{
			name: "status",
			args: func(c *qt.C, dir, query string) []string {
				return []string{"migrate", "status", "--dir", fileURL(dir, query),
					"--url", sqliteURL(c, "status.db")}
			},
			assertSelected: noAssertion,
			assertNative:   noAssertion,
			controlCode:    1,
			assertControl:  noAssertion,
		},
		{
			name: "set",
			args: func(c *qt.C, dir, query string) []string {
				return []string{"migrate", "set", "1", "--dir", fileURL(dir, query),
					"--url", sqliteURL(c, "set.db")}
			},
			assertSelected: noAssertion,
			assertNative:   noAssertion,
			controlCode:    1,
			assertControl:  noAssertion,
		},
		{
			name: "new",
			args: func(_ *qt.C, dir, query string) []string {
				return []string{"migrate", "new", "query_contract", "--dir", fileURL(dir, query)}
			},
			writesDirectory: true,
			assertSelected:  noAssertion,
			assertNative:    noAssertion,
			controlCode:     1,
			assertControl:   noAssertion,
		},
		{
			name: "diff",
			args: func(c *qt.C, dir, query string) []string {
				return []string{"migrate", "diff", "query_contract", "--dir", fileURL(dir, query),
					"--dev-url", sqliteURL(c, "diff.db"), "--to", "file://" + writeTarget(c)}
			},
			writesDirectory: true,
			assertSelected:  noAssertion,
			assertNative:    noAssertion,
			controlCode:     1,
			assertControl:   noAssertion,
		},
	}
}

func flagVerbRows() []verbRow {
	rows := verbRows()
	return []verbRow{rows[1], rows[2], rows[3], rows[4], rows[5], rows[6], rows[7]}
}

type extensionVerbRow struct {
	name string
	args func(c *qt.C, dir, query string) []string
}

func extensionOnlyVerbRows() []extensionVerbRow {
	return []extensionVerbRow{
		{name: "checkpoint", args: func(_ *qt.C, dir, query string) []string {
			return []string{"migrate", "checkpoint", "query_contract", "--dir", fileURL(dir, query)}
		}},
		{name: "down", args: func(c *qt.C, dir, query string) []string {
			return []string{"migrate", "down", "--dir", fileURL(dir, query), "--url", sqliteURL(c, "down.db")}
		}},
		{name: "edit", args: func(_ *qt.C, dir, query string) []string {
			return []string{"migrate", "edit", "1", "--dir", fileURL(dir, query)}
		}},
		{name: "rebase", args: func(_ *qt.C, dir, query string) []string {
			return []string{"migrate", "rebase", "1", "--dir", fileURL(dir, query)}
		}},
		{name: "rm", args: func(_ *qt.C, dir, query string) []string {
			return []string{"migrate", "rm", "1", "--dir", fileURL(dir, query)}
		}},
		{name: "test", args: func(c *qt.C, dir, query string) []string {
			return []string{"migrate", "test", "--dir", fileURL(dir, query), "--dev-url", sqliteURL(c, "test.db")}
		}},
	}
}

func writeNativeDir(c *qt.C, binary string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	result := runCommand(c, binary, "migrate", "hash", "--dir", "file://"+dir)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("hash stdout: %s\nhash stderr: %s", result.stdout, result.stderr))
	return dir
}

func writeGolangMigrateDir(c *qt.C, binary string) string {
	c.Helper()
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.up.sql"),
		[]byte("CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.down.sql"),
		[]byte("DROP TABLE widgets;\n"), 0o600), qt.IsNil)
	result := runCommand(c, binary, "migrate", "hash", "--dir", "file://"+dir+"?format=golang-migrate")
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("hash stdout: %s\nhash stderr: %s", result.stdout, result.stderr))
	return dir
}

func writeTarget(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "target.sql")
	c.Assert(os.WriteFile(path, []byte(
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"+
			"CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	return path
}

func fileURL(dir, query string) string {
	return "file://" + dir + query
}

func sqliteURL(c *qt.C, name string) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TempDir(), name)
}

func directoryEntries(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func directoryContents(c *qt.C, dir string) map[string]string {
	c.Helper()
	contents := make(map[string]string)
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}
		contents[relative] = string(data)
		return nil
	})
	c.Assert(err, qt.IsNil)
	return contents
}

func assertWriterChangedDirectory(c *qt.C, writer bool, before, after []string) {
	c.Helper()
	if writer {
		c.Assert(len(after) > len(before), qt.IsTrue,
			qt.Commentf("writer left entries unchanged: %v", after))
	}
}

func assertIgnoredKeyReport(c *qt.C, binary, stderr string) {
	c.Helper()
	if strings.HasSuffix(binary, "ptah-compat") {
		c.Assert(stderr, qt.Contains, ignoredKey)
	}
}

func assertGolangMigrateSum(c *qt.C, dir string) {
	c.Helper()
	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "1_init.up.sql")
	c.Assert(string(sum), qt.Not(qt.Contains), "1_init.down.sql")
}

func assertAtlasSum(c *qt.C, dir string) {
	c.Helper()
	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "1_init.sql")
}

func assertNativeSum(c *qt.C, dir string) {
	c.Helper()
	sum, err := os.ReadFile(filepath.Join(dir, "atlas.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(sum), qt.Contains, "1_init.up.sql")
	c.Assert(string(sum), qt.Contains, "1_init.down.sql")
}

func runCommand(c *qt.C, binary string, args ...string) commandResult {
	c.Helper()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.Command(binary, args...) //nolint:gosec // binary is the built compat executable or the operator-provided pinned oracle
	cmd.Env = cleanEnvironment()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return commandResult{code: exitErr.ExitCode(), stdout: stdout.String(), stderr: stderr.String()}
	}
	c.Assert(err, qt.IsNil, qt.Commentf("%s %s\nstdout: %s\nstderr: %s",
		binary, strings.Join(args, " "), stdout.String(), stderr.String()))
	return commandResult{stdout: stdout.String(), stderr: stderr.String()}
}

func cleanEnvironment() []string {
	return withoutPtahEnvironment(os.Environ())
}

func withoutPtahEnvironment(environment []string) []string {
	result := make([]string, 0, len(environment))
	for _, item := range environment {
		key, _, _ := strings.Cut(item, "=")
		if strings.HasPrefix(strings.ToUpper(key), "PTAH_") {
			continue
		}
		result = append(result, item)
	}
	return result
}

func buildCompatBinary(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "ptah-compat")
	out, err := exec.Command("go", "build", "-o", path, "go.5x5.cz/ptah/cmd/ptah-compat").CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("build ptah-compat: %s", out))
	return path
}

func requireAtlasOracle(t *testing.T) string {
	t.Helper()
	oracle := os.Getenv(oracleEnv)
	if oracle == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run the migrate-dir query conformance test",
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
