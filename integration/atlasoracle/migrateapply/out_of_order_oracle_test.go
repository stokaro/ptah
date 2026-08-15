//go:build integration

package migrateapply_test

import (
	"database/sql"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"
)

const (
	oracleEnv     = "PTAH_ATLAS_ORACLE"
	oracleVersion = "atlas community version v1.3.0"

	earlyVersion  = "20240101000000"
	middleVersion = "20240102000000"
	lateVersion   = "20240103000000"
)

const (
	earlyBody  = "CREATE TABLE oracle_early (id INTEGER PRIMARY KEY);\n"
	middleBody = "CREATE TABLE oracle_middle (id INTEGER PRIMARY KEY);\n"
	lateBody   = "CREATE TABLE oracle_late (id INTEGER PRIMARY KEY);\n"
)

// TestOracleDistinguishesPrefixAndIntervalInsertions reconciles the two
// different default-order answers recorded for stokaro/ptah#1241 item 5.
//
// With only the late revision applied, adding an earlier prefix migration is
// exit 0 in the pinned binary and the migration stays unapplied. With applied
// revisions on both sides of a newly inserted migration, the same binary
// refuses it as out of order. Ptah refuses both by default, because accepting
// the first answer would silently discard an authored migration.
func TestOracleDistinguishesPrefixAndIntervalInsertions(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	t.Run("prefix insertion is the retained divergence", func(t *testing.T) {
		c := qt.New(t)
		lateOnly := writeOracleMigrationDir(c, oracle, map[string]string{
			lateVersion + "_late.sql": lateBody,
		})
		prefixed := writeOracleMigrationDir(c, oracle, map[string]string{
			earlyVersion + "_early.sql": earlyBody,
			lateVersion + "_late.sql":   lateBody,
		})

		oracleDB := filepath.Join(c.TempDir(), "oracle.db")
		firstOracle := runApply(c, oracle, oracleDB, lateOnly)
		c.Assert(firstOracle.code, qt.Equals, 0, qt.Commentf("oracle output: %s", firstOracle.output))
		secondOracle := runApply(c, oracle, oracleDB, prefixed)
		c.Assert(secondOracle.code, qt.Equals, 0, qt.Commentf("oracle output: %s", secondOracle.output))
		c.Assert(secondOracle.output, qt.Contains, "No migration files to execute")
		assertMigrationState(c, oracleDB, []string{"oracle_late"}, []string{lateVersion})

		compatDB := filepath.Join(c.TempDir(), "compat.db")
		firstCompat := runApply(c, compat, compatDB, lateOnly)
		c.Assert(firstCompat.code, qt.Equals, 0, qt.Commentf("ptah-compat output: %s", firstCompat.output))
		secondCompat := runApply(c, compat, compatDB, prefixed)
		c.Assert(secondCompat.code, qt.Equals, 1, qt.Commentf("ptah-compat output: %s", secondCompat.output))
		c.Assert(secondCompat.output, qt.Contains, "out-of-order pending migrations")
		assertMigrationState(c, compatDB, []string{"oracle_late"}, []string{lateVersion})
	})

	t.Run("interval insertion is parity", func(t *testing.T) {
		c := qt.New(t)
		initial := writeOracleMigrationDir(c, oracle, map[string]string{
			earlyVersion + "_early.sql": earlyBody,
			lateVersion + "_late.sql":   lateBody,
		})
		withMiddle := writeOracleMigrationDir(c, oracle, map[string]string{
			earlyVersion + "_early.sql":   earlyBody,
			middleVersion + "_middle.sql": middleBody,
			lateVersion + "_late.sql":     lateBody,
		})

		oracleDB := filepath.Join(c.TempDir(), "oracle.db")
		firstOracle := runApply(c, oracle, oracleDB, initial)
		c.Assert(firstOracle.code, qt.Equals, 0, qt.Commentf("oracle output: %s", firstOracle.output))
		secondOracle := runApply(c, oracle, oracleDB, withMiddle)
		c.Assert(secondOracle.code, qt.Equals, 1, qt.Commentf("oracle output: %s", secondOracle.output))
		c.Assert(secondOracle.output, qt.Contains, "out of order")
		assertMigrationState(c, oracleDB,
			[]string{"oracle_early", "oracle_late"},
			[]string{earlyVersion, lateVersion})

		compatDB := filepath.Join(c.TempDir(), "compat.db")
		firstCompat := runApply(c, compat, compatDB, initial)
		c.Assert(firstCompat.code, qt.Equals, 0, qt.Commentf("ptah-compat output: %s", firstCompat.output))
		secondCompat := runApply(c, compat, compatDB, withMiddle)
		c.Assert(secondCompat.code, qt.Equals, 1, qt.Commentf("ptah-compat output: %s", secondCompat.output))
		c.Assert(secondCompat.output, qt.Contains, "out-of-order pending migrations")
		assertMigrationState(c, compatDB,
			[]string{"oracle_early", "oracle_late"},
			[]string{earlyVersion, lateVersion})
	})
}

type commandResult struct {
	code   int
	output string
}

func writeOracleMigrationDir(c *qt.C, oracle string, files map[string]string) string {
	c.Helper()
	dir := c.TempDir()
	for name, body := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	result := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("oracle hash output: %s", result.output))
	return dir
}

func runApply(c *qt.C, binary, dbPath, dir string) commandResult {
	c.Helper()
	return runCommand(c, binary,
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
	)
}

func runCommand(c *qt.C, binary string, args ...string) commandResult {
	c.Helper()
	cmd := exec.Command(binary, args...)
	cmd.Env = commandEnvironmentWithoutPtahVariables(os.Environ())
	out, err := cmd.CombinedOutput()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return commandResult{code: exitErr.ExitCode(), output: string(out)}
	}
	c.Assert(err, qt.IsNil, qt.Commentf("%s %s\n%s", binary, strings.Join(args, " "), out))
	return commandResult{output: string(out)}
}

func commandEnvironmentWithoutPtahVariables(environment []string) []string {
	filtered := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if strings.HasPrefix(strings.ToUpper(key), "PTAH_") {
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered
}

func assertMigrationState(c *qt.C, dbPath string, wantTables, wantVersions []string) {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	// tb.Cleanup rather than c.Defer: (*qt.C).Defer registers a cleanup that
	// panics unless Done() ran, and C.Run supplies that Done while a checker
	// built here does not. Cleanup is what Defer wraps, and it needs no pair.
	c.Cleanup(func() { c.Assert(db.Close(), qt.IsNil) })

	rows, err := db.Query("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
	c.Assert(err, qt.IsNil)

	var tables []string
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		if name != "atlas_schema_revisions" {
			tables = append(tables, name)
		}
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(rows.Close(), qt.IsNil)
	c.Assert(tables, qt.DeepEquals, wantTables)

	versionRows, err := db.Query("SELECT version FROM atlas_schema_revisions ORDER BY version")
	c.Assert(err, qt.IsNil)

	var versions []string
	for versionRows.Next() {
		var version string
		c.Assert(versionRows.Scan(&version), qt.IsNil)
		versions = append(versions, version)
	}
	c.Assert(versionRows.Err(), qt.IsNil)
	c.Assert(versionRows.Close(), qt.IsNil)
	c.Assert(versions, qt.DeepEquals, wantVersions)
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
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run the migrate-apply conformance test",
			oracleEnv, oracleVersion)
	}

	out, err := exec.Command(oracle, "version").Output() // #nosec G204 G702 -- the oracle path is operator-provided via PTAH_ATLAS_ORACLE
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
