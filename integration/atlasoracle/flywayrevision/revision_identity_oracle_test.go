//go:build integration

// Package flywayrevision_test compares converted Flyway revision identity with
// the pinned Atlas CE binary while keeping Ptah's numeric execution order.
package flywayrevision_test

import (
	"database/sql"
	"errors"
	"io/fs"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"
	_ "modernc.org/sqlite"
)

const (
	oracleEnv         = "PTAH_ATLAS_ORACLE"
	oraclePostgresEnv = "PTAH_ATLAS_ORACLE_POSTGRES_DEV_URL"
	oracleVersion     = "atlas community version v1.3.0"
)

type commandResult struct {
	code   int
	stdout string
	stderr string
}

type migrationFile struct {
	name string
	body string
}

type revisionRow struct {
	Version     string
	Description string
}

type identityCase struct {
	name          string
	files         []migrationFile
	wantRows      []revisionRow
	oracleCurrent string
	compatCurrent string
	lintLatest    string
	oracleLint    string
	compatLint    string
}

func TestFlywayRevisionIdentityMatchesAtlasCE(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, test := range identityCases() {
		c.Run(test.name, func(c *qt.C) {
			dir := writeHashedFlywayDir(c, oracle, test.files)
			oracleDB := filepath.Join(c.TempDir(), "oracle.db")
			compatDB := filepath.Join(c.TempDir(), "compat.db")

			assertSQLiteApplyIdentity(c, oracle, dir, oracleDB, test.wantRows)
			assertSQLiteApplyIdentity(c, compat, dir, compatDB, test.wantRows)
			assertStatusCurrent(c, oracle, dir, oracleDB, test.oracleCurrent)
			assertStatusCurrent(c, compat, dir, compatDB, test.compatCurrent)
			assertLintHeader(c, oracle, dir, test.lintLatest, test.oracleLint)
			assertLintHeader(c, compat, dir, test.lintLatest, test.compatLint)
		})
	}
}

func TestFlywayApplyBaselineUsesExactSourceToken(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "B1.5__base.sql", body: "CREATE TABLE exact_baseline (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__later.sql", body: "CREATE TABLE after_exact_baseline (id INTEGER PRIMARY KEY);\n"},
	})

	for _, binary := range []string{oracle, compat} {
		result := runCommand(c, binary,
			"migrate", "apply",
			"--dir", "file://"+dir+"?format=flyway",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "baseline.db"),
			"--baseline", "1.5",
			"--dry-run",
		)
		c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
		c.Assert(result.stdout+result.stderr, qt.Contains, "2")
		c.Assert(result.stdout+result.stderr, qt.Not(qt.Contains), "461168")

		unknown := runCommand(c, binary,
			"migrate", "apply",
			"--dir", "file://"+dir+"?format=flyway",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "unknown-baseline.db"),
			"--baseline", "missing",
			"--dry-run",
		)
		c.Assert(unknown.code, qt.Equals, 1)
		c.Assert(unknown.stderr, qt.Contains, `baseline version "missing" not found`)
	}
}

func TestFlywayApplyToVersionExtensionUsesExactSourceToken(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V1__one.sql", body: "CREATE TABLE exact_bound_one (id INTEGER PRIMARY KEY);\n"},
		{name: "V1.5__half.sql", body: "CREATE TABLE exact_bound_half (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__two.sql", body: "CREATE TABLE exact_bound_two (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "exact-bound.db")

	result := runCommand(c, compat,
		"migrate", "apply",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--to-version", "1.5",
	)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, []revisionRow{
		{Version: "1", Description: "one"},
		{Version: "1.5", Description: "half"},
	})

	ceControl := runCommand(c, oracle, "migrate", "apply", "--to-version", "1.5")
	c.Assert(ceControl.code, qt.Equals, 1)
	c.Assert(ceControl.stderr, qt.Contains, "unknown flag: --to-version")
}

func TestFlywaySameTokenBaselineInteropFailsClosedWhenAtlasTypeIsAmbiguous(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V2__base.sql", body: "CREATE TABLE same_token_base (id INTEGER PRIMARY KEY);\n"},
		{name: "B2__base.sql", body: "CREATE TABLE same_token_base (id INTEGER PRIMARY KEY);\n"},
	})
	want := []revisionRow{{Version: "2", Description: "base"}}

	ptahFirstDB := filepath.Join(c.TempDir(), "ptah-first.db")
	assertSQLiteApplyIdentity(c, compat, dir, ptahFirstDB, want)
	before := readSQLiteRevisionRows(c, ptahFirstDB)
	settled := runCommand(c, oracle,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+ptahFirstDB)
	c.Assert(settled.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", settled.stdout, settled.stderr))
	c.Assert(readSQLiteRevisionRows(c, ptahFirstDB), qt.DeepEquals, before)

	atlasFirstDB := filepath.Join(c.TempDir(), "atlas-first.db")
	assertSQLiteApplyIdentity(c, oracle, dir, atlasFirstDB, want)
	before = readSQLiteRevisionRows(c, atlasFirstDB)
	refused := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+atlasFirstDB)
	c.Assert(refused.code, qt.Equals, 1)
	c.Assert(refused.stderr, qt.Contains, "B2__base.sql")
	c.Assert(refused.stderr, qt.Contains, "V2__base.sql")
	c.Assert(readSQLiteRevisionRows(c, atlasFirstDB), qt.DeepEquals, before)
}

func TestFlywaySameTokenBaselineSetMarkerKeepsCEReadableHistory(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V2__base.sql", body: "CREATE TABLE set_same_token_base (id INTEGER PRIMARY KEY);\n"},
		{name: "B2__base.sql", body: "CREATE TABLE set_same_token_base (id INTEGER PRIMARY KEY);\n"},
	})

	atlasSetDB := filepath.Join(c.TempDir(), "atlas-set.db")
	atlasSet := runCommand(c, oracle,
		"migrate", "set", "2", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+atlasSetDB)
	c.Assert(atlasSet.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", atlasSet.stdout, atlasSet.stderr))
	c.Assert(readSQLiteRevisionType(c, atlasSetDB, "2"), qt.Equals, 4)
	atlasSettled := runCommand(c, oracle,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+atlasSetDB)
	c.Assert(atlasSettled.code, qt.Equals, 0)
	ptahRefusal := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+atlasSetDB)
	c.Assert(ptahRefusal.code, qt.Equals, 1)
	c.Assert(ptahRefusal.stderr, qt.Contains, "B2__base.sql")
	c.Assert(ptahRefusal.stderr, qt.Contains, "V2__base.sql")

	ptahSetDB := filepath.Join(c.TempDir(), "ptah-set.db")
	ptahSet := runCommand(c, compat,
		"migrate", "set", "2", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+ptahSetDB)
	c.Assert(ptahSet.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", ptahSet.stdout, ptahSet.stderr))
	c.Assert(ptahSet.stdout, qt.Equals, atlasSet.stdout)
	c.Assert(readSQLiteRevisionType(c, ptahSetDB, "2"), qt.Equals, 7)
	ptahSettled := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+ptahSetDB)
	c.Assert(ptahSettled.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", ptahSettled.stdout, ptahSettled.stderr))
	ceReadsMarker := runCommand(c, oracle,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+ptahSetDB)
	c.Assert(ceReadsMarker.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", ceReadsMarker.stdout, ceReadsMarker.stderr))
}

func TestFlywayDotPrefixedTokenAtlasCEContract(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V.foo__dot.sql", body: "CREATE TABLE dot_prefixed_identity (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "dot-prefixed.db")
	apply := runCommand(c, oracle,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
	c.Assert(apply.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", apply.stdout, apply.stderr))
	c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals,
		[]revisionRow{{Version: ".foo", Description: "dot"}})
	status := runCommand(c, oracle,
		"migrate", "status", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
	c.Assert(status.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", status.stdout, status.stderr))
	c.Assert(status.stdout, qt.Contains, "-- Current Version: .foo")
	lint := runCommand(c, oracle,
		"migrate", "lint", "--dir", "file://"+dir+"?format=flyway",
		"--dev-url", "sqlite://"+filepath.Join(c.TempDir(), "dot-lint.db"), "--latest", "1")
	c.Assert(lint.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", lint.stdout, lint.stderr))
	c.Assert(lint.stdout, qt.Contains, "-- analyzing version .foo")

	setDB := filepath.Join(c.TempDir(), "dot-set.db")
	set := runCommand(c, oracle,
		"migrate", "set", ".foo", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+setDB)
	c.Assert(set.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", set.stdout, set.stderr))
	c.Assert(set.stdout, qt.Contains, "Current version is .foo")
	c.Assert(readSQLiteRevisionRows(c, setDB), qt.DeepEquals,
		[]revisionRow{{Version: ".foo", Description: "dot"}})
}

func TestFlywayExecutionFailureNeverLeaksConvertedOrderKey(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V1.5__broken.sql", body: "INVALID SQL;\n"},
	})

	for _, binary := range []string{oracle, compat} {
		result := runCommand(c, binary,
			"migrate", "apply", "--dir", "file://"+dir+"?format=flyway",
			"--url", "sqlite://"+filepath.Join(c.TempDir(), "failure.db"))
		c.Assert(result.code, qt.Equals, 1)
		c.Assert(result.stdout, qt.Contains, "Migrating to version 1.5")
		c.Assert(result.stderr, qt.Contains, "1.5")
		c.Assert(result.stdout+result.stderr, qt.Not(qt.Contains), "4611686018427471935")
		c.Assert(result.stdout+result.stderr, qt.Not(qt.Contains), "461168")
	}
}

func TestFlywayDirtyOpaqueStatusCurrentMatchesFailedExactToken(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "Vx__broken.sql", body: "CREATE TABLE dirty_opaque (id INTEGER PRIMARY KEY);\nINVALID SQL;\n"},
	})

	oracleDB := filepath.Join(c.TempDir(), "oracle-dirty-opaque.db")
	oracleApply := runCommand(c, oracle,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+oracleDB,
		"--tx-mode", "none")
	c.Assert(oracleApply.code, qt.Equals, 1)
	oracleStatus := runCommand(c, oracle,
		"migrate", "status", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+oracleDB)
	c.Assert(oracleStatus.code, qt.Equals, 0,
		qt.Commentf("stdout: %s\nstderr: %s", oracleStatus.stdout, oracleStatus.stderr))
	c.Assert(oracleStatus.stdout, qt.Contains,
		"-- Current Version: No migration applied yet (1 statements applied)")

	compatDB := filepath.Join(c.TempDir(), "compat-dirty-opaque.db")
	compatApply := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+compatDB,
		"--tx-mode", "none")
	c.Assert(compatApply.code, qt.Equals, 1)
	compatStatus := runCommand(c, compat,
		"migrate", "status", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+compatDB)
	c.Assert(compatStatus.code, qt.Equals, 0,
		qt.Commentf("stdout: %s\nstderr: %s", compatStatus.stdout, compatStatus.stderr))
	c.Assert(compatStatus.stdout, qt.Contains,
		"-- Current Version: No migration applied yet (1 statements applied)")
	c.Assert(compatStatus.stdout, qt.Contains, "-- Next Version:    x (1 statements left)")
}

func TestFlywayCrossToolReuseKeepsIdentityAndRefusesChecksumEncoding(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, test := range interoperableIdentityCases() {
		c.Run(test.name, func(c *qt.C) {
			dir := writeHashedFlywayDir(c, oracle, test.files)
			for _, direction := range []struct {
				name   string
				first  string
				second string
			}{
				{name: "Atlas CE then Ptah", first: oracle, second: compat},
				{name: "Ptah then Atlas CE", first: compat, second: oracle},
			} {
				c.Run(direction.name, func(c *qt.C) {
					dbPath := filepath.Join(c.TempDir(), "interop.db")
					assertSQLiteApplyIdentity(c, direction.first, dir, dbPath, test.wantRows)
					before := readSQLiteRevisionRows(c, dbPath)
					result := runCommand(c, direction.second,
						"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
					assertCrossToolReuseResult(c, direction.name, result)
					c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, before)
				})
			}
		})
	}
}

func TestFlywayOnlyRepeatableCrossToolReuseMatchesExactEmptyIdentity(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "R__only.sql", body: "CREATE TABLE only_repeatable_identity (id INTEGER PRIMARY KEY);\n"},
	})
	want := []revisionRow{{Version: "", Description: "only"}}
	for _, direction := range []struct {
		name   string
		first  string
		second string
	}{
		{name: "Atlas CE then Ptah", first: oracle, second: compat},
		{name: "Ptah then Atlas CE", first: compat, second: oracle},
	} {
		c.Run(direction.name, func(c *qt.C) {
			dbPath := filepath.Join(c.TempDir(), "only-repeatable.db")
			assertSQLiteApplyIdentity(c, direction.first, dir, dbPath, want)
			before := readSQLiteRevisionRows(c, dbPath)
			result := runCommand(c, direction.second,
				"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
			c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
			c.Assert(result.stdout, qt.Equals, "No migration files to execute\n")
			c.Assert(result.stderr, qt.Equals, "")
			c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, before)
		})
	}
}

func TestFlywayRepeatableBodyChangeRemainsSettled(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	for _, binary := range []string{oracle, compat} {
		dir := writeHashedFlywayDir(c, oracle, []migrationFile{
			{name: "R__only.sql", body: "CREATE TABLE repeatable_settled (id INTEGER PRIMARY KEY);\n"},
		})
		dbPath := filepath.Join(c.TempDir(), "repeatable-settled.db")
		assertSQLiteApplyIdentity(c, binary, dir, dbPath, []revisionRow{{Version: "", Description: "only"}})
		c.Assert(os.WriteFile(filepath.Join(dir, "R__only.sql"), []byte(
			"CREATE TABLE repeatable_settled (id INTEGER PRIMARY KEY);\n"+
				"CREATE TABLE repeatable_changed (id INTEGER PRIMARY KEY);\n",
		), 0o600), qt.IsNil)
		hash := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
		c.Assert(hash.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", hash.stdout, hash.stderr))

		result := runCommand(c, binary,
			"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
		c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
		c.Assert(result.stdout, qt.Equals, "No migration files to execute\n")
		c.Assert(result.stderr, qt.Equals, "")
	}
}

func assertCrossToolReuseResult(c *qt.C, direction string, result commandResult) {
	c.Helper()
	switch direction {
	case "Atlas CE then Ptah":
		c.Assert(result.code, qt.Equals, 1, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
		c.Assert(result.stdout, qt.Equals, "")
		c.Assert(result.stderr, qt.Contains, "checksum mismatch")
	case "Ptah then Atlas CE":
		c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
		c.Assert(result.stdout, qt.Equals, "No migration files to execute\n")
		c.Assert(result.stderr, qt.Equals, "")
	default:
		c.Fatalf("unknown cross-tool direction %q", direction)
	}
}

func TestFlywayMigrateSetMatchesAtlasCE(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	tests := []struct {
		name     string
		files    []migrationFile
		target   string
		wantRows []revisionRow
	}{
		{
			name: "dotted",
			files: []migrationFile{
				{name: "V1__one.sql", body: "CREATE TABLE set_one (id INTEGER PRIMARY KEY);\n"},
				{name: "V1.5__half.sql", body: "CREATE TABLE set_half (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__two.sql", body: "CREATE TABLE set_two (id INTEGER PRIMARY KEY);\n"},
			},
			target: "1.5",
			wantRows: []revisionRow{
				{Version: "1", Description: "one"},
				{Version: "1.5", Description: "half"},
			},
		},
		{
			name: "zero padded",
			files: []migrationFile{
				{name: "V01__padded.sql", body: "CREATE TABLE set_padded (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__two.sql", body: "CREATE TABLE set_padded_two (id INTEGER PRIMARY KEY);\n"},
			},
			target:   "01",
			wantRows: []revisionRow{{Version: "01", Description: "padded"}},
		},
		{
			name: "non numeric",
			files: []migrationFile{
				{name: "Vx__named.sql", body: "CREATE TABLE set_named (id INTEGER PRIMARY KEY);\n"},
			},
			target:   "x",
			wantRows: []revisionRow{{Version: "x", Description: "named"}},
		},
		{
			name: "baseline",
			files: []migrationFile{
				{name: "B1.5__base.sql", body: "CREATE TABLE set_baseline (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__two.sql", body: "CREATE TABLE set_baseline_two (id INTEGER PRIMARY KEY);\n"},
			},
			target:   "1.5",
			wantRows: []revisionRow{{Version: "1.5", Description: "base"}},
		},
		{
			name: "dot prefixed",
			files: []migrationFile{
				{name: "V.foo__dot.sql", body: "CREATE TABLE set_dot (id INTEGER PRIMARY KEY);\n"},
			},
			target:   ".foo",
			wantRows: []revisionRow{{Version: ".foo", Description: "dot"}},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dir := writeHashedFlywayDir(c, oracle, test.files)
			oracleDB := filepath.Join(c.TempDir(), "oracle-set.db")
			compatDB := filepath.Join(c.TempDir(), "compat-set.db")
			oracleResult := runCommand(c, oracle,
				"migrate", "set", test.target, "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+oracleDB)
			compatResult := runCommand(c, compat,
				"migrate", "set", test.target, "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+compatDB)
			c.Assert(compatResult.code, qt.Equals, oracleResult.code)
			c.Assert(compatResult.stdout, qt.Equals, oracleResult.stdout)
			c.Assert(compatResult.stderr, qt.Equals, oracleResult.stderr)
			c.Assert(readSQLiteRevisionRows(c, oracleDB), qt.ContentEquals, test.wantRows)
			c.Assert(readSQLiteRevisionRows(c, compatDB), qt.ContentEquals, test.wantRows)
		})
	}
}

func TestFlywayMigrateSetOrdersRetiredBaselinesLikeAtlasCE(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	tests := []struct {
		name        string
		retiredFile migrationFile
		targetFile  migrationFile
		target      string
		wantStdout  string
		wantRows    []revisionRow
	}{
		{
			name: "retired B2 remains below target B3",
			retiredFile: migrationFile{
				name: "B2__two.sql",
				body: "CREATE TABLE retired_baseline (id INTEGER PRIMARY KEY);\n",
			},
			targetFile: migrationFile{
				name: "B3__three.sql",
				body: "CREATE TABLE target_baseline (id INTEGER PRIMARY KEY);\n",
			},
			target:     "3",
			wantStdout: "Current version is 3 (1 set):\n\n  + 3 (three)\n\n",
			wantRows: []revisionRow{
				{Version: "2", Description: "two"},
				{Version: "3", Description: "three"},
			},
		},
		{
			name: "retired B20 is removed above target B10",
			retiredFile: migrationFile{
				name: "B20__twenty.sql",
				body: "CREATE TABLE retired_baseline (id INTEGER PRIMARY KEY);\n",
			},
			targetFile: migrationFile{
				name: "B10__ten.sql",
				body: "CREATE TABLE target_baseline (id INTEGER PRIMARY KEY);\n",
			},
			target: "10",
			wantStdout: "Current version is 10 (1 set, 1 removed):\n\n" +
				"  + 10 (ten)\n  - 20 (twenty)\n\n",
			wantRows: []revisionRow{{Version: "10", Description: "ten"}},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			for _, binary := range []string{oracle, compat} {
				dir := writeHashedFlywayDir(c, oracle, []migrationFile{test.retiredFile})
				dbPath := filepath.Join(c.TempDir(), "baseline-rotation.db")
				assertSQLiteApplyIdentity(c, binary, dir, dbPath, []revisionRow{{
					Version:     strings.TrimPrefix(strings.SplitN(test.retiredFile.name, "__", 2)[0], "B"),
					Description: strings.TrimSuffix(strings.SplitN(test.retiredFile.name, "__", 2)[1], ".sql"),
				}})
				c.Assert(os.Remove(filepath.Join(dir, test.retiredFile.name)), qt.IsNil)
				c.Assert(os.WriteFile(
					filepath.Join(dir, test.targetFile.name),
					[]byte(test.targetFile.body),
					0o600,
				), qt.IsNil)
				hash := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
				c.Assert(hash.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", hash.stdout, hash.stderr))

				result := runCommand(c, binary,
					"migrate", "set", test.target,
					"--dir", "file://"+dir+"?format=flyway",
					"--url", "sqlite://"+dbPath,
				)
				c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
				c.Assert(result.stdout, qt.Equals, test.wantStdout)
				c.Assert(result.stderr, qt.Equals, "")
				c.Assert(readSQLiteRevisionRows(c, dbPath), qt.ContentEquals, test.wantRows)
			}
		})
	}
}

func TestFlywayMigrateSetEmptyIdentityNamesTokenExplicitly(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{{
		name: "R__only.sql",
		body: "CREATE TABLE set_repeatable (id INTEGER PRIMARY KEY);\n",
	}})
	oracleDB := filepath.Join(c.TempDir(), "oracle-set-empty.db")
	compatDB := filepath.Join(c.TempDir(), "compat-set-empty.db")
	oracleResult := runCommand(c, oracle,
		"migrate", "set", "", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+oracleDB)
	compatResult := runCommand(c, compat,
		"migrate", "set", "", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+compatDB)

	c.Assert(oracleResult.code, qt.Equals, 0)
	c.Assert(compatResult.code, qt.Equals, oracleResult.code)
	c.Assert(oracleResult.stdout, qt.Equals, "Current version is  (1 set):\n\n  +  (only)\n\n")
	c.Assert(compatResult.stdout, qt.Equals, "Current version is \"\" (1 set):\n\n  + \"\" (only)\n\n")
	c.Assert(compatResult.stderr, qt.Equals, oracleResult.stderr)
	wantRows := []revisionRow{{Version: "", Description: "only"}}
	c.Assert(readSQLiteRevisionRows(c, oracleDB), qt.ContentEquals, wantRows)
	c.Assert(readSQLiteRevisionRows(c, compatDB), qt.ContentEquals, wantRows)
}

func TestFlywaySetRemovesRetiredExactHistoryAboveTarget(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, binary := range []string{oracle, compat} {
		dir := writeHashedFlywayDir(c, oracle, []migrationFile{{
			name: "V2__two.sql",
			body: "CREATE TABLE retired_v2 (id INTEGER PRIMARY KEY);\n",
		}})
		dbPath := filepath.Join(c.TempDir(), "retired-above-target.db")
		assertSQLiteApplyIdentity(c, binary, dir, dbPath,
			[]revisionRow{{Version: "2", Description: "two"}})
		c.Assert(os.Remove(filepath.Join(dir, "V2__two.sql")), qt.IsNil)
		c.Assert(os.WriteFile(
			filepath.Join(dir, "V1__one.sql"),
			[]byte("CREATE TABLE current_v1 (id INTEGER PRIMARY KEY);\n"),
			0o600,
		), qt.IsNil)
		hash := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
		c.Assert(hash.code, qt.Equals, 0,
			qt.Commentf("oracle hash stdout: %s\nstderr: %s", hash.stdout, hash.stderr))

		result := runCommand(c, binary,
			"migrate", "set", "1", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)

		c.Assert(result.code, qt.Equals, 0,
			qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
		c.Assert(result.stdout, qt.Equals,
			"Current version is 1 (1 set, 1 removed):\n\n  + 1 (one)\n  - 2 (two)\n\n")
		c.Assert(result.stderr, qt.Equals, "")
		c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals,
			[]revisionRow{{Version: "1", Description: "one"}})
	}
}

func TestGolangMigrateIdentityRemainsNumeric(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.up.sql"), []byte("CREATE TABLE gm_identity (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "1_init.down.sql"), []byte("DROP TABLE gm_identity;\n"), 0o600), qt.IsNil)
	hash := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir+"?format=golang-migrate")
	c.Assert(hash.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", hash.stdout, hash.stderr))

	for _, binary := range []string{oracle, compat} {
		dbPath := filepath.Join(c.TempDir(), "gm.db")
		result := runCommand(c, binary,
			"migrate", "apply", "--dir", "file://"+dir+"?format=golang-migrate", "--url", "sqlite://"+dbPath)
		c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
		c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, []revisionRow{{Version: "1", Description: "init"}})
	}
}

func TestLegacyPtahOrderingKeyRefusesBeforeRecovery(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V1__init.sql", body: "CREATE TABLE legacy_identity (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "legacy.db")
	assertSQLiteApplyIdentity(c, compat, dir, dbPath, []revisionRow{{Version: "1", Description: "init"}})
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	// Reproduce both fields the older Ptah build wrote. Rewriting only version
	// would retain the current source-identity marker and correctly model a
	// retired exact numeric token instead of an obsolete ordering key.
	_, err = db.Exec("UPDATE atlas_schema_revisions SET version = '10000', operator_version = 'Ptah' WHERE version = '1'")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)

	refused := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
	c.Assert(refused.code, qt.Equals, 1)
	c.Assert(refused.stderr, qt.Contains, "recorded version -> exact Flyway source token")
	c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, []revisionRow{{Version: "10000", Description: "init"}})

	db, err = sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec("UPDATE atlas_schema_revisions SET version = '1' WHERE version = '10000'")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	settled := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
	c.Assert(settled.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", settled.stdout, settled.stderr))
	c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, []revisionRow{{Version: "1", Description: "init"}})
}

func TestFlywayRevisionIdentityMatchesAtlasCEOnPostgres(t *testing.T) {
	oracle := requireAtlasOracle(t)
	postgresURL := requirePostgresURL(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V01__padded.sql", body: "CREATE TABLE padded_identity (id BIGINT PRIMARY KEY);\n"},
		{name: "V1.5__dotted.sql", body: "CREATE TABLE dotted_identity (id BIGINT PRIMARY KEY);\n"},
		{name: "Vx__named.sql", body: "CREATE TABLE named_identity (id BIGINT PRIMARY KEY);\n"},
	})
	want := []revisionRow{
		{Version: "01", Description: "padded"},
		{Version: "1.5", Description: "dotted"},
		{Version: "x", Description: "named"},
	}

	oracleURL, cleanupOracle := postgresSchemaURL(c, postgresURL, "oracle")
	defer cleanupOracle()
	compatURL, cleanupCompat := postgresSchemaURL(c, postgresURL, "compat")
	defer cleanupCompat()
	assertPostgresApplyIdentity(c, oracle, dir, oracleURL, want)
	assertPostgresApplyIdentity(c, compat, dir, compatURL, want)
}

func requirePostgresURL(t *testing.T) string {
	t.Helper()
	postgresURL := os.Getenv(oraclePostgresEnv)
	if postgresURL == "" {
		t.Skipf("SKIPPED: set %s to run the PostgreSQL Flyway identity control", oraclePostgresEnv)
	}
	return postgresURL
}

func TestFlywayRevisionIdentityRefusesDuplicateEmptyTokensBeforeMutation(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)
	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V1__table.sql", body: "CREATE TABLE duplicate_empty_control (id INTEGER PRIMARY KEY);\n"},
		{name: "R1__a.sql", body: "CREATE VIEW repeatable_a AS SELECT id FROM duplicate_empty_control;\n"},
		{name: "R2__b.sql", body: "CREATE VIEW repeatable_b AS SELECT id FROM duplicate_empty_control;\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "compat.db")

	result := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)

	c.Assert(result.code, qt.Equals, 1, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	c.Assert(result.stderr, qt.Contains, "both carry the empty Atlas version and cannot be executed together")
	_, statErr := os.Stat(dbPath)
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}

func TestEnvironmentWithoutPtahVariables(t *testing.T) {
	c := qt.New(t)

	c.Assert(environmentWithoutPtahVariables([]string{
		"PATH=/usr/bin",
		"PTAH_MIGRATIONS_DIR=/ambient",
		"ptah_token=secret",
		"PTAH_ATLAS_STRICT_COMPAT=1",
		"DATABASE_URL=sqlite://kept.db",
	}), qt.DeepEquals, []string{
		"PATH=/usr/bin",
		"DATABASE_URL=sqlite://kept.db",
	})
}

func identityCases() []identityCase {
	return []identityCase{
		{
			name: "plain",
			files: []migrationFile{
				{name: "V1__plain.sql", body: "CREATE TABLE plain_identity (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__second.sql", body: "CREATE TABLE second_identity (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []revisionRow{
				{Version: "1", Description: "plain"},
				{Version: "2", Description: "second"},
			},
			oracleCurrent: "2",
			compatCurrent: "2",
			lintLatest:    "1",
			oracleLint:    "Analyzing changes from version 1 to 2",
			compatLint:    "Analyzing changes from version 1 to 2",
		},
		{
			name: "dotted",
			files: []migrationFile{
				{name: "V1__one.sql", body: "CREATE TABLE dotted_one (id INTEGER PRIMARY KEY);\n"},
				{name: "V1.5__half.sql", body: "CREATE TABLE dotted_half (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__two.sql", body: "CREATE TABLE dotted_two (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []revisionRow{
				{Version: "1", Description: "one"},
				{Version: "1.5", Description: "half"},
				{Version: "2", Description: "two"},
			},
			oracleCurrent: "2",
			compatCurrent: "2",
			lintLatest:    "1",
			oracleLint:    "Analyzing changes from version 1.5 to 2",
			compatLint:    "Analyzing changes from version 1.5 to 2",
		},
		{
			name: "zero padded",
			files: []migrationFile{
				{name: "V01__padded.sql", body: "CREATE TABLE padded_identity (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__two.sql", body: "CREATE TABLE padded_two (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []revisionRow{
				{Version: "01", Description: "padded"},
				{Version: "2", Description: "two"},
			},
			oracleCurrent: "2",
			compatCurrent: "2",
			lintLatest:    "1",
			oracleLint:    "Analyzing changes from version 01 to 2",
			compatLint:    "Analyzing changes from version 01 to 2",
		},
		{
			name: "non numeric",
			files: []migrationFile{
				{name: "Vx__named.sql", body: "CREATE TABLE named_identity (id INTEGER PRIMARY KEY);\n"},
				{name: "V1__one.sql", body: "CREATE TABLE named_one (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []revisionRow{
				{Version: "x", Description: "named"},
				{Version: "1", Description: "one"},
			},
			oracleCurrent: "x",
			compatCurrent: "x",
			lintLatest:    "1",
			oracleLint:    "Analyzing changes from version x to 1",
			compatLint:    "Analyzing changes from version x to 1",
		},
		{
			name: "baseline",
			files: []migrationFile{
				{name: "B1.5__base.sql", body: "CREATE TABLE baseline_identity (id INTEGER PRIMARY KEY);\n"},
				{name: "V2__later.sql", body: "CREATE TABLE baseline_later (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []revisionRow{
				{Version: "1.5", Description: "base"},
				{Version: "2", Description: "later"},
			},
			oracleCurrent: "2",
			compatCurrent: "2",
			lintLatest:    "1",
			oracleLint:    "Analyzing changes from version 1.5 to 2",
			compatLint:    "Analyzing changes from version 1.5 to 2",
		},
		{
			name: "dot prefixed",
			files: []migrationFile{
				{name: "V.foo__dot.sql", body: "CREATE TABLE dot_prefixed_identity (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []revisionRow{
				{Version: ".foo", Description: "dot"},
			},
			oracleCurrent: ".foo",
			compatCurrent: ".foo",
			lintLatest:    "1",
			oracleLint:    "-- analyzing version .foo",
			compatLint:    "-- analyzing version .foo",
		},
		{
			name: "ordinary token ending R",
			files: []migrationFile{
				{name: "V1R__ordinary.sql", body: "CREATE TABLE token_ending_r (id INTEGER PRIMARY KEY);\n"},
			},
			wantRows: []revisionRow{
				{Version: "1R", Description: "ordinary"},
			},
			oracleCurrent: "1R",
			compatCurrent: "1R",
			lintLatest:    "1",
			oracleLint:    "Analyzing changes until version 1R (1 migration in total):",
			compatLint:    "Analyzing changes until version 1R (1 migration in total):",
		},
		{
			name: "repeatable empty token",
			files: []migrationFile{
				{name: "V1__table.sql", body: "CREATE TABLE repeatable_identity (id INTEGER PRIMARY KEY);\n"},
				{name: "R__view.sql", body: "CREATE VIEW repeatable_view AS SELECT id FROM repeatable_identity;\n"},
			},
			wantRows: []revisionRow{
				{Version: "1", Description: "table"},
				{Version: "", Description: "view"},
			},
			oracleCurrent: "1",
			compatCurrent: "1",
			lintLatest:    "1",
			oracleLint:    "Analyzing changes (1 migration in total):",
			compatLint:    "Analyzing changes (1 migration in total):",
		},
	}
}

func interoperableIdentityCases() []identityCase {
	return slices.DeleteFunc(slices.Clone(identityCases()), func(test identityCase) bool {
		return test.name == "baseline"
	})
}

func writeHashedFlywayDir(c *qt.C, oracle string, files []migrationFile) string {
	c.Helper()
	dir := c.TempDir()
	for _, file := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, file.name), []byte(file.body), 0o600), qt.IsNil)
	}
	result := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("oracle hash stdout: %s\nstderr: %s", result.stdout, result.stderr))
	return dir
}

func assertSQLiteApplyIdentity(c *qt.C, binary, dir, dbPath string, want []revisionRow) {
	c.Helper()
	result := runCommand(c, binary,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Assert(readRevisionRows(c, db), qt.ContentEquals, want)
	c.Assert(db.Close(), qt.IsNil)
}

func assertPostgresApplyIdentity(c *qt.C, binary, dir, databaseURL string, want []revisionRow) {
	c.Helper()
	result := runCommand(c, binary,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", databaseURL)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	db, err := sql.Open("pgx", databaseURL)
	c.Assert(err, qt.IsNil)
	c.Assert(readRevisionRows(c, db), qt.ContentEquals, want)
	c.Assert(db.Close(), qt.IsNil)
}

func readRevisionRows(c *qt.C, db *sql.DB) []revisionRow {
	c.Helper()
	rows, err := db.Query("SELECT version, description FROM atlas_schema_revisions ORDER BY executed_at, version")
	c.Assert(err, qt.IsNil)
	var revisions []revisionRow
	for rows.Next() {
		var revision revisionRow
		c.Assert(rows.Scan(&revision.Version, &revision.Description), qt.IsNil)
		revisions = append(revisions, revision)
	}
	c.Assert(rows.Err(), qt.IsNil)
	c.Assert(rows.Close(), qt.IsNil)
	return revisions
}

func readSQLiteRevisionRows(c *qt.C, dbPath string) []revisionRow {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	revisions := readRevisionRows(c, db)
	c.Assert(db.Close(), qt.IsNil)
	return revisions
}

func readSQLiteRevisionType(c *qt.C, dbPath, version string) int {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	var revisionType int
	c.Assert(db.QueryRow(
		"SELECT type FROM atlas_schema_revisions WHERE version = ?", version,
	).Scan(&revisionType), qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	return revisionType
}

func assertStatusCurrent(c *qt.C, binary, dir, dbPath, want string) {
	c.Helper()
	result := runCommand(c, binary,
		"migrate", "status", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	c.Assert(result.stdout, qt.Contains, "-- Current Version: "+want)
}

func assertLintHeader(c *qt.C, binary, dir, latest, want string) {
	c.Helper()
	devURL := "sqlite://" + filepath.Join(c.TempDir(), "dev.db")
	result := runCommand(c, binary,
		"migrate", "lint", "--dir", "file://"+dir+"?format=flyway",
		"--dev-url", devURL, "--latest", latest)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	c.Assert(result.stdout, qt.Contains, want)
}

func postgresSchemaURL(c *qt.C, rawURL, prefix string) (string, func()) {
	c.Helper()
	parsed, err := url.Parse(rawURL)
	c.Assert(err, qt.IsNil)
	schema := "fwrev_" + prefix + "_" + strings.ReplaceAll(filepath.Base(c.TempDir()), "-", "_")
	admin, err := sql.Open("pgx", rawURL)
	c.Assert(err, qt.IsNil)
	quotedSchema := `"` + strings.ReplaceAll(schema, `"`, `""`) + `"`
	_, err = admin.Exec(`CREATE SCHEMA ` + quotedSchema)
	c.Assert(err, qt.IsNil)
	cleanup := func() {
		_, err := admin.Exec(`DROP SCHEMA ` + quotedSchema + ` CASCADE`)
		c.Assert(err, qt.IsNil)
		c.Assert(admin.Close(), qt.IsNil)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String(), cleanup
}

func runCommand(c *qt.C, binary string, args ...string) commandResult {
	c.Helper()
	cmd := exec.Command(binary, args...)
	cmd.Env = environmentWithoutPtahVariables(os.Environ())
	stdout, err := cmd.Output()
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return commandResult{code: exitErr.ExitCode(), stdout: string(stdout), stderr: string(exitErr.Stderr)}
	}
	c.Assert(err, qt.IsNil, qt.Commentf("%s %s", binary, strings.Join(args, " ")))
	return commandResult{stdout: string(stdout)}
}

func environmentWithoutPtahVariables(environment []string) []string {
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

func buildCompatBinary(c *qt.C) string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "ptah-compat")
	result := exec.Command("go", "build", "-o", path, "go.5x5.cz/ptah/cmd/ptah-compat")
	output, err := result.CombinedOutput()
	c.Assert(err, qt.IsNil, qt.Commentf("build ptah-compat: %s", output))
	return path
}

func requireAtlasOracle(t *testing.T) string {
	t.Helper()
	oracle := os.Getenv(oracleEnv)
	if oracle == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s)", oracleEnv, oracleVersion)
	}
	out, err := exec.Command(oracle, "version").Output() // #nosec -- operator-provided pinned oracle path
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", oracleEnv, oracle, err)
	}
	got, _, _ := strings.Cut(string(out), "\n")
	if strings.TrimSpace(got) != oracleVersion {
		t.Fatalf("%s=%s reports %q, want %q", oracleEnv, oracle, strings.TrimSpace(got), oracleVersion)
	}
	return oracle
}
