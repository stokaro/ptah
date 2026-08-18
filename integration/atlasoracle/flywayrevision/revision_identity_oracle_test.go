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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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

// TestFlywayCrossToolReuseContinuesEitherHistory pins the outcome
// stokaro/ptah#1209 exists for: a Flyway revision history written by either
// binary is continued by the other with no hand-over step.
//
// Before that issue this direction was asymmetric. Ptah read a history the
// community binary wrote, agreed about every identity, and then refused to
// apply into it with a checksum mismatch: the community binary stores the
// atlas.sum h1, Ptah stored the hex SHA-256 of the up SQL, because converting a
// Flyway directory rebuilds it with no integrity file and the source hash was
// dropped on the way. The refusal was the safe answer to a question that should
// never have been asked.
func TestFlywayCrossToolReuseContinuesEitherHistory(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, test := range interoperableIdentityCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeHashedFlywayDir(c, oracle, test.files)
			for _, direction := range []struct {
				name   string
				first  string
				second string
			}{
				{name: "Atlas CE then Ptah", first: oracle, second: compat},
				{name: "Ptah then Atlas CE", first: compat, second: oracle},
			} {
				t.Run(direction.name, func(t *testing.T) {
					c := qt.New(t)
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
		t.Run(direction.name, func(t *testing.T) {
			c := qt.New(t)
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

// assertCrossToolReuseResult requires the same answer in both directions: the
// second binary has nothing to do, and says so.
//
// It takes the direction so a future asymmetry has somewhere to be written
// down, and asserts the direction is one it knows rather than passing an
// unrecognized one silently.
func assertCrossToolReuseResult(c *qt.C, direction string, result commandResult) {
	c.Helper()
	c.Assert(
		direction == "Atlas CE then Ptah" || direction == "Ptah then Atlas CE",
		qt.IsTrue, qt.Commentf("unknown cross-tool direction %q", direction),
	)
	c.Assert(result.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
	c.Assert(result.stdout, qt.Equals, "No migration files to execute\n")
	c.Assert(result.stderr, qt.Equals, "")
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
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
	out, err := exec.Command(oracle, "version").Output() // #nosec G204 G702 -- operator-provided pinned oracle path
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", oracleEnv, oracle, err)
	}
	got, _, _ := strings.Cut(string(out), "\n")
	if strings.TrimSpace(got) != oracleVersion {
		t.Fatalf("%s=%s reports %q, want %q", oracleEnv, oracle, strings.TrimSpace(got), oracleVersion)
	}
	return oracle
}

// TestFlywayContinuesAtlasCEHistoryWithoutHandover walks the whole user flow
// stokaro/ptah#1209 describes, in one test, for every interoperable token
// shape: the community binary applies a directory, Ptah continues it, and the
// community binary reads the result back.
//
// The steps are the issue's own numbered definition of done. The one worth
// naming is step 4: `migrate apply` with nothing pending has to be a clean
// no-op rather than a refusal, because that is the state a user lands in the
// moment they point Ptah at an existing database, and a refusal there is what
// used to force `migrate set`.
//
// The last step is the reverse direction the issue calls desirable rather than
// required. It is asserted rather than merely recorded: a history Ptah appended
// to stays readable by the binary that started it.
func TestFlywayContinuesAtlasCEHistoryWithoutHandover(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	for _, test := range linearlyExtendableIdentityCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeHashedFlywayDir(c, oracle, test.files)
			dbPath := filepath.Join(c.TempDir(), "handover.db")

			// 1-3: the community binary applies the directory, and Ptah reads
			// every one of its migrations back as already applied.
			assertSQLiteApplyIdentity(c, oracle, dir, dbPath, test.wantRows)
			applied := readSQLiteRevisionRows(c, dbPath)

			// 4: nothing pending, so nothing runs and nothing is rewritten.
			settled := runCommand(c, compat,
				"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
			c.Assert(settled.code, qt.Equals, 0,
				qt.Commentf("stdout: %s\nstderr: %s", settled.stdout, settled.stderr))
			c.Assert(settled.stdout, qt.Equals, "No migration files to execute\n")
			c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, applied)

			// 5-6: a new migration is added, and Ptah applies only that one.
			appended := append(slices.Clone(test.files), migrationFile{
				name: "V9000__handover_appended.sql",
				body: "CREATE TABLE handover_appended (id INTEGER PRIMARY KEY);\n",
			})
			extended := writeHashedFlywayDir(c, oracle, appended)
			applyNew := runCommand(c, compat,
				"migrate", "apply", "--dir", "file://"+extended+"?format=flyway", "--url", "sqlite://"+dbPath)
			c.Assert(applyNew.code, qt.Equals, 0,
				qt.Commentf("stdout: %s\nstderr: %s", applyNew.stdout, applyNew.stderr))
			c.Assert(applyNew.stdout, qt.Contains, "1 pending migrations")
			c.Assert(readSQLiteRevisionRows(c, dbPath), qt.HasLen, len(applied)+1)

			// 7: and a second apply performs no SQL.
			again := runCommand(c, compat,
				"migrate", "apply", "--dir", "file://"+extended+"?format=flyway", "--url", "sqlite://"+dbPath)
			c.Assert(again.code, qt.Equals, 0,
				qt.Commentf("stdout: %s\nstderr: %s", again.stdout, again.stderr))
			c.Assert(again.stdout, qt.Equals, "No migration files to execute\n")

			// 8, and the reverse direction: the community binary still reads
			// the history Ptah extended, and has nothing of its own to do.
			back := runCommand(c, oracle,
				"migrate", "apply", "--dir", "file://"+extended+"?format=flyway", "--url", "sqlite://"+dbPath)
			c.Assert(back.code, qt.Equals, 0,
				qt.Commentf("stdout: %s\nstderr: %s", back.stdout, back.stderr))
			c.Assert(back.stdout, qt.Equals, "No migration files to execute\n")
		})
	}
}

// linearlyExtendableIdentityCases are the fixtures a strictly greater numeric
// token can be appended to.
//
// They are exactly the three shapes stokaro/ptah#1209's definition of done
// names: a plain numeric token, a dotted one, and a zero-padded one. The others
// are excluded because appending to them tests linearity rather than
// continuation -- a directory whose newest token is non-numeric, or which holds
// only a repeatable, treats any numeric append as out of order, and BOTH
// binaries refuse it with an exec-order diagnostic that has nothing to do with
// which history wrote the rows.
//
// Steps 1 to 4 and the reverse-readability step are covered for every shape by
// [TestFlywayCrossToolReuseContinuesEitherHistory]; only the append half is
// narrowed here.
func linearlyExtendableIdentityCases() []identityCase {
	extendable := []string{"plain", "dotted", "zero padded"}
	return slices.DeleteFunc(interoperableIdentityCases(), func(test identityCase) bool {
		return !slices.Contains(extendable, test.name)
	})
}

// TestFlywayOutOfOrderInsertionStaysRefusedOnBothSides is the narrowing the
// issue permits, measured rather than assumed.
//
// An atlas.sum h1 is a CUMULATIVE chain over every preceding file, so inserting
// a migration between two applied ones silently changes the recorded hash of
// everything after it. Measured: with V1 and V3 applied, adding V2 moves V3's
// h1, and the row the community binary wrote keeps the pre-insert value.
//
// Neither binary continues from there, so this is not a compatibility gap and
// nothing here should be widened to accept it. They refuse for different
// stated reasons, which is worth pinning: the community binary names the
// out-of-order file, and Ptah reports the checksum the chain moved.
func TestFlywayOutOfOrderInsertionStaysRefusedOnBothSides(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V1__ooo_first.sql", body: "CREATE TABLE ooo_first (id INTEGER PRIMARY KEY);\n"},
		{name: "V3__ooo_third.sql", body: "CREATE TABLE ooo_third (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "out-of-order.db")
	assertSQLiteApplyIdentity(c, oracle, dir, dbPath, []revisionRow{
		{Version: "1", Description: "ooo_first"},
		{Version: "3", Description: "ooo_third"},
	})
	before := readSQLiteRevisionRows(c, dbPath)

	c.Assert(os.WriteFile(filepath.Join(dir, "V2__ooo_second.sql"),
		[]byte("CREATE TABLE ooo_second (id INTEGER PRIMARY KEY);\n"), 0o600), qt.IsNil)
	rehash := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
	c.Assert(rehash.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", rehash.stdout, rehash.stderr))

	for _, binary := range []struct {
		name   string
		path   string
		reason string
	}{
		{name: "Atlas CE", path: oracle, reason: "out of order"},
		{name: "Ptah", path: compat, reason: "checksum mismatch"},
	} {
		t.Run(binary.name, func(t *testing.T) {
			c := qt.New(t)

			result := runCommand(c, binary.path,
				"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)

			c.Assert(result.code, qt.Equals, 1,
				qt.Commentf("stdout: %s\nstderr: %s", result.stdout, result.stderr))
			c.Assert(result.stderr, qt.Contains, binary.reason)
			c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, before)
		})
	}
}

// TestFlywayEditedAppliedFileStaysRefusedAfterCEWroteIt is the control on
// stokaro/ptah#1209: accepting the community binary's checksum encoding must
// not accept a file whose bytes changed after it ran.
//
// The two are easy to conflate, because both surface as the same message. The
// difference is which side moved. A CE-written row holds the source atlas.sum
// h1 for bytes that are still on disk, and continuing from it is the point of
// #1209. An edited applied file has neither hash: the source h1 moved with the
// edit and the content digest never matched. Ptah refuses, and that refusal is
// the deliberate divergence documented in the retained-divergences page — the
// community binary records the checksum without comparing it, so the same edit
// is a no-op there.
func TestFlywayEditedAppliedFileStaysRefusedAfterCEWroteIt(t *testing.T) {
	oracle := requireAtlasOracle(t)
	c := qt.New(t)
	compat := buildCompatBinary(c)

	dir := writeHashedFlywayDir(c, oracle, []migrationFile{
		{name: "V1__edited.sql", body: "CREATE TABLE edited_applied (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(c.TempDir(), "edited.db")
	assertSQLiteApplyIdentity(c, oracle, dir, dbPath, []revisionRow{
		{Version: "1", Description: "edited"},
	})
	before := readSQLiteRevisionRows(c, dbPath)

	c.Assert(os.WriteFile(filepath.Join(dir, "V1__edited.sql"),
		[]byte("CREATE TABLE edited_applied (id INTEGER PRIMARY KEY, note TEXT);\n"), 0o600), qt.IsNil)
	rehash := runCommand(c, oracle, "migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
	c.Assert(rehash.code, qt.Equals, 0, qt.Commentf("stdout: %s\nstderr: %s", rehash.stdout, rehash.stderr))

	refused := runCommand(c, compat,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)

	c.Assert(refused.code, qt.Equals, 1,
		qt.Commentf("stdout: %s\nstderr: %s", refused.stdout, refused.stderr))
	c.Assert(refused.stderr, qt.Contains, "checksum mismatch")
	c.Assert(readSQLiteRevisionRows(c, dbPath), qt.DeepEquals, before)

	// The divergence half: the community binary records the checksum without
	// comparing it, so the same edited directory is a no-op there.
	accepted := runCommand(c, oracle,
		"migrate", "apply", "--dir", "file://"+dir+"?format=flyway", "--url", "sqlite://"+dbPath)
	c.Assert(accepted.code, qt.Equals, 0,
		qt.Commentf("stdout: %s\nstderr: %s", accepted.stdout, accepted.stderr))
	c.Assert(accepted.stdout, qt.Equals, "No migration files to execute\n")
}
