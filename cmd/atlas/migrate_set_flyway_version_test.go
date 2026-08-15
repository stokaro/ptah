package atlas_test

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"
)

// These tests pin stokaro/ptah#1206 on `ptah-compat migrate set`: a migration
// converted from a Flyway directory is addressed by the version its own file
// spells, not by the int64 ordering key the conversion projects that version
// onto.
//
// Every expectation below is a measurement against the pinned community binary
// v1.3.0 at ptah-atlas-conformance/bin/atlas, run by absolute path on the same
// fixtures, each invocation unpiped with its own exit code read on the next
// line. Before this change:
//
//	migrate set 1                    community exit 0    ptah exit 1
//	migrate set 1.5                  community exit 0    ptah exit 1
//	migrate set 01                   community exit 0    ptah exit 1
//	migrate set 4611686018427469511  community exit 1    ptah exit 0
//
// The last row is the one that made this a parity violation rather than an
// inconvenience: ptah-compat exited 0 where the binary it stands in for exits
// 1, on the only spelling that worked here.

// setFlywayMigration is one file in a Flyway fixture.
type setFlywayMigration struct {
	name string
	body string
}

// setFlywayRow is one revision row, as the version and description columns
// spell it.
type setFlywayRow struct {
	// Exported so quicktest's DeepEquals can compare rows; cmp refuses
	// unexported fields.
	Version     string
	Description string
}

// writeHashedFlywayDir writes a Flyway directory and the atlas.sum that layout
// covers, through the shipped `migrate hash` so the gate these tests pass is
// the one a user's directory passes.
func writeHashedFlywayDir(c *qt.C, migrations []setFlywayMigration) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	for _, migration := range migrations {
		writeAtlasApplyProjectMigration(c, dir, migration.name, migration.body)
	}
	hashConvertedApplyDir(c, dir, "flyway")
	return dir
}

// revisionRows reads the version and description columns in recorded order.
//
// It tolerates a database with no revision table, which is the state a refused
// `migrate set` leaves behind: the refusal precedes the write, but the
// connection has already created the file. The absence is established by
// reading sqlite_master rather than by swallowing a query error, so a table
// that exists and cannot be read still fails the test instead of reporting
// "nothing was recorded".
func revisionRows(c *qt.C, dbPath string) []setFlywayRow {
	c.Helper()
	if _, err := os.Stat(dbPath); err != nil {
		return nil
	}
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(db.Close(), qt.IsNil) })
	if !revisionTableExists(c, db) {
		return nil
	}
	rows, err := db.Query(`SELECT version, description FROM atlas_schema_revisions ORDER BY rowid`)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var out []setFlywayRow
	for rows.Next() {
		var row setFlywayRow
		c.Assert(rows.Scan(&row.Version, &row.Description), qt.IsNil)
		out = append(out, row)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return out
}

func revisionTableExists(c *qt.C, db *sql.DB) bool {
	c.Helper()
	var count int
	c.Assert(db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'atlas_schema_revisions'`,
	).Scan(&count), qt.IsNil)
	return count == 1
}

func flywayRevisionType(c *qt.C, dbPath, version string) int {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var revisionType int
	c.Assert(db.QueryRow(
		`SELECT type FROM atlas_schema_revisions WHERE version = ?`, version,
	).Scan(&revisionType), qt.IsNil)
	return revisionType
}

func sqliteTableExists(c *qt.C, dbPath, table string) bool {
	c.Helper()
	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(db.Close(), qt.IsNil) }()
	var count int
	c.Assert(db.QueryRow(
		`SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table,
	).Scan(&count), qt.IsNil)
	return count == 1
}

// Fixtures. Each is one of the three token shapes stokaro/ptah#1206 measures,
// because the projection collapses each of them differently: a plain token is
// the only one that even looks like an int64, a dotted token cannot be parsed
// as one at all, and a zero-padded token parses to a DIFFERENT string than the
// one the community binary records.
var (
	setFlywayPlain = []setFlywayMigration{
		{name: "V1.sql", body: "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__ok.sql", body: "CREATE TABLE gadgets (id INTEGER PRIMARY KEY);\n"},
	}
	setFlywayDotted = []setFlywayMigration{
		{name: "V1__a.sql", body: "CREATE TABLE a (id INTEGER PRIMARY KEY);\n"},
		{name: "V1.5__b.sql", body: "CREATE TABLE b (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__c.sql", body: "CREATE TABLE c (id INTEGER PRIMARY KEY);\n"},
	}
	setFlywayZeroPadded = []setFlywayMigration{
		{name: "V01__a.sql", body: "CREATE TABLE a (id INTEGER PRIMARY KEY);\n"},
		{name: "V2__b.sql", body: "CREATE TABLE b (id INTEGER PRIMARY KEY);\n"},
	}
)

// TestCompatMigrateSet_ConvertedFlywayTakesTheSourceToken is the discriminator.
//
// Reverting resolveAtlasMigrateSetVersion to the int64 parsing it had turns
// every accepted row here into `Error: migration with version "1" not found`
// (or, on the dotted fixture, `--version "1.5" is not a valid migration
// version: strconv.ParseInt: parsing "1.5": invalid syntax`), and turns the
// ordering-key row from a refusal back into a success.
func TestCompatMigrateSet_ConvertedFlywayTakesTheSourceToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		migrations []setFlywayMigration
		operand    string
		wantStdout string
		wantErr    string
		wantRows   []setFlywayRow
	}{
		{
			name:       "plain token reaches the first migration",
			migrations: setFlywayPlain,
			operand:    "1",
			wantStdout: "Current version is 1 (1 set):\n\n  + 1\n\n",
			wantRows:   []setFlywayRow{{Version: "1", Description: ""}},
		},
		{
			name:       "plain token reaches the last migration and sets both",
			migrations: setFlywayPlain,
			operand:    "2",
			wantStdout: "Current version is 2 (2 set):\n\n  + 1\n  + 2 (ok)\n\n",
			wantRows:   []setFlywayRow{{Version: "1", Description: ""}, {Version: "2", Description: "ok"}},
		},
		{
			name:       "dotted token is addressable at all",
			migrations: setFlywayDotted,
			operand:    "1.5",
			wantStdout: "Current version is 1.5 (2 set):\n\n  + 1 (a)\n  + 1.5 (b)\n\n",
			wantRows:   []setFlywayRow{{Version: "1", Description: "a"}, {Version: "1.5", Description: "b"}},
		},
		{
			name:       "zero-padded token is matched byte for byte",
			migrations: setFlywayZeroPadded,
			operand:    "01",
			wantStdout: "Current version is 01 (1 set):\n\n  + 01 (a)\n\n",
			wantRows:   []setFlywayRow{{Version: "01", Description: "a"}},
		},
		{
			name:       "a zero-padded token is not reachable by its numeric value",
			migrations: setFlywayZeroPadded,
			operand:    "1",
			wantErr:    `migration with version "1" not found`,
		},
		{
			name:       "a plain token is not reachable by a zero-padded spelling",
			migrations: setFlywayPlain,
			operand:    "01",
			wantErr:    `migration with version "01" not found`,
		},
		{
			name:       "a dotted token is not reachable with the separator dropped",
			migrations: setFlywayDotted,
			operand:    "15",
			wantErr:    `migration with version "15" not found`,
		},
		{
			// The parity violation. This spelling exited 0 here and exits 1 on
			// the community binary, which is the one direction that is never
			// allowed.
			name:       "the ordering key is no longer an accepted spelling",
			migrations: setFlywayPlain,
			operand:    "4611686018427469511",
			wantErr:    `migration with version "4611686018427469511" not found`,
		},
		{
			// Ptah answered `--version must be greater than zero` and
			// `--version "abc" is not a valid migration version: ...`, both
			// diagnostics about int64 parsing on a directory whose versions are
			// not int64s.
			name:       "zero is a token this directory does not carry",
			migrations: setFlywayPlain,
			operand:    "0",
			wantErr:    `migration with version "0" not found`,
		},
		{
			name:       "a non-numeric operand is a token like any other",
			migrations: setFlywayPlain,
			operand:    "abc",
			wantErr:    `migration with version "abc" not found`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedFlywayDir(c, test.migrations)
			dbPath := filepath.Join(filepath.Dir(dir), "set.db")

			stdout, stderr, err := runCompatExit(
				"migrate", "set", test.operand,
				"--dir", "file://"+dir+"?format=flyway",
				"--url", "sqlite://"+dbPath,
			)

			c.Assert(errorText(err), qt.Equals, test.wantErr)
			c.Assert(stdout, qt.Equals, test.wantStdout)
			c.Assert(stderr, qt.Equals, errorLine(test.wantErr))
			c.Assert(revisionRows(c, dbPath), qt.DeepEquals, test.wantRows)
		})
	}
}

// TestCompatMigrateSet_PlainPrefixLayoutsAreUnchanged is the control that
// separates "converted directories are addressed by token" (false) from "the
// Flyway projection is addressed by token" (true).
//
// golang-migrate, goose, dbmate and liquibase carry a plain numeric prefix, so
// their token IS the int64 they convert to and the projection is the identity
// on them. The single layout gate lives inside
// atlasmigrateimport.FlywayCoveredSourceVersions, so routing a plain-prefix
// layout through the token path would show up here as a changed diagnostic.
//
// Measured on the community binary v1.3.0 on the same directory: `migrate set
// 1` exits 0 with `Current version is 1 (1 set):` and `+ 1 (init)`, and the
// revision row is `1|init` on both binaries.
func TestCompatMigrateSet_PlainPrefixLayoutsAreUnchanged(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := filepath.Join(c.TempDir(), "migrations")
	writeAtlasApplyProjectMigration(c, dir, "1_init.up.sql",
		"CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n")
	writeAtlasApplyProjectMigration(c, dir, "1_init.down.sql", "DROP TABLE widgets;\n")
	hashConvertedApplyDir(c, dir, "golang-migrate")
	dbPath := filepath.Join(filepath.Dir(dir), "gm.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", "1",
		"--dir", "file://"+dir+"?format=golang-migrate",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "Current version is 1 (1 set):\n\n  + 1 (init)\n\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals,
		[]setFlywayRow{{Version: "1", Description: "init"}})
}

// TestCompatMigrateSet_FlywayBaselineReadsSquashedExactHistory pins the full
// identity map used while revision rows are scanned. The operand index remains
// the surviving directory only: retired V1.5 is readable and renderable, but
// cannot become pending work or an addressable set target.
func TestCompatMigrateSet_FlywayBaselineReadsSquashedExactHistory(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	initialDir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__old.sql", body: "CREATE TABLE old_history (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(initialDir), "baseline.db")
	_, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+initialDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	baselineDir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V1.5__old.sql", body: "CREATE TABLE old_history (id INTEGER PRIMARY KEY);\n"},
		{name: "B2__base.sql", body: "CREATE TABLE baseline_state (id INTEGER PRIMARY KEY);\n"},
	})
	stdout, stderr, err := runCompatExit(
		"migrate", "set", "2",
		"--dir", "file://"+baselineDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "Current version is 2 (1 set):\n\n  + 2 (base)\n\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []setFlywayRow{
		{Version: "1.5", Description: "old"},
		{Version: "2", Description: "base"},
	})

	_, stderr, err = runCompatExit(
		"migrate", "set", "1.5",
		"--dir", "file://"+baselineDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(errorText(err), qt.Equals, `migration with version "1.5" not found`)
	c.Assert(stderr, qt.Equals, "Error: migration with version \"1.5\" not found\n")
}

// TestCompatMigrateSet_RemovesRetiredExactHistoryAboveTarget pins the source
// identity ordering used when the current directory no longer contains a
// recorded migration. Measured on Atlas CE v1.3.0: after V2 is applied and
// replaced on disk by V1, `migrate set 1` records V1 and removes V2. Mapping
// the retired row to runtime zero used to leave V2 silently applied.
func TestCompatMigrateSet_RemovesRetiredExactHistoryAboveTarget(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__two.sql", body: "CREATE TABLE retired_v2 (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "retired-above-target.db")
	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	c.Assert(os.Remove(filepath.Join(dir, "V2__two.sql")), qt.IsNil)
	writeAtlasApplyProjectMigration(c, dir, "V1__one.sql",
		"CREATE TABLE current_v1 (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", "1",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals,
		"Current version is 1 (1 set, 1 removed):\n\n  + 1 (one)\n  - 2 (two)\n\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals,
		[]setFlywayRow{{Version: "1", Description: "one"}})
}

// TestCompatMigrateSet_KeepsRetiredExactHistoryBelowMultiDigitTarget proves
// that metadata movement uses Flyway's numeric component order rather than the
// byte order of the persisted source tokens. In byte order "9" sorts after
// "10", which used to delete the already-applied V9 row while setting V10.
func TestCompatMigrateSet_KeepsRetiredExactHistoryBelowMultiDigitTarget(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V9__nine.sql", body: "CREATE TABLE retired_v9 (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "retired-below-multidigit-target.db")
	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	c.Assert(os.Remove(filepath.Join(dir, "V9__nine.sql")), qt.IsNil)
	writeAtlasApplyProjectMigration(c, dir, "V10__ten.sql",
		"CREATE TABLE current_v10 (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", "10",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals,
		"Current version is 10 (1 set):\n\n  + 10 (ten)\n\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []setFlywayRow{
		{Version: "9", Description: "nine"},
		{Version: "10", Description: "ten"},
	})
}

// TestCompatMigrateSet_OrdersRetiredHistoryByKnownFlywayRole proves that a
// source token does not erase the file role that established its execution
// position. A retired baseline ran before every versioned migration even when
// its numeric token is larger; a retired versioned migration keeps ordinary
// numeric component order.
func TestCompatMigrateSet_OrdersRetiredHistoryByKnownFlywayRole(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		retiredFile string
		wantStdout  string
		wantRows    []setFlywayRow
	}{
		{
			name:        "baseline 20 remains below versioned 10",
			retiredFile: "B20__twenty.sql",
			wantStdout:  "Current version is 10 (1 set):\n\n  + 10 (ten)\n\n",
			wantRows: []setFlywayRow{
				{Version: "20", Description: "twenty"},
				{Version: "10", Description: "ten"},
			},
		},
		{
			name:        "versioned 20 remains above versioned 10",
			retiredFile: "V20__twenty.sql",
			wantStdout: "Current version is 10 (1 set, 1 removed):\n\n" +
				"  + 10 (ten)\n  - 20 (twenty)\n\n",
			wantRows: []setFlywayRow{{Version: "10", Description: "ten"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedFlywayDir(c, []setFlywayMigration{{
				name: test.retiredFile,
				body: "CREATE TABLE retired_twenty (id INTEGER PRIMARY KEY);\n",
			}})
			dbPath := filepath.Join(filepath.Dir(dir), "retired-role.db")
			_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
			c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

			c.Assert(os.Remove(filepath.Join(dir, test.retiredFile)), qt.IsNil)
			writeAtlasApplyProjectMigration(c, dir, "V10__ten.sql",
				"CREATE TABLE current_v10 (id INTEGER PRIMARY KEY);\n")
			hashConvertedApplyDir(c, dir, "flyway")

			stdout, stderr, err := runCompatExit(
				"migrate", "set", "10",
				"--dir", "file://"+dir+"?format=flyway",
				"--url", "sqlite://"+dbPath,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
			c.Assert(stdout, qt.Equals, test.wantStdout)
			c.Assert(revisionRows(c, dbPath), qt.DeepEquals, test.wantRows)
		})
	}
}

// TestCompatMigrateSet_OrdersRetiredBaselinesByRawToken proves that baseline
// rotation uses the same raw-token order as Flyway's baseline selection. A
// retired B2 remains when B3 is selected, while a retired B20 is removed when
// B10 is selected. Set changes metadata only and executes neither baseline.
func TestCompatMigrateSet_OrdersRetiredBaselinesByRawToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		retiredFile string
		targetFile  string
		target      string
		wantStdout  string
		wantRows    []setFlywayRow
	}{
		{
			name:        "retired B2 remains below target B3",
			retiredFile: "B2__two.sql",
			targetFile:  "B3__three.sql",
			target:      "3",
			wantStdout:  "Current version is 3 (1 set):\n\n  + 3 (three)\n\n",
			wantRows: []setFlywayRow{
				{Version: "2", Description: "two"},
				{Version: "3", Description: "three"},
			},
		},
		{
			name:        "retired B20 is removed above target B10",
			retiredFile: "B20__twenty.sql",
			targetFile:  "B10__ten.sql",
			target:      "10",
			wantStdout: "Current version is 10 (1 set, 1 removed):\n\n" +
				"  + 10 (ten)\n  - 20 (twenty)\n\n",
			wantRows: []setFlywayRow{{Version: "10", Description: "ten"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			dir := writeHashedFlywayDir(c, []setFlywayMigration{{
				name: test.retiredFile,
				body: "CREATE TABLE retired_baseline (id INTEGER PRIMARY KEY);\n",
			}})
			dbPath := filepath.Join(filepath.Dir(dir), "retired-baseline-order.db")
			_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
			c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

			c.Assert(os.Remove(filepath.Join(dir, test.retiredFile)), qt.IsNil)
			writeAtlasApplyProjectMigration(c, dir, test.targetFile,
				"CREATE TABLE target_baseline (id INTEGER PRIMARY KEY);\n")
			hashConvertedApplyDir(c, dir, "flyway")

			stdout, stderr, err := runCompatExit(
				"migrate", "set", test.target,
				"--dir", "file://"+dir+"?format=flyway",
				"--url", "sqlite://"+dbPath,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
			c.Assert(stdout, qt.Equals, test.wantStdout)
			c.Assert(revisionRows(c, dbPath), qt.DeepEquals, test.wantRows)
			c.Assert(sqliteTableExists(c, dbPath, "retired_baseline"), qt.IsTrue)
			c.Assert(sqliteTableExists(c, dbPath, "target_baseline"), qt.IsFalse)
		})
	}
}

// TestCompatMigrateSet_TargetBaselineKeepsHistoryItsRawTokenCutSquashes
// proves that metadata movement uses the same raw-token cut as Flyway baseline
// selection. Although numeric output order puts V10 after V2, a B2 baseline
// squashes V10 because "10" sorts before "2" as a source token. The retired
// row therefore remains applied when B2 becomes the selected metadata target.
func TestCompatMigrateSet_TargetBaselineKeepsHistoryItsRawTokenCutSquashes(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{{
		name: "V10__ten.sql",
		body: "CREATE TABLE retired_v10 (id INTEGER PRIMARY KEY);\n",
	}})
	dbPath := filepath.Join(filepath.Dir(dir), "retired-version-below-baseline.db")
	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	c.Assert(os.Remove(filepath.Join(dir, "V10__ten.sql")), qt.IsNil)
	writeAtlasApplyProjectMigration(c, dir, "B2__base.sql",
		"CREATE TABLE target_baseline (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", "2",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "Current version is 2 (1 set):\n\n  + 2 (base)\n\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []setFlywayRow{
		{Version: "10", Description: "ten"},
		{Version: "2", Description: "base"},
	})
	c.Assert(sqliteTableExists(c, dbPath, "retired_v10"), qt.IsTrue)
	c.Assert(sqliteTableExists(c, dbPath, "target_baseline"), qt.IsFalse)
}

// TestCompatMigrateSet_TargetRepeatableKeepsRetiredVersionedHistory proves
// that set preserves the source role of a converted empty-identity target. A
// repeatable follows every versioned migration, so retired V1 remains applied
// when the single R__ migration becomes the metadata target. Neither body is
// re-executed by the set operation.
func TestCompatMigrateSet_TargetRepeatableKeepsRetiredVersionedHistory(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{{
		name: "V1__one.sql",
		body: "CREATE TABLE retired_v1 (id INTEGER PRIMARY KEY);\n",
	}})
	dbPath := filepath.Join(filepath.Dir(dir), "retired-version-before-repeatable.db")
	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	c.Assert(os.Remove(filepath.Join(dir, "V1__one.sql")), qt.IsNil)
	writeAtlasApplyProjectMigration(c, dir, "R__only.sql",
		"CREATE TABLE target_repeatable (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", "",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "Current version is \"\" (1 set):\n\n  + \"\" (only)\n\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []setFlywayRow{
		{Version: "1", Description: "one"},
		{Version: "", Description: "only"},
	})
	c.Assert(sqliteTableExists(c, dbPath, "retired_v1"), qt.IsTrue)
	c.Assert(sqliteTableExists(c, dbPath, "target_repeatable"), qt.IsFalse)
}

// TestCompatMigrateSet_RefusesRetiredHistoryWithoutFlywayRole covers rows
// written by Atlas CE, whose ordinary applied type does not distinguish B20
// from V20. The token alone is insufficient: treating every "20" as V20 would
// corrupt a retired B20, while treating every one as B20 would preserve a
// retired V20 above the selected target. The metadata operation must refuse
// without changing either row.
func TestCompatMigrateSet_RefusesRetiredHistoryWithoutFlywayRole(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{{
		name: "B20__twenty.sql",
		body: "CREATE TABLE retired_ce_twenty (id INTEGER PRIMARY KEY);\n",
	}})
	dbPath := filepath.Join(filepath.Dir(dir), "retired-ce-role.db")
	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	db, err := sql.Open("sqlite", dbPath)
	c.Assert(err, qt.IsNil)
	_, err = db.Exec(
		"UPDATE atlas_schema_revisions SET type = 2, operator_version = 'Atlas CLI v1.3.0' WHERE version = '20'",
	)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	c.Assert(os.Remove(filepath.Join(dir, "B20__twenty.sql")), qt.IsNil)
	writeAtlasApplyProjectMigration(c, dir, "V10__ten.sql",
		"CREATE TABLE current_v10 (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", "10",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	want := "cannot set Atlas revision: source order between retired exact identity \"20\" and target \"10\" is ambiguous"
	c.Assert(errorText(err), qt.Equals, want)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals,
		[]setFlywayRow{{Version: "20", Description: "twenty"}})
}

func TestCompatMigrateSet_RefusesAmbiguousRetiredSourceOrder(t *testing.T) {
	t.Parallel()
	c := qt.New(t)

	assertCompatMigrateSetRetiredSourceOrderAmbiguous(
		c,
		"V01__old.sql",
		"01",
		"V1__target.sql",
		"1",
	)
	assertCompatMigrateSetRetiredSourceOrderAmbiguous(
		c,
		"Vx__old.sql",
		"x",
		"Vy__target.sql",
		"y",
	)
}

func assertCompatMigrateSetRetiredSourceOrderAmbiguous(
	c *qt.C,
	retiredFile, retiredToken, targetFile, targetToken string,
) {
	c.Helper()
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: retiredFile, body: "CREATE TABLE retired_ambiguous (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "retired-ambiguous.db")
	_, stderr, err := compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	c.Assert(os.Remove(filepath.Join(dir, retiredFile)), qt.IsNil)
	writeAtlasApplyProjectMigration(c, dir, targetFile,
		"CREATE TABLE current_ambiguous (id INTEGER PRIMARY KEY);\n")
	hashConvertedApplyDir(c, dir, "flyway")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", targetToken,
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	want := fmt.Sprintf(
		"cannot set Atlas revision: source order between retired exact identity %q and target %q is ambiguous",
		retiredToken,
		targetToken,
	)
	c.Assert(errorText(err), qt.Equals, want)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals,
		[]setFlywayRow{{Version: retiredToken, Description: "old"}})
}

// TestCompatMigrateSet_FlywayBaselineMatchesExistingExactIdentity prevents a
// surviving baseline from inserting a duplicate row when its lower runtime
// key shares the exact token of previously applied versioned history.
func TestCompatMigrateSet_FlywayBaselineMatchesExistingExactIdentity(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	initialDir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__old.sql", body: "CREATE TABLE old_v2 (id INTEGER PRIMARY KEY);\n"},
		{name: "V3__tail.sql", body: "CREATE TABLE tail_v3 (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(initialDir), "same-token.db")
	_, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+initialDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	baselineDir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__old.sql", body: "CREATE TABLE old_v2 (id INTEGER PRIMARY KEY);\n"},
		{name: "B2__old.sql", body: "CREATE TABLE baseline_v2 (id INTEGER PRIMARY KEY);\n"},
		{name: "V3__tail.sql", body: "CREATE TABLE tail_v3 (id INTEGER PRIMARY KEY);\n"},
	})
	stdout, stderr, err := runCompatExit(
		"migrate", "set", "3",
		"--dir", "file://"+baselineDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals, []setFlywayRow{
		{Version: "2", Description: "old"},
		{Version: "3", Description: "tail"},
	})

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+baselineDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("status stderr: %s", stderr))
	c.Assert(stdout, qt.Contains, "  -- Executed Files:  2\n")
	c.Assert(stdout, qt.Contains, "  -- Pending Files:   0\n")
}

func TestCompatMigrateSet_FlywayBaselineTargetKeepsExistingExactIdentity(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	initialDir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__old.sql", body: "CREATE TABLE target_old_v2 (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(initialDir), "target-same-token.db")
	_, stderr, err := runCompatExit(
		"migrate", "apply",
		"--dir", "file://"+initialDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("apply stderr: %s", stderr))

	baselineDir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__old.sql", body: "CREATE TABLE target_old_v2 (id INTEGER PRIMARY KEY);\n"},
		{name: "B2__old.sql", body: "CREATE TABLE target_baseline_v2 (id INTEGER PRIMARY KEY);\n"},
	})
	stdout, stderr, err := runCompatExit(
		"migrate", "set", "2",
		"--dir", "file://"+baselineDir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(revisionRows(c, dbPath), qt.DeepEquals,
		[]setFlywayRow{{Version: "2", Description: "old"}})
}

func TestCompatMigrateSet_FlywayBaselineMarkerSettlesLaterApply(t *testing.T) {
	t.Parallel()
	c := qt.New(t)
	dir := writeHashedFlywayDir(c, []setFlywayMigration{
		{name: "V2__base.sql", body: "CREATE TABLE set_baseline_marker (id INTEGER PRIMARY KEY);\n"},
		{name: "B2__base.sql", body: "CREATE TABLE set_baseline_marker (id INTEGER PRIMARY KEY);\n"},
	})
	dbPath := filepath.Join(filepath.Dir(dir), "set-baseline-marker.db")

	stdout, stderr, err := runCompatExit(
		"migrate", "set", "2",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("set stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "Current version is 2 (1 set):\n\n  + 2 (base)\n\n")
	c.Assert(flywayRevisionType(c, dbPath, "2"), qt.Equals, 7)

	stdout, stderr, err = compatApplyConverted(dir, "flyway", dbPath)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
	c.Assert(stdout, qt.Contains, "No migration files to execute")

	stdout, stderr, err = runCompatExit(
		"migrate", "status",
		"--dir", "file://"+dir+"?format=flyway",
		"--url", "sqlite://"+dbPath,
		"--format", `{{ (index .Applied 0).Type }}`,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("status stderr: %s", stderr))
	c.Assert(stdout, qt.Equals, "manually set")
}

// errorLine renders the stderr a refusal writes, so the expectation is derived
// from the message rather than repeated beside it.
func errorLine(message string) string {
	if message == "" {
		return ""
	}
	return "Error: " + message + "\n"
}
