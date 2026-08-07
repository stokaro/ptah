package atlas_test

import (
	"database/sql"
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
			wantRows:   []setFlywayRow{{Description: ""}},
		},
		{
			name:       "plain token reaches the last migration and sets both",
			migrations: setFlywayPlain,
			operand:    "2",
			wantStdout: "Current version is 2 (2 set):\n\n  + 1\n  + 2 (ok)\n\n",
			wantRows:   []setFlywayRow{{Description: ""}, {Description: "ok"}},
		},
		{
			name:       "dotted token is addressable at all",
			migrations: setFlywayDotted,
			operand:    "1.5",
			wantStdout: "Current version is 1.5 (2 set):\n\n  + 1 (a)\n  + 1.5 (b)\n\n",
			wantRows:   []setFlywayRow{{Description: "a"}, {Description: "b"}},
		},
		{
			name:       "zero-padded token is matched byte for byte",
			migrations: setFlywayZeroPadded,
			operand:    "01",
			wantStdout: "Current version is 01 (1 set):\n\n  + 01 (a)\n\n",
			wantRows:   []setFlywayRow{{Description: "a"}},
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
			// The descriptions are asserted here rather than the versions: the
			// version column still holds the ordering key, which is the half of
			// stokaro/ptah#1206 this change does not settle. Row COUNT and
			// description are what say which migrations were recorded.
			c.Assert(revisionDescriptions(revisionRows(c, dbPath)), qt.DeepEquals,
				revisionDescriptions(test.wantRows))
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

// errorLine renders the stderr a refusal writes, so the expectation is derived
// from the message rather than repeated beside it.
func errorLine(message string) string {
	if message == "" {
		return ""
	}
	return "Error: " + message + "\n"
}

// revisionDescriptions projects rows onto the column this change settles.
func revisionDescriptions(rows []setFlywayRow) []string {
	out := make([]string, 0, len(rows))
	for _, row := range rows {
		out = append(out, row.Description)
	}
	return out
}
