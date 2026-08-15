package atlas_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/migration/migrator"
)

// These tests pin stokaro/ptah#976: on a native Atlas directory the files
// ptah-compat EXECUTES are exactly the files atlas.sum COVERS.
//
// Before the fix the two sets were computed by different code. The hasher used
// a shallow, case-sensitive `*.sql` glob — byte-identical to the community
// binary — while the loader walked the whole tree case-insensitively, so a
// migration in a subdirectory ran without any checksum reaching it. `migrate
// validate` reported such a directory clean, and editing that file afterwards
// changed what ran without changing any hash. The headline case below tampers
// with exactly that file and asserts the table it adds never appears.

// sqliteUserTables returns the non-Atlas tables in dbPath, sorted. Asserting the
// whole set rather than one name is what separates "the nested file did not run"
// from "the nested file ran and happened not to create the table I looked for".
func sqliteUserTables(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.Query(
		"SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'atlas_%' ORDER BY name")
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	tables := []string{}
	for rows.Next() {
		var name string
		c.Assert(rows.Scan(&name), qt.IsNil)
		tables = append(tables, name)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return tables
}

// coveredSetFile is one file a row lays down below the migration directory,
// written in row order so a nested name and its top-level twin always arrive
// the same way round.
type coveredSetFile struct {
	name string
	body string
}

// writeCoveredSetFile writes one file below dir, creating parents.
func writeCoveredSetFile(c *qt.C, dir, name, body string) {
	c.Helper()
	path := filepath.Join(dir, filepath.FromSlash(name))
	c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
}

// appendCoveredSetFile appends to an existing file below dir, which is how a
// tamper is expressed: the file's name and position never change, only bytes
// that no entry in atlas.sum is derived from.
func appendCoveredSetFile(c *qt.C, dir, name, body string) {
	c.Helper()
	path := filepath.Join(dir, filepath.FromSlash(name))
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	c.Assert(err, qt.IsNil)
	_, err = file.WriteString(body)
	c.Assert(err, qt.IsNil)
	c.Assert(file.Close(), qt.IsNil)
}

// hashCoveredSetDir runs `migrate hash` over dir, which is the step that fixes
// the covered set. Every row hashes through the compat surface rather than
// writing atlas.sum by hand, so the covered set under test is the one the tool
// actually produces.
func hashCoveredSetDir(c *qt.C, dir string) {
	c.Helper()
	out, errOut, err := runCompat("migrate", "hash", "--dir", "file://"+dir)
	c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", out, errOut))
}

const (
	coveredSetTopLevelSQL = "CREATE TABLE a (id INTEGER PRIMARY KEY);\n"
	coveredSetNestedSQL   = "CREATE TABLE b (id INTEGER PRIMARY KEY);\n"
	coveredSetTamperSQL   = "CREATE TABLE pwned (id INTEGER PRIMARY KEY);\n"
	coveredSetNestedWarn  = "warning: sub/2_b.sql is not covered by atlas.sum and will not run; " +
		"Atlas migrations are top-level files named *.sql\n"
)

// TestCompatMigrateApply_ExecutesOnlyTheCoveredSet walks the shapes where the
// covered set and the discovered set used to disagree.
//
// Row-by-row, each is chosen to separate the fix from a plausible alternative:
//
//   - "tampered nested file never runs" is the headline. Three candidate
//     behaviors give three different results here — the defect exits 0 with
//     `pwned` present, matching the community binary exits 0 without it, and a
//     hypothetical "hash recursively instead" fix exits 1 on a checksum
//     mismatch. Only the middle one passes.
//   - "top-level only is unchanged" is the null control. Without it, every row
//     above is also consistent with "ptah differs from the community binary
//     everywhere", which would make the fixture prove nothing.
//   - "top-level tamper still refuses" pins that narrowing the set did not
//     narrow the guarantee: inside the covered set the checksum still bites.
//   - the two uppercase rows are one fix seen twice. A directory holding only
//     `1_a.SQL` used to be REFUSED as "unrecognized SQL files" while `1_a.sql`
//     beside `2_c.SQL` was silently accepted — inconsistent rather than strict.
//     The second row is what pins that we did not start refusing a directory
//     the community binary applies.
//   - "duplicate version across depth" used to exit 1 with "duplicate Atlas up
//     migration for version 1", produced entirely by the nested file the
//     covered set excludes.
func TestCompatMigrateApply_ExecutesOnlyTheCoveredSet(t *testing.T) {
	tests := []struct {
		name  string
		files []coveredSetFile
		// tamperAfterHash is appended once atlas.sum exists, which is the only
		// way to express an edit no entry in it is derived from.
		tamperAfterHash []coveredSetFile
		wantStdout      string
		wantStderr      string
		wantTables      []string
	}{
		{
			name: "tampered nested file never runs",
			files: []coveredSetFile{
				{name: "1_a.sql", body: coveredSetTopLevelSQL},
				{name: "sub/2_b.sql", body: coveredSetNestedSQL},
			},
			tamperAfterHash: []coveredSetFile{{name: "sub/2_b.sql", body: coveredSetTamperSQL}},
			wantStdout:      "Migrating to version 1 from 1 pending migrations.",
			wantStderr:      coveredSetNestedWarn,
			wantTables:      []string{"a"},
		},
		{
			name:       "nested file is the only migration",
			files:      []coveredSetFile{{name: "sub/2_b.sql", body: coveredSetNestedSQL}},
			wantStdout: "No migration files to execute",
			wantStderr: coveredSetNestedWarn,
			wantTables: []string{},
		},
		{
			name:       "top-level only is unchanged",
			files:      []coveredSetFile{{name: "1_a.sql", body: coveredSetTopLevelSQL}},
			wantStdout: "Migrating to version 1 from 1 pending migrations.",
			wantStderr: "",
			wantTables: []string{"a"},
		},
		{
			name:       "uppercase-only directory has nothing to execute",
			files:      []coveredSetFile{{name: "1_a.SQL", body: coveredSetTopLevelSQL}},
			wantStdout: "No migration files to execute",
			wantStderr: "warning: 1_a.SQL is not covered by atlas.sum and will not run; " +
				"Atlas migrations are top-level files named *.sql\n",
			wantTables: []string{},
		},
		{
			name: "uppercase file beside a migration does not refuse the directory",
			files: []coveredSetFile{
				{name: "1_a.sql", body: coveredSetTopLevelSQL},
				{name: "2_c.SQL", body: "CREATE TABLE c (id INTEGER PRIMARY KEY);\n"},
			},
			wantStdout: "Migrating to version 1 from 1 pending migrations.",
			wantStderr: "warning: 2_c.SQL is not covered by atlas.sum and will not run; " +
				"Atlas migrations are top-level files named *.sql\n",
			wantTables: []string{"a"},
		},
		{
			name: "duplicate version across depth is not a duplicate",
			files: []coveredSetFile{
				{name: "1_a.sql", body: coveredSetTopLevelSQL},
				{name: "sub/1_a.sql", body: coveredSetNestedSQL},
			},
			wantStdout: "Migrating to version 1 from 1 pending migrations.",
			wantStderr: "warning: sub/1_a.sql is not covered by atlas.sum and will not run; " +
				"Atlas migrations are top-level files named *.sql\n",
			wantTables: []string{"a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tempDir := c.TempDir()
			dir := filepath.Join(tempDir, "m")
			c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
			for _, file := range tt.files {
				writeCoveredSetFile(c, dir, file.name, file.body)
			}
			hashCoveredSetDir(c, dir)
			for _, file := range tt.tamperAfterHash {
				appendCoveredSetFile(c, dir, file.name, file.body)
			}
			dbPath := filepath.Join(tempDir, "target.db")

			stdout, stderr, err := compatApply(dir, dbPath)

			// The database is asserted before the narration: what ran is the
			// property under test, and what was printed about it is secondary.
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(sqliteUserTables(c, dbPath), qt.DeepEquals, tt.wantTables)
			c.Assert(stdout, qt.Contains, tt.wantStdout)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
		})
	}
}

// TestCompatMigrateApply_CoveredTamperStillRefuses is the other half of the
// headline row, and the reason narrowing the set is not weakening the gate:
// edit a file the covered set DOES contain and the apply is refused before
// anything runs, with the community binary's own wording.
func TestCompatMigrateApply_CoveredTamperStillRefuses(t *testing.T) {
	c := qt.New(t)
	tempDir := c.TempDir()
	dir := filepath.Join(tempDir, "m")
	writeCoveredSetFile(c, dir, "1_a.sql", coveredSetTopLevelSQL)
	writeCoveredSetFile(c, dir, "sub/2_b.sql", coveredSetNestedSQL)
	hashCoveredSetDir(c, dir)
	appendCoveredSetFile(c, dir, "1_a.sql", coveredSetTamperSQL)
	dbPath := filepath.Join(tempDir, "target.db")

	_, stderr, err := compatApply(dir, dbPath)

	c.Assert(err, qt.IsNotNil)
	c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
	_, statErr := os.Stat(dbPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

// TestAtlasDiscoveryMatchesSumCoverage asserts the invariant structurally
// rather than through a database: what migrator.DiscoverMigrationFiles selects
// for the Atlas format is a subset of what atlasmigrateimport.SumFileNames
// covers for it.
//
// The behavioral rows above can only catch a divergence on a shape someone
// thought to write down; this catches one on any shape, and it is the property
// stokaro/ptah#976 and stokaro/ptah#982 are both instances of violating —
// consumed must never exceed covered. The "covered" operand is pinned too, so a
// future change that widens BOTH sets together cannot pass by moving the
// target: `1_a.sql` is the only covered name here, out of six SQL files.
func TestAtlasDiscoveryMatchesSumCoverage(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	writeCoveredSetFile(c, dir, "1_a.sql", coveredSetTopLevelSQL)
	writeCoveredSetFile(c, dir, "2_c.SQL", coveredSetTopLevelSQL)
	writeCoveredSetFile(c, dir, "sub/2_b.sql", coveredSetNestedSQL)
	writeCoveredSetFile(c, dir, "sub/3_d.SQL", coveredSetNestedSQL)
	writeCoveredSetFile(c, dir, "sub/deep/4_e.sql", coveredSetNestedSQL)
	writeCoveredSetFile(c, dir, ".hidden/5_f.sql", coveredSetNestedSQL)
	fsys := os.DirFS(dir)

	covered, err := atlasmigrateimport.SumFileNames(fsys, atlasmigrateimport.FormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(covered, qt.DeepEquals, []string{"1_a.sql"})

	discovered, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	paths := make([]string, 0, len(discovered))
	for _, file := range discovered {
		paths = append(paths, file.Path)
	}
	c.Assert(paths, qt.DeepEquals, covered)
}
