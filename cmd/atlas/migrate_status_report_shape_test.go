package atlas_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// The report shape of `ptah-compat migrate status` (stokaro/ptah#1102).
//
// This is the one verb a deploy pipeline parses with a machine, so the whole
// point of the assertions below is that they compare the WHOLE stdout rather
// than probing for a substring: a field name, a padding column or a sentinel
// value moving is exactly the regression, and a Contains assertion would sit
// through most of them.
//
// Every want string is the byte-for-byte output of the pinned community binary
// v1.3.0 on the same directory over the same database state, captured by
// absolute path.

const (
	statusShapeVersionOne = "20240401000001"
	statusShapeVersionTwo = "20240401000002"
)

// writeStatusShapeDir writes a hashed two-migration Atlas directory whose
// bodies both succeed.
func writeStatusShapeDir(c *qt.C, dir string) {
	c.Helper()
	writeStatusShapeFiles(c, dir, "CREATE TABLE ss_two (id INTEGER PRIMARY KEY);\n")
}

// writeStatusShapeDirtyDir writes the same directory with a second migration
// whose SECOND statement fails, so an apply leaves a half-applied revision row
// behind: applied=1, total=2.
func writeStatusShapeDirtyDir(c *qt.C, dir string) {
	c.Helper()
	writeStatusShapeFiles(c, dir,
		"CREATE TABLE ss_two (id INTEGER PRIMARY KEY);\nCREATE TABLE ss_one (id INTEGER PRIMARY KEY);\n")
}

func writeStatusShapeFiles(c *qt.C, dir, second string) {
	c.Helper()
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, statusShapeVersionOne+"_one.sql"),
		[]byte("CREATE TABLE ss_one (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, statusShapeVersionTwo+"_two.sql"),
		[]byte(second),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
}

// TestCompatMigrateStatus_MirrorsTheAtlasReportShape walks the states a
// pipeline actually sees.
//
// Reverted, every row prints the native block instead and all four `want`
// comparisons go red with the same diff shape: got `=== MIGRATION STATUS ===`
// / `Current Version: 0` / `Total Migrations: 2` / `Applied Migrations: 0` /
// `Pending Migrations: 2` / `Status: Pending migrations available` against a
// want that begins `Migration Status: PENDING`. Concretely, reverted:
//
//   - "nothing applied" reports Current Version 0 where the mirrored report
//     says "No migration applied yet", so `awk '{print $NF}'` reads a real
//     version instead of "none";
//   - "all applied" never contains "Migration Status: OK", which is the string
//     a deploy gate greps for;
//   - no row contains a "Next Version:" line at all;
//   - "half-applied second migration" loses the statement counters and the
//     `  -- SQL:` / `  -- ERROR:` block in favour of `Dirty Migration:` /
//     `Error Statement:` / `Error:`.
func TestCompatMigrateStatus_MirrorsTheAtlasReportShape(t *testing.T) {
	tests := []struct {
		name       string
		writeDir   func(*qt.C, string)
		seed       func(*qt.C, string, string)
		wantStdout string
	}{
		{
			name:     "nothing applied",
			writeDir: writeStatusShapeDir,
			seed:     func(*qt.C, string, string) {},
			wantStdout: "Migration Status: PENDING\n" +
				"  -- Current Version: No migration applied yet\n" +
				"  -- Next Version:    " + statusShapeVersionOne + "\n" +
				"  -- Executed Files:  0\n" +
				"  -- Pending Files:   2\n",
		},
		{
			name:     "one of two applied",
			writeDir: writeStatusShapeDir,
			seed:     seedStatusShapeBounded,
			wantStdout: "Migration Status: PENDING\n" +
				"  -- Current Version: " + statusShapeVersionOne + "\n" +
				"  -- Next Version:    " + statusShapeVersionTwo + "\n" +
				"  -- Executed Files:  1\n" +
				"  -- Pending Files:   1\n",
		},
		{
			name:     "all applied",
			writeDir: writeStatusShapeDir,
			seed:     seedStatusShapeAll,
			wantStdout: "Migration Status: OK\n" +
				"  -- Current Version: " + statusShapeVersionTwo + "\n" +
				"  -- Next Version:    Already at latest version\n" +
				"  -- Executed Files:  2\n" +
				"  -- Pending Files:   0\n",
		},
		{
			name:     "half-applied second migration",
			writeDir: writeStatusShapeDirtyDir,
			seed:     seedStatusShapeFailing,
			wantStdout: "Migration Status: PENDING\n" +
				"  -- Current Version: " + statusShapeVersionTwo + " (1 statements applied)\n" +
				"  -- Next Version:    " + statusShapeVersionTwo + " (1 statements left)\n" +
				"  -- Executed Files:  2 (last one partially)\n" +
				"  -- Pending Files:   1\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			root := c.TempDir()
			dir := filepath.Join(root, "migrations")
			dbPath := filepath.Join(root, "shape.db")
			tt.writeDir(c, dir)
			tt.seed(c, dir, dbPath)

			stdout, stderr, err := runCompat(
				"migrate", "status",
				"--url", "sqlite://"+dbPath,
				"--dir", "file://"+dir,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(statusShapeCounts(stdout), qt.Equals, tt.wantStdout)
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// statusShapeCounts keeps the counts block and drops the failure block, whose
// text is Ptah's own migration error rather than a mirrored literal. The
// failure block's own shape is asserted separately, below.
func statusShapeCounts(stdout string) string {
	head, _, _ := strings.Cut(stdout, "\nLast migration attempt had errors:\n")
	return head
}

// TestCompatMigrateStatus_HalfAppliedFailureBlock pins the block the counts
// test deliberately trims, including the blank line that separates it from the
// counts and the `SQL:`/`ERROR:` padding column.
//
// Reverted, the output carries `Error Statement: ...` and `Error: ...` with no
// leading `  -- `, no blank line above them and no "Last migration attempt had
// errors:" header, so all four assertions go red.
func TestCompatMigrateStatus_HalfAppliedFailureBlock(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	dbPath := filepath.Join(root, "failure.db")
	writeStatusShapeDirtyDir(c, dir)
	seedStatusShapeFailing(c, dir, dbPath)

	stdout, stderr, err := runCompat(
		"migrate", "status",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
	c.Assert(stdout, qt.Contains, "  -- Pending Files:   1\n\nLast migration attempt had errors:\n")
	c.Assert(stdout, qt.Contains, "\n  -- SQL:   CREATE TABLE ss_one (id INTEGER PRIMARY KEY);")
	c.Assert(stdout, qt.Not(qt.Contains), "failed to execute migration SQL")
	c.Assert(stdout, qt.Contains, "\n  -- ERROR: ")
	// Nine lines exactly. The stored error is one line since
	// stokaro/ptah#1196 -- Ptah's wrapping and the `SQL:` line it carried are
	// gone -- but the report keeps folding a stored newline to a space, because
	// a driver is still free to produce one and a parser keying on `  -- `
	// would lose the tail of the message to a line with no prefix.
	c.Assert(strings.Count(stdout, "\n"), qt.Equals, 9)
}

func seedStatusShapeBounded(c *qt.C, dir, dbPath string) {
	c.Helper()
	_, stderr, err := runCompat(
		"migrate", "apply", "1",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
}

func seedStatusShapeAll(c *qt.C, dir, dbPath string) {
	c.Helper()
	_, stderr, err := runCompat(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
	)
	c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
}

// seedStatusShapeFailing runs the apply that is EXPECTED to fail, under
// --tx-mode none so the first statement of the second migration commits and the
// revision row records applied=1 of 2.
func seedStatusShapeFailing(c *qt.C, dir, dbPath string) {
	c.Helper()
	_, _, err := runCompat(
		"migrate", "apply",
		"--url", "sqlite://"+dbPath,
		"--dir", "file://"+dir,
		"--tx-mode", "none",
	)
	c.Assert(err, qt.IsNotNil)
}
