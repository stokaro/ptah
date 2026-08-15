package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// futureDatedMigration is a version no clock will reach, whose seconds field is
// 59. Both halves matter: the version is higher than any stamp `migrate diff`
// could produce, and adding one to it yields `29991231235960` -- a sixtieth
// second, which is not a time.
const futureDatedMigration = "29991231235959_future.sql"

// hashMigrationDir writes atlas.sum over dir. A directory that already holds a
// migration is refused by the checksum preflight without it, which would make
// every row below fail for a reason that is not the version.
func hashMigrationDir(c *qt.C, dir string) {
	c.Helper()
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"migrate", "hash", "--dir", "file://" + dir})

	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("output:\n%s", out.String()))
}

// seedMigrationDir creates a migration directory holding files, and returns it.
//
// A directory that already holds a migration is refused by the checksum
// preflight, so anything seeded is hashed; an empty directory has nothing to
// hash and `migrate hash` on one writes a sum for no files.
func seedMigrationDir(c *qt.C, files map[string]string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	if len(files) > 0 {
		hashMigrationDir(c, dir)
	}
	return dir
}

// dirEntryNames lists the base names in dir.
func dirEntryNames(c *qt.C, dir string) []string {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

// runMigrateDiffInto plans one migration into dir and returns the base names
// that were not there before, excluding atlas.sum. Comparing against a listing
// taken first keeps the caller from having to know which files it seeded.
func runMigrateDiffInto(c *qt.C, dir string) []string {
	c.Helper()
	before := dirEntryNames(c, dir)
	desiredPath := seedSQLiteDB(c, "CREATE TABLE desired_users (id INTEGER PRIMARY KEY)")
	devPath := filepath.Join(c.TempDir(), "dev.db")
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{
		"migrate", "diff", "add_desired_users",
		"--to", "sqlite://" + desiredPath,
		"--dev-url", "sqlite://" + devPath,
		"--dir", "file://" + dir,
	})

	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("output:\n%s", out.String()))

	var written []string
	for _, name := range dirEntryNames(c, dir) {
		if name == "atlas.sum" || slices.Contains(before, name) {
			continue
		}
		written = append(written, name)
	}
	return written
}

// TestMigrateDiffStampsTheUTCTimestampVersion pins the version spelling
// `migrate diff` writes (stokaro/ptah#1218).
//
// Both rows were divergences from the pinned community binary v1.3.0, and they
// were different divergences, which is why one row would not have found the
// other:
//
//   - into an empty directory the version came from `time.Now().Unix()`, a
//     ten-digit epoch that is not a timestamp in any spelling;
//   - into a directory holding a higher version it came from `max + 1`, which
//     on a neighbour ending in `59` seconds produces `...235960`.
//
// The binary answers the current UTC stamp to both, and to the future-dated one
// it does so even though the result sorts BEFORE the migration already there.
func TestMigrateDiffStampsTheUTCTimestampVersion(t *testing.T) {
	tests := []struct {
		name string
		// seeded is what the directory already holds when the run starts.
		seeded map[string]string
	}{
		{
			name: "an empty directory",
		},
		{
			name: "a directory holding a future-dated migration",
			seeded: map[string]string{
				futureDatedMigration: "CREATE TABLE placeholder (id INTEGER);\n",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := seedMigrationDir(c, test.seeded)
			before := time.Now().UTC().Add(-time.Second)

			written := runMigrateDiffInto(c, dir)

			c.Assert(written, qt.HasLen, 1)
			version, _, found := strings.Cut(written[0], "_")
			c.Assert(found, qt.IsTrue, qt.Commentf("file name %q carries no version", written[0]))
			// Fourteen digits rules out the epoch, which has ten.
			c.Assert(version, qt.HasLen, 14)
			stamp, err := time.Parse("20060102150405", version)
			// Parsing rules out `...235960`, which has fourteen digits and is
			// still not a time.
			c.Assert(err, qt.IsNil, qt.Commentf("version %q does not parse as a timestamp", version))
			c.Assert(stamp.Before(before), qt.IsFalse, qt.Commentf("version %q predates the run", version))
			c.Assert(stamp.After(time.Now().UTC().Add(time.Minute)), qt.IsFalse,
				qt.Commentf("version %q is in the future", version))
		})
	}
}

// TestMigrateDiffAdvancesPastAnExistingVersionOfThisSecond covers what the
// wall clock alone cannot: a directory that already holds the stamp this run
// would produce. The version advances by a second rather than colliding, which
// is the rule `migrate new` already applies.
func TestMigrateDiffAdvancesPastAnExistingVersionOfThisSecond(t *testing.T) {
	c := qt.New(t)
	taken := time.Now().UTC().Format("20060102150405")
	dir := seedMigrationDir(c, map[string]string{
		taken + "_taken.sql": "CREATE TABLE taken (id INTEGER);\n",
	})

	written := runMigrateDiffInto(c, dir)

	c.Assert(written, qt.HasLen, 1)
	version, _, _ := strings.Cut(written[0], "_")
	takenVersion, err := strconv.ParseInt(taken, 10, 64)
	c.Assert(err, qt.IsNil)
	plannedVersion, err := strconv.ParseInt(version, 10, 64)
	c.Assert(err, qt.IsNil)
	c.Assert(plannedVersion > takenVersion, qt.IsTrue,
		qt.Commentf("planned %d does not advance past taken %d", plannedVersion, takenVersion))
}
