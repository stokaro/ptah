package migrateops_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrateops"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// stampLayout is the layout every Atlas-format version this binary stamps is
// rendered in. It is spelled out here rather than imported from the package the
// fix uses, so a test failure means the NAME ON DISK is not an instant, not that
// two copies of the same helper disagree.
const stampLayout = "20060102150405"

// atlasDir writes files into a fresh directory, adds atlas.sum, and returns the
// directory.
func atlasDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, sql := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas); err != nil {
		t.Fatal(err)
	}
	return dir
}

// assertNamedInstant reads the version back out of the file name rebase left on
// disk and parses it as a UTC yyyyMMddHHmmss instant.
//
// It PARSES rather than string-matches on purpose. 29991231235960 and
// 20260231000000 are fourteen digits that look exactly like stamps and are
// neither of them a time; an assertion written as `qt.Equals, "29991231235960"`
// passes on the defect it was added to catch (stokaro/ptah#938). Parsing is the
// only check that separates a version that reads back from one that does not.
func assertNamedInstant(c *qt.C, dir, description string, version int64) {
	c.Helper()
	name := strconv.FormatInt(version, 10) + "_" + description + ".sql"
	_, statErr := os.Stat(filepath.Join(dir, name))
	c.Assert(statErr, qt.IsNil, qt.Commentf("rebase reported version %d but no %s is on disk", version, name))

	parsed, parseErr := migrator.ParseAtlasMigrationFileName(name)
	c.Assert(parseErr, qt.IsNil)

	digits := strconv.FormatInt(parsed.Version, 10)
	at, timeErr := time.Parse(stampLayout, digits)
	c.Assert(timeErr, qt.IsNil, qt.Commentf("version %s in %s is not an instant", digits, name))
	c.Assert(at.UTC().Format(stampLayout), qt.Equals, digits)
}

// TestAtlasRebaseBumpsOntoASecondThatExists covers the bump past a future-dated
// migration.
//
// `migrate rebase` computed maxVersion+1 as plain integer arithmetic. Beside a
// 29991231235959_future.sql that is 29991231235960 -- sixty seconds past the
// minute -- and rebasing the future migration afterwards wrote 29991231235961 on
// top of it. Both were written, hashed into atlas.sum and reported at exit 0.
//
// The cheaper wrong implementation is migrationversion.Next, which bounds the
// arithmetic against the int64 ceiling and says nothing about the calendar; it
// returns 29991231235960 here and this test fails on it.
func TestAtlasRebaseBumpsOntoASecondThatExists(t *testing.T) {
	c := qt.New(t)
	dir := atlasDir(t, map[string]string{
		"20200101000000_users.sql":  "CREATE TABLE users (id int);\n",
		"29991231235959_future.sql": "CREATE TABLE future (id int);\n",
	})

	first, _, err := migrateops.Rebase(dir, 20200101000000, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(first, qt.Equals, int64(30000101000000))
	assertNamedInstant(c, dir, "users", first)
	validates(c, dir, migrator.MigrationDirFormatAtlas)

	second, _, err := migrateops.Rebase(dir, 29991231235959, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(second, qt.Equals, int64(30000101000001))
	assertNamedInstant(c, dir, "future", second)
	validates(c, dir, migrator.MigrationDirFormatAtlas)
}

// TestAtlasRebaseStampsTheCalendarSecond covers the clock rebase reaches for
// when the newest version is below it.
//
// Rebase took migrator.GetNextMigrationVersion() -- a Unix epoch -- for every
// layout, so moving a migration to the end of an Atlas directory whose versions
// were all small wrote a ten-digit 1786268355_init.sql beside fourteen-digit
// neighbors. `migrate new`, `migrate diff` and `migrate checkpoint` all stamp
// the UTC yyyyMMddHHmmss second into those directories.
//
// The cheaper wrong implementation is one clock for both layouts, which is what
// the code did; it returns the ten-digit epoch here and this test fails on it
// twice, on the digit count and on the parse.
func TestAtlasRebaseStampsTheCalendarSecond(t *testing.T) {
	c := qt.New(t)
	dir := atlasDir(t, map[string]string{
		"1_init.sql":   "CREATE TABLE init (id int);\n",
		"2_second.sql": "CREATE TABLE two (id int);\n",
	})

	version, _, err := migrateops.Rebase(dir, 1, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(strconv.FormatInt(version, 10), qt.HasLen, 14, qt.Commentf("version %d is not a fourteen-digit stamp", version))
	assertNamedInstant(c, dir, "init", version)
	validates(c, dir, migrator.MigrationDirFormatAtlas)
}

// TestPtahRebaseKeepsTheTenDigitVersion is the non-interference control for the
// two tests above.
//
// The paired ptah layout renders its version with %010d and parses it with a
// ten-digit regex, so a fourteen-digit stamp is a name its reader drops. A fix
// that stamped the calendar second for every layout instead of for the Atlas
// layout alone would turn this green test red, which is the point of keeping it.
func TestPtahRebaseKeepsTheTenDigitVersion(t *testing.T) {
	c := qt.New(t)
	dir := ptahFixture(t)

	version, _, err := migrateops.Rebase(dir, 1, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(version > 3, qt.IsTrue, qt.Commentf("version %d must outrank the previous max 3", version))
	c.Assert(version <= int64(9999999999), qt.IsTrue, qt.Commentf("version %d does not fit the ten-digit paired name", version))

	moved := migrator.GenerateMigrationFileName(version, "first", "up")
	_, statErr := os.Stat(filepath.Join(dir, moved))
	c.Assert(statErr, qt.IsNil)
	validates(c, dir, migrator.MigrationDirFormatPtah)
}
