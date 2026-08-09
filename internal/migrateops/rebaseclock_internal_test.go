package migrateops

// White-box testing required: the rows below have to state the EXACT version a
// rebase lands on, and that version is read off the wall clock. rebaseNow is the
// only seam that can freeze it and production never assigns it, so a black-box
// test can assert the shape of the version but not its value -- which is how the
// two clocks stayed indistinguishable. Everything asserted is otherwise
// observable: the version rebase reports and the name it leaves on disk.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/migration/migrator"
)

// frozen is the instant every row below rebases in. Its seconds field is 07 so
// no row can pass by accidentally landing on a minute boundary, and it is far
// enough in the future that a directory numbered 1, 2 sorts below it.
var frozen = time.Date(2026, time.August, 9, 9, 43, 7, 0, time.UTC)

// freezeRebaseClock points rebaseNow at frozen for the duration of one test.
func freezeRebaseClock(t *testing.T) {
	t.Helper()
	previous := rebaseNow
	rebaseNow = func() time.Time { return frozen }
	t.Cleanup(func() { rebaseNow = previous })
}

// writeFixture writes files into a fresh directory, adds the integrity file
// format expects, and returns the directory.
func writeFixture(t *testing.T, format migrator.MigrationDirFormat, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, sql := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := migratesum.WriteWithFormat(dir, format); err != nil {
		t.Fatal(err)
	}
	return dir
}

// TestRebaseClockIsTheCalendarSecondOnAtlas pins the Atlas clock to the value
// its layout is read in.
//
// 20260809094307 is frozen rendered as UTC yyyyMMddHHmmss. The cheaper wrong
// implementation is the Unix epoch that rebase took for every layout, which for
// the same instant is 1786268587 -- a ten-digit version that is a perfectly
// valid Atlas file name, sorts correctly, and is not the shape `migrate new`,
// `migrate diff` and `migrate checkpoint` write into that directory. Only an
// assertion on the VALUE separates the two; both are positive int64s.
func TestRebaseClockIsTheCalendarSecondOnAtlas(t *testing.T) {
	c := qt.New(t)
	freezeRebaseClock(t)

	dir := writeFixture(t, migrator.MigrationDirFormatAtlas, map[string]string{
		"1_init.sql":   "CREATE TABLE init (id int);\n",
		"2_second.sql": "CREATE TABLE two (id int);\n",
	})

	version, _, err := Rebase(dir, 1, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(20260809094307))

	_, statErr := os.Stat(filepath.Join(dir, "20260809094307_init.sql"))
	c.Assert(statErr, qt.IsNil)
}

// TestRebaseClockIsTheEpochOnPtah is the inverse control for the row above.
//
// 1786268587 is frozen as a Unix epoch. The paired layout renders its version
// with %010d and parses exactly ten digits, so the calendar second the Atlas row
// demands is a name its own reader drops; a fix that stamped the calendar for
// every layout would turn this row red.
func TestRebaseClockIsTheEpochOnPtah(t *testing.T) {
	c := qt.New(t)
	freezeRebaseClock(t)

	dir := writeFixture(t, migrator.MigrationDirFormatPtah, map[string]string{
		"0000000001_first.up.sql":    "CREATE TABLE a (id int);\n",
		"0000000001_first.down.sql":  "DROP TABLE a;\n",
		"0000000002_second.up.sql":   "CREATE TABLE b (id int);\n",
		"0000000002_second.down.sql": "DROP TABLE b;\n",
	})

	version, _, err := Rebase(dir, 1, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(1786268587))

	_, statErr := os.Stat(filepath.Join(dir, "1786268587_first.up.sql"))
	c.Assert(statErr, qt.IsNil)
}
