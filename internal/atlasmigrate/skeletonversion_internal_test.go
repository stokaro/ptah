package atlasmigrate

// White-box testing required: the row below has to name the second the skeleton
// writer starts from, and the wall clock MigrationVersion reads has no exported
// seam -- production never assigns migrationVersionClock. Reaching the retry
// through the exported WriteSkeletonMigration alone would mean racing two
// `migrate new` runs into one real second, which is a fixture that passes or
// fails by timing rather than by the rule under test. Everything asserted is
// otherwise observable: the names of the files left on disk.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// TestWriteSkeletonMigrationRetriesOntoASecondThatExists pins the `...235960`
// half of stokaro/ptah#938 on `migrate new` for the five external layouts.
//
// The retry is documented as advancing "to the next second", and adding one to
// a version numbered :59 does not do that -- it produces a fourteen-digit value
// that is not an instant, the shape `migrate checkpoint` was measured writing as
// 29991231235960. Two `migrate new` runs inside one second reach it, which is
// ordinary use rather than a contrived directory.
//
// The cheaper wrong implementation is the loop this branch shipped:
// `for version := MigrationVersion(); ; version++`, bounded by nothing and
// aware of no calendar, which answers 29990101123060 here.
//
// The retry is keyed on the file NAME, so the fixture is the same migration
// name in the same second -- `migrate new hello` run twice -- rather than any
// migration in that second.
func TestWriteSkeletonMigrationRetriesOntoASecondThatExists(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationVersionClock = func() time.Time { return time.Date(2999, time.January, 1, 12, 30, 59, 0, time.UTC) }
	defer func() { migrationVersionClock = func() time.Time { return time.Now().UTC() } }()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "29990101123059_hello.up.sql"), []byte("SELECT 1;\n"), 0o600,
	), qt.IsNil)

	paths, err := WriteSkeletonMigration(nil, dir, atlasmigrateimport.FormatGolangMigrate, "hello")

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.HasLen, 2)
	c.Assert(filepath.Base(paths[0]), qt.Equals, "29990101123100_hello.up.sql")
	c.Assert(filepath.Base(paths[1]), qt.Equals, "29990101123100_hello.down.sql")
}

// TestWriteSkeletonMigrationRetryStillAddsOneMidMinute is the non-interference
// control. Reverting the calendar-aware retry to a plain increment must NOT
// redden it: fifty-nine seconds out of every sixty, the next integer is already
// a second that exists and the answer is unchanged.
func TestWriteSkeletonMigrationRetryStillAddsOneMidMinute(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	migrationVersionClock = func() time.Time { return time.Date(2999, time.January, 1, 12, 30, 58, 0, time.UTC) }
	defer func() { migrationVersionClock = func() time.Time { return time.Now().UTC() } }()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "29990101123058_hello.up.sql"), []byte("SELECT 1;\n"), 0o600,
	), qt.IsNil)

	paths, err := WriteSkeletonMigration(nil, dir, atlasmigrateimport.FormatGolangMigrate, "hello")

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.HasLen, 2)
	c.Assert(filepath.Base(paths[0]), qt.Equals, "29990101123059_hello.up.sql")
}
