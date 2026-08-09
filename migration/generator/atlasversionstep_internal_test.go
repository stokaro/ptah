package generator

// White-box testing required: the row below has to name the second the stamp is
// taken in, and the wall clock the Atlas stamp reads has no exported seam --
// production never assigns atlasVersionClock, so nothing outside this package
// needs one. Reaching the same state through the exported API would mean racing
// two migrations into one real second, which is a fixture that passes or fails
// by timing rather than by the rule under test. Everything asserted is otherwise
// observable: the version in the name of the file left on disk.

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/migrator"
)

// TestAtlasCollisionStepLandsOnASecondThatExists pins the `...235960` half of
// stokaro/ptah#938 on `migrate new`.
//
// `migrate new` stamps the clock and steps only past a version the directory
// already holds, which is the rule the pinned community binary was measured to
// use. That step is reachable in ordinary use -- two migrations created inside
// one second -- and stepping by ONE off a second numbered :59 produced a
// fourteen-digit version that is not an instant, the same shape
// `migrate checkpoint` was measured writing as 29991231235960.
//
// The cheaper wrong implementation is not deleting the step. It is the bounded
// integer step this branch shipped:
//
//	if err := migrationversion.Check(version, migrator.MigrationDirFormatAtlas); err != nil {
//		return 0, err
//	}
//	...
//	version++
//
// which passes every bound and still answers 29990101123060.
func TestAtlasCollisionStepLandsOnASecondThatExists(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasVersionClock = func() time.Time { return time.Date(2999, time.January, 1, 12, 30, 59, 0, time.UTC) }
	defer func() { atlasVersionClock = func() time.Time { return time.Now().UTC() } }()
	c.Assert(os.WriteFile(
		filepath.Join(dir, atlasEmptyMigrationFileName(29990101123059, "seed")),
		[]byte("SELECT 1;\n"), 0o600,
	), qt.IsNil)

	files, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "hello",
		OutputDir:     dir,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].Version, qt.Equals, int64(29990101123100))

	_, statErr := os.Stat(filepath.Join(dir, atlasEmptyMigrationFileName(29990101123100, "hello")))
	c.Assert(statErr, qt.IsNil)
}

// TestAtlasCollisionStepStillAddsOneMidMinute is the non-interference control
// for the row above. Reverting the calendar-aware step to a plain increment must
// NOT redden it: everywhere the next integer is already a second that exists --
// which is fifty-nine seconds out of every sixty -- the answer is unchanged.
func TestAtlasCollisionStepStillAddsOneMidMinute(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	atlasVersionClock = func() time.Time { return time.Date(2999, time.January, 1, 12, 30, 58, 0, time.UTC) }
	defer func() { atlasVersionClock = func() time.Time { return time.Now().UTC() } }()
	c.Assert(os.WriteFile(
		filepath.Join(dir, atlasEmptyMigrationFileName(29990101123058, "seed")),
		[]byte("SELECT 1;\n"), 0o600,
	), qt.IsNil)

	files, err := GenerateEmptyMigration(EmptyMigrationOptions{
		MigrationName: "hello",
		OutputDir:     dir,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].Version, qt.Equals, int64(29990101123059))
}
