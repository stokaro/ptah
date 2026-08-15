package generator_test

import (
	"math"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/migrator"
)

// These rows pin stokaro/ptah#938 on the verb that creates a migration file:
// `ptah migrations create`, which `ptah-compat migrate new` forwards to.
//
// Measured on master e05752ef, in one directory holding one
// `29991231235959_future.sql`, seconds apart:
//
//	ptah-compat migrate new  -> 29991231235960_hello.sql
//	ptah-compat migrate diff -> 20260809042338_adddiff.sql
//	atlas       migrate new  -> 20260809042349_hello.sql   (community v1.3.0)
//	atlas       migrate diff -> 20260809042349_adddiff.sql
//
// So one binary stamped two shapes, and the shape only `migrate new` produced
// -- newest+1 -- is also not a time: `...235960` has sixty seconds in it and
// cannot be parsed back. The pinned community binary stamps the clock for both
// verbs, and so does this binary's `migrate diff` since stokaro/ptah#1218.
//
// The arithmetic behind that bump had a second failure with a worse ending. A
// directory whose newest version is math.MaxInt64 -- which
// `migrate import --dir-format flyway` produces, stamping a Flyway `R__`
// repeatable there so it sorts last -- made newest+1 wrap to math.MinInt64, and
// the verb wrote `-9223372036854775808_addposts.sql`, hashed it into atlas.sum
// and exited 0. The same binary's discovery then refused that name and dropped
// it with no diagnostic: `migrate validate` exited 0, `migrate apply` reported
// "2 pending migrations" over three files, and the table the migration created
// never appeared.
//
// The cheaper wrong implementation for every row below is the rule that was
// there before, not the deletion of the new one: put
// `if latest := latestAtlasVersionIn(names); latest >= version { version = latest + 1 }`
// back at the top of firstFreeAtlasVersion and rows 1 and 2 fail.

// atlasStamp renders at as the UTC yyyyMMddHHmmss version, so a row can bracket
// "the clock" without naming a second.
func atlasStamp(tb testing.TB, at time.Time) int64 {
	c := qt.New(tb)
	c.Helper()
	stamp, err := strconv.ParseInt(at.UTC().Format("20060102150405"), 10, 64)
	c.Assert(err, qt.IsNil)
	return stamp
}

// atlasFileVersions returns the versions of the Atlas migrations in dir.
func atlasFileVersions(tb testing.TB, dir string) []int64 {
	c := qt.New(tb)
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	versions := make([]int64, 0, len(entries))
	for _, entry := range entries {
		parsed, parseErr := migrator.ParseAtlasMigrationFileName(entry.Name())
		if parseErr != nil {
			continue
		}
		versions = append(versions, parsed.Version)
	}
	slices.Sort(versions)
	return versions
}

// TestGenerateEmptyMigration_AtlasStampsTheClockBesideAFutureMigration is the
// discriminator for the two-shapes half of #938. A directory whose newest
// migration is dated 2999 is the only fixture that separates the two candidate
// rules: "the clock" and "newest + 1" agree on every ordinary directory.
func TestGenerateEmptyMigration_AtlasStampsTheClockBesideAFutureMigration(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "29991231235959_future.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600,
	), qt.IsNil)

	before := atlasStamp(c.TB, time.Now())
	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "hello",
		OutputDir:     dir,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})
	after := atlasStamp(c.TB, time.Now())

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	version := files.Files[0].Version

	// The clock, not the neighbor. 29991231235960 is what newest+1 wrote.
	c.Assert(version >= before, qt.IsTrue, qt.Commentf("version %d predates the call", version))
	c.Assert(version <= after, qt.IsTrue, qt.Commentf("version %d postdates the call", version))
	c.Assert(version, qt.Not(qt.Equals), int64(29991231235960))

	// And it is a stamp that parses back as the time it claims to be, which
	// `...235960` never was.
	roundTrip, parseErr := time.Parse("20060102150405", strconv.FormatInt(version, 10))
	c.Assert(parseErr, qt.IsNil)
	c.Assert(roundTrip.UTC().Format("20060102150405"), qt.Equals, strconv.FormatInt(version, 10))
}

// TestGenerateEmptyMigration_AtlasSurvivesAMaxInt64Neighbor is the overflow
// half. The fixture is the directory `migrate import --dir-format flyway`
// writes: the repeatable migration is stamped math.MaxInt64 so it sorts last.
func TestGenerateEmptyMigration_AtlasSurvivesAMaxInt64Neighbor(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	repeatable := strconv.FormatInt(math.MaxInt64, 10) + "_active_users.sql"
	c.Assert(os.WriteFile(
		filepath.Join(dir, repeatable),
		[]byte("CREATE VIEW active_users AS SELECT 1;\n"), 0o600,
	), qt.IsNil)

	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "addposts",
		OutputDir:     dir,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].Version > 0, qt.IsTrue,
		qt.Commentf("version %d is not a version any reader accepts", files.Files[0].Version))

	// Every name in the directory is one discovery reads back. The wrapped
	// version wrote `-9223372036854775808_addposts.sql`, which parses as
	// nothing and was dropped in silence.
	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	for _, name := range names {
		c.Assert(strings.HasPrefix(name, "-"), qt.IsFalse, qt.Commentf("name %q", name))
	}
	c.Assert(atlasFileVersions(c.TB, dir), qt.HasLen, 2)
}

// TestGenerateEmptyMigration_PtahRefusesPastTheTenDigitCeiling is the same class
// on the paired layout, where the bound is the file name's fixed width rather
// than int64. The paired layout keeps its monotonic newest+1 rule -- nothing
// outside Ptah reads it, so no parity argument moves it -- so here the only
// honest answer at the ceiling is a refusal.
//
// Measured on master: `ptah migrations create --dir-format ptah` into a
// directory holding `9999999999_seed.up.sql` wrote
// `10000000000_addposts.up.sql` at exit 0, and `ptah migrations up` then
// reported "Total migrations: 1" over two pairs and created only the seed's
// table.
//
// The cheaper wrong implementation is `return latest + 1, nil` with no bound in
// migrationversion.Next.
func TestGenerateEmptyMigration_PtahRefusesPastTheTenDigitCeiling(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "9999999999_seed.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "9999999999_seed.down.sql"), []byte("DROP TABLE users;\n"), 0o600,
	), qt.IsNil)

	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "addposts",
		OutputDir:     dir,
		DirFormat:     migrator.MigrationDirFormatPtah,
	})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "9999999999")
	c.Assert(files, qt.IsNil)

	// Nothing was written, so no unreadable name is left behind for the reader
	// to skip.
	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
}

// TestGenerateEmptyMigration_PtahStillCountsUpBelowTheCeiling is the
// non-interference control for the row above: reverting the bound must not
// redden it. The paired layout's monotonic rule is unchanged everywhere the
// file name still has room, which is everywhere a real project lives.
func TestGenerateEmptyMigration_PtahStillCountsUpBelowTheCeiling(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(
		filepath.Join(dir, "9999999998_seed.up.sql"),
		[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"), 0o600,
	), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "9999999998_seed.down.sql"), []byte("DROP TABLE users;\n"), 0o600,
	), qt.IsNil)

	files, err := generator.GenerateEmptyMigration(generator.EmptyMigrationOptions{
		MigrationName: "addposts",
		OutputDir:     dir,
		DirFormat:     migrator.MigrationDirFormatPtah,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(files.Files, qt.HasLen, 1)
	c.Assert(files.Files[0].Version, qt.Equals, int64(9999999999))
}
