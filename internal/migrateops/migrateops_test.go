package migrateops_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/migrateops"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

// ptahFixture writes three versioned migration pairs and a ptah.sum, returning
// the directory.
func ptahFixture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"0000000001_first.up.sql":    "CREATE TABLE a (id int);\n",
		"0000000001_first.down.sql":  "DROP TABLE a;\n",
		"0000000002_second.up.sql":   "CREATE TABLE b (id int);\n",
		"0000000002_second.down.sql": "DROP TABLE b;\n",
		"0000000003_third.up.sql":    "CREATE TABLE c (id int);\n",
		"0000000003_third.down.sql":  "DROP TABLE c;\n",
	}
	for name, sql := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatPtah); err != nil {
		t.Fatal(err)
	}
	return dir
}

func validates(c *qt.C, dir string, format migrator.MigrationDirFormat) {
	c.Helper()
	res, err := migratesum.VerifyDirWithFormat(dir, format)
	c.Assert(err, qt.IsNil)
	c.Assert(res.OK(), qt.IsTrue, qt.Commentf("added=%v removed=%v changed=%v dirmismatch=%v", res.Added, res.Removed, res.Changed, res.DirHashMismatch))
}

func TestRemove(t *testing.T) {
	c := qt.New(t)
	dir := ptahFixture(t)

	res, err := migrateops.Remove(dir, 2, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(res.SumFile, qt.Equals, "ptah.sum")
	c.Assert(res.Files, qt.DeepEquals, []string{"0000000002_second.down.sql", "0000000002_second.up.sql"})

	// The pair is gone, the others remain, and the directory still validates.
	_, statErr := os.Stat(filepath.Join(dir, "0000000002_second.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	_, statErr = os.Stat(filepath.Join(dir, "0000000001_first.up.sql"))
	c.Assert(statErr, qt.IsNil)
	validates(c, dir, migrator.MigrationDirFormatPtah)
}

func TestRemoveNotFound(t *testing.T) {
	c := qt.New(t)
	dir := ptahFixture(t)
	_, err := migrateops.Remove(dir, 99, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.ErrorMatches, `migration version 99 not found in .*`)
}

// TestAutoRejectsAmbiguousBothSums guards that auto refuses to guess when both
// ptah.sum and atlas.sum exist, matching the verification subsystem, instead of
// silently rewriting the wrong one.
func TestAutoRejectsAmbiguousBothSums(t *testing.T) {
	c := qt.New(t)
	dir := ptahFixture(t) // writes ptah.sum
	c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"), []byte("h1:x\n"), 0o600), qt.IsNil)
	_, err := migrateops.Remove(dir, 2, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.ErrorMatches, `both ptah.sum and atlas.sum exist.*`)
}

// TestAutoDetectsAtlasWithoutSum guards that auto detects an Atlas directory from
// its file content when no sum file exists yet, consistent with hash/validate,
// rather than falling back to ptah and reporting the file as unrecognized.
func TestAutoDetectsAtlasWithoutSum(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	files := map[string]string{
		"20230101120000_first.up.sql":    "CREATE TABLE a (id int);\n",
		"20230101120000_first.down.sql":  "DROP TABLE a;\n",
		"20230102120000_second.up.sql":   "CREATE TABLE b (id int);\n",
		"20230102120000_second.down.sql": "DROP TABLE b;\n",
	}
	for name, sql := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
	}
	// No sum file at all — auto must detect atlas from the 14-digit names.
	res, err := migrateops.Remove(dir, 20230101120000, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(res.SumFile, qt.Equals, "atlas.sum")
	validates(c, dir, migrator.MigrationDirFormatAtlas)
}

func TestRebaseMovesToEnd(t *testing.T) {
	c := qt.New(t)
	dir := ptahFixture(t)

	newVersion, res, err := migrateops.Rebase(dir, 1, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(newVersion > 3, qt.IsTrue, qt.Commentf("new version %d must exceed the previous max 3", newVersion))
	c.Assert(res.SumFile, qt.Equals, "ptah.sum")

	// The original pair is gone; a re-timestamped pair with the same description
	// exists; the directory validates.
	_, statErr := os.Stat(filepath.Join(dir, "0000000001_first.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	moved := migrator.GenerateMigrationFileName(newVersion, "first", "up")
	_, statErr = os.Stat(filepath.Join(dir, moved))
	c.Assert(statErr, qt.IsNil)
	validates(c, dir, migrator.MigrationDirFormatPtah)
}

func TestRebaseAlreadyLast(t *testing.T) {
	c := qt.New(t)
	dir := ptahFixture(t)
	_, _, err := migrateops.Rebase(dir, 3, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.ErrorMatches, `migration version 3 is already last.*`)
}

func TestRehashAfterEdit(t *testing.T) {
	c := qt.New(t)
	dir := ptahFixture(t)

	// Edit a migration in place — the committed sum no longer matches.
	c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_second.up.sql"), []byte("CREATE TABLE b (id bigint);\n"), 0o600), qt.IsNil)
	pre, err := migratesum.VerifyDirWithFormat(dir, migrator.MigrationDirFormatPtah)
	c.Assert(err, qt.IsNil)
	c.Assert(pre.OK(), qt.IsFalse)

	res, err := migrateops.Rehash(dir, migrator.MigrationDirFormatAuto)
	c.Assert(err, qt.IsNil)
	c.Assert(res.SumFile, qt.Equals, "ptah.sum")
	validates(c, dir, migrator.MigrationDirFormatPtah)
}

func TestEnsureNotApplied_Refused(t *testing.T) {
	c := qt.New(t)
	c.Assert(migrateops.EnsureNotApplied([]int64{1, 2}, 2), qt.ErrorMatches, `migration version 2 is already applied.*`)
}

func TestEnsureNotApplied_Allowed(t *testing.T) {
	c := qt.New(t)
	c.Assert(migrateops.EnsureNotApplied([]int64{1}, 2), qt.IsNil)
	c.Assert(migrateops.EnsureNotApplied(nil, 2), qt.IsNil)
}

// TestAtlasRoundTrip proves the operations keep an atlas.sum directory consistent.
func TestAtlasRoundTrip(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	files := map[string]string{
		"20230101120000_first.up.sql":    "CREATE TABLE a (id int);\n",
		"20230101120000_first.down.sql":  "DROP TABLE a;\n",
		"20230102120000_second.up.sql":   "CREATE TABLE b (id int);\n",
		"20230102120000_second.down.sql": "DROP TABLE b;\n",
	}
	for name, sql := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	res, err := migrateops.Remove(dir, 20230101120000, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(res.SumFile, qt.Equals, "atlas.sum")
	validates(c, dir, migrator.MigrationDirFormatAtlas)
}

// TestAtlasRebase proves rebase re-timestamps an atlas-format pair (preserving
// the timestamp width) and keeps atlas.sum consistent.
func TestAtlasRebase(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	files := map[string]string{
		"20230101120000_first.up.sql":    "CREATE TABLE a (id int);\n",
		"20230101120000_first.down.sql":  "DROP TABLE a;\n",
		"20230102120000_second.up.sql":   "CREATE TABLE b (id int);\n",
		"20230102120000_second.down.sql": "DROP TABLE b;\n",
	}
	for name, sql := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(sql), 0o600), qt.IsNil)
	}
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)

	newVersion, res, err := migrateops.Rebase(dir, 20230101120000, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(newVersion > 20230102120000, qt.IsTrue)
	c.Assert(res.SumFile, qt.Equals, "atlas.sum")
	_, statErr := os.Stat(filepath.Join(dir, "20230101120000_first.up.sql"))
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	validates(c, dir, migrator.MigrationDirFormatAtlas)
}
