package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/importer"
)

func TestImportWritesPtahPairsAndSum(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	result, err := importer.Import(golangMigrateFS(), nil, out, importer.Options{})
	c.Assert(err, qt.IsNil)

	// Integer versions become zero-padded Ptah file names.
	c.Assert(result.Files, qt.Contains, "0000000001_init.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000001_init.down.sql")
	c.Assert(result.Files, qt.Contains, "0000000003_no_rollback.down.sql")
	c.Assert(result.SumFile, qt.Equals, "ptah.sum")

	up, err := os.ReadFile(filepath.Join(out, "0000000001_init.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "CREATE TABLE users (id int);\n")

	// A source migration with no down file gets a placeholder down.
	down, err := os.ReadFile(filepath.Join(out, "0000000003_no_rollback.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, "No rollback")

	// The integrity file was written and is non-empty.
	sum, err := os.ReadFile(filepath.Join(out, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(len(sum) > 0, qt.IsTrue)
}

func TestImportDryRunWritesNothing(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	result, err := importer.Import(golangMigrateFS(), nil, out, importer.Options{DryRun: true})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.Contains, "0000000001_init.up.sql")

	entries, err := os.ReadDir(out)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 0) // nothing written
}

func TestImportRefusesToOverwrite(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()
	// Pre-existing target file.
	c.Assert(os.WriteFile(filepath.Join(out, "0000000001_init.up.sql"), []byte("old"), 0o600), qt.IsNil)

	_, err := importer.Import(golangMigrateFS(), nil, out, importer.Options{})
	c.Assert(err, qt.ErrorMatches, `.*refusing to overwrite existing migration file .*`)
}

func TestImportRemapsWideTimestampVersions(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()
	// 14-digit golang-migrate timestamp versions do not fit Ptah's 10-digit
	// format, so they must be reassigned to sequential versions (in order),
	// carrying the original version into the description — and the result must be
	// readable by Ptah.
	src := fstest.MapFS{
		"20230102030405_init.up.sql":   {Data: []byte("CREATE TABLE t (id int);\n")},
		"20230102030405_init.down.sql": {Data: []byte("DROP TABLE t;\n")},
		"20230103040506_add.up.sql":    {Data: []byte("ALTER TABLE t ADD c text;\n")},
	}
	result, err := importer.Import(src, nil, out, importer.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Remapped, qt.IsTrue)
	// Sequential Ptah versions in source order, original version in the name.
	c.Assert(result.Files, qt.Contains, "0000000001_v20230102030405_init.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000002_v20230103040506_add.up.sql")
	c.Assert(result.SumFile, qt.Equals, "ptah.sum")
	// The written files are readable by Ptah (10-digit versions).
	_, err = os.Stat(filepath.Join(out, "0000000001_v20230102030405_init.up.sql"))
	c.Assert(err, qt.IsNil)
	sum, err := os.ReadFile(filepath.Join(out, "ptah.sum"))
	c.Assert(err, qt.IsNil)
	c.Assert(len(sum) > 0, qt.IsTrue)
}

func TestImportFallsBackForEmptySanitizedName(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()
	// A name that sanitizes to empty would produce 0000000001_.up.sql, which Ptah
	// rejects; it must fall back to a usable description.
	src := fstest.MapFS{
		"1_日本語.up.sql":   {Data: []byte("SELECT 1;")},
		"1_日本語.down.sql": {Data: []byte("SELECT 2;")},
	}
	result, err := importer.Import(src, nil, out, importer.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.Contains, "0000000001_migration.up.sql")
	_, err = os.Stat(filepath.Join(out, "0000000001_migration.up.sql"))
	c.Assert(err, qt.IsNil)
}

// TestEmitRepeatableImportedAsOneTime checks that a repeatable source migration
// (which Ptah has no concept of) is emitted as a one-time migration ordered
// after every versioned one, named "repeatable_<name>".
func TestEmitRepeatableImportedAsOneTime(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	normalized, err := importer.Normalize([]importer.SourceMigration{
		{Version: 1, Name: "init", UpSQL: "CREATE TABLE t (id int);"},
		{Repeatable: true, Name: "view", UpSQL: "CREATE VIEW v AS SELECT 1;"},
	})
	c.Assert(err, qt.IsNil)

	result, err := importer.Emit(out, normalized, importer.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.Contains, "0000000001_init.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000002_repeatable_view.up.sql")

	up, err := os.ReadFile(filepath.Join(out, "0000000002_repeatable_view.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Contains, "CREATE VIEW v")
}

func TestImportUnknownToolViaDetect(t *testing.T) {
	c := qt.New(t)
	_, err := importer.Import(fstest.MapFS{"x.txt": {Data: []byte("hi")}}, nil, t.TempDir(), importer.Options{DryRun: true})
	c.Assert(err, qt.ErrorMatches, `could not detect the source migration tool.*`)
}
