package importer_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
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

	result, err := importer.Emit(out, normalized, nil, importer.Options{})
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

// A migration file is a text file, and a text file's last line has a
// terminator. The importers differ in what they hand Emit: golang-migrate and
// Flyway pass whole files, which already end with a newline, while Goose and
// dbmate pass a section carved out of one, which ends wherever its last
// statement did. Without a terminator at the emit boundary the second pair
// writes files git reports as "\ No newline at end of file", and every
// line-oriented reader of them has to special-case the last line.
//
// The rows are the two shapes that reach Emit, driven through the public
// import path rather than through the helper, so a body that stops gaining its
// newline reddens here.
func TestImportTerminatesEveryWrittenFileWithANewline(t *testing.T) {
	tests := []struct {
		name   string
		file   string
		source string
	}{
		{
			name: "goose section ending at a statement block",
			file: "1_trigger.sql",
			source: "-- +goose Up\n" +
				"-- +goose StatementBegin\n" +
				"CREATE TRIGGER t AFTER UPDATE ON users\nBEGIN\n  SELECT 1;\nEND;\n" +
				"-- +goose StatementEnd\n" +
				"\n-- +goose Down\n-- +goose StatementBegin\nDROP TRIGGER t;\n-- +goose StatementEnd\n",
		},
		{
			name:   "dbmate section ending at a statement",
			file:   "20240101000000_users.sql",
			source: "-- migrate:up\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n-- migrate:down\nDROP TABLE users;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			out := t.TempDir()
			result, err := importer.Import(
				fstest.MapFS{test.file: {Data: []byte(test.source)}}, nil, out, importer.Options{})
			c.Assert(err, qt.IsNil)
			c.Assert(len(result.Files) > 0, qt.IsTrue,
				qt.Commentf("the fixture produced no files, so the assertion below measures nothing"))

			for _, name := range result.Files {
				body, err := os.ReadFile(filepath.Join(out, name))
				c.Assert(err, qt.IsNil)
				c.Assert(strings.HasSuffix(string(body), "\n"), qt.IsTrue,
					qt.Commentf("%s does not end with a newline", name))
				c.Assert(strings.HasSuffix(string(body), "\n\n"), qt.IsFalse,
					qt.Commentf("%s gained a blank line at the end", name))
			}
		})
	}
}
