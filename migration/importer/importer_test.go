package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/importer"
)

func golangMigrateFS() fstest.MapFS {
	return fstest.MapFS{
		"1_init.up.sql":        {Data: []byte("CREATE TABLE users (id int);\n")},
		"1_init.down.sql":      {Data: []byte("DROP TABLE users;\n")},
		"2_add_email.up.sql":   {Data: []byte("ALTER TABLE users ADD email text;\n")},
		"2_add_email.down.sql": {Data: []byte("ALTER TABLE users DROP email;\n")},
		"3_no_rollback.up.sql": {Data: []byte("CREATE INDEX idx ON users (email);\n")},
		"README.md":            {Data: []byte("# migrations")},
	}
}

func TestParserByNameAndDetect(t *testing.T) {
	c := qt.New(t)

	parser, err := importer.ParserByName("golang-migrate")
	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "golang-migrate")

	_, err = importer.ParserByName("nope")
	c.Assert(err, qt.ErrorMatches, `unsupported source tool "nope".*`)

	detected, err := importer.DetectParser(golangMigrateFS())
	c.Assert(err, qt.IsNil)
	c.Assert(detected.Name(), qt.Equals, "golang-migrate")

	_, err = importer.DetectParser(fstest.MapFS{"notes.txt": {Data: []byte("hi")}})
	c.Assert(err, qt.ErrorMatches, `could not detect the source migration tool.*`)
}

func TestGolangMigrateParse(t *testing.T) {
	c := qt.New(t)

	parser, err := importer.ParserByName("golang-migrate")
	c.Assert(err, qt.IsNil)

	migrations, err := parser.Parse(golangMigrateFS())
	c.Assert(err, qt.IsNil)

	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	c.Assert(normalized, qt.HasLen, 3)

	c.Assert(normalized[0], qt.DeepEquals, importer.SourceMigration{
		Version: 1, Name: "init",
		UpSQL: "CREATE TABLE users (id int);\n", DownSQL: "DROP TABLE users;\n",
	})
	c.Assert(normalized[1].Version, qt.Equals, int64(2))
	c.Assert(normalized[1].Name, qt.Equals, "add_email")
	// A migration with no .down.sql keeps an empty DownSQL.
	c.Assert(normalized[2].Version, qt.Equals, int64(3))
	c.Assert(normalized[2].DownSQL, qt.Equals, "")
}

func TestGolangMigrateParseTimestampVersions(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"20230102030405_a.up.sql": {Data: []byte("SELECT 1;")},
		"20230101020304_b.up.sql": {Data: []byte("SELECT 2;")},
	}
	parser, _ := importer.ParserByName("golang-migrate")
	migrations, err := parser.Parse(fsys)
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	// Sorted ascending by version regardless of directory order.
	c.Assert(normalized[0].Version, qt.Equals, int64(20230101020304))
	c.Assert(normalized[1].Version, qt.Equals, int64(20230102030405))
}

func TestGolangMigrateParseErrors(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
		re   string
	}{
		{
			name: "down without up",
			fsys: fstest.MapFS{"1_x.down.sql": {Data: []byte("DROP TABLE x;")}},
			re:   `.*has a down file but no .up.sql.*`,
		},
		{
			name: "no migration files at all",
			fsys: fstest.MapFS{"notes.txt": {Data: []byte("hi")}},
			re:   `.*no golang-migrate migration files.*`,
		},
		{
			name: "mismatched names across directions",
			fsys: fstest.MapFS{
				"1_init.up.sql":    {Data: []byte("SELECT 1;")},
				"1_other.down.sql": {Data: []byte("SELECT 2;")},
			},
			re: `.*mismatched names.*`,
		},
		{
			name: "two up files with the same numeric version",
			fsys: fstest.MapFS{
				"1_a.up.sql":      {Data: []byte("-- A")},
				"000001_a.up.sql": {Data: []byte("-- B")},
			},
			re: `.*has two up files.*`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			parser, _ := importer.ParserByName("golang-migrate")
			_, err := parser.Parse(tt.fsys)
			c.Assert(err, qt.ErrorMatches, tt.re)
		})
	}
}

func TestNormalizeDuplicateVersion(t *testing.T) {
	c := qt.New(t)
	_, err := importer.Normalize([]importer.SourceMigration{
		{Version: 1, Name: "a", UpSQL: "x"},
		{Version: 1, Name: "b", UpSQL: "y"},
	})
	c.Assert(err, qt.ErrorMatches, `duplicate source version 1 \("a" and "b"\)`)
}

func TestNormalizeRepeatableSortLast(t *testing.T) {
	c := qt.New(t)
	normalized, err := importer.Normalize([]importer.SourceMigration{
		{Repeatable: true, Name: "view", UpSQL: "v"},
		{Version: 2, Name: "b", UpSQL: "y"},
		{Version: 1, Name: "a", UpSQL: "x"},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(normalized[0].Name, qt.Equals, "a")
	c.Assert(normalized[1].Name, qt.Equals, "b")
	c.Assert(normalized[2].Name, qt.Equals, "view")
	c.Assert(normalized[2].Repeatable, qt.IsTrue)
}
