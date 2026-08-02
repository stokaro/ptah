package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/importer"
)

func dbmateFS() fstest.MapFS {
	return fstest.MapFS{
		"20240101120000_create_users.sql": {Data: []byte(
			"-- migrate:up transaction:false\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n\n-- migrate:down\nDROP TABLE users;\n",
		)},
		"20240102120000_create_orders.sql": {Data: []byte(
			"-- MIGRATE:UP\nCREATE TABLE orders (id INTEGER PRIMARY KEY);\n",
		)},
		"README.md": {Data: []byte("# migrations\n")},
	}
}

func TestDbmateParserDetectAndParse(t *testing.T) {
	c := qt.New(t)
	fsys := dbmateFS()

	parser, err := importer.DetectParser(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "dbmate")

	migrations, err := parser.Parse(fsys)
	c.Assert(err, qt.IsNil)
	c.Assert(migrations, qt.HasLen, 2)

	c.Assert(migrations[0].Version, qt.Equals, int64(20240101120000))
	c.Assert(migrations[0].Name, qt.Equals, "create_users")
	// Directive lines (including trailing options) never leak into the SQL.
	c.Assert(migrations[0].UpSQL, qt.Equals, "CREATE TABLE users (id INTEGER PRIMARY KEY);")
	c.Assert(migrations[0].DownSQL, qt.Equals, "DROP TABLE users;")

	// A case-insensitive up directive and a missing down section are valid.
	c.Assert(migrations[1].UpSQL, qt.Equals, "CREATE TABLE orders (id INTEGER PRIMARY KEY);")
	c.Assert(migrations[1].DownSQL, qt.Equals, "")
}

func TestDbmateParserRejectsEmptyUpSection(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_empty.sql": {Data: []byte("-- migrate:up\n\n-- migrate:down\nDROP TABLE t;\n")},
	}

	parser, err := importer.ParserByName("dbmate")
	c.Assert(err, qt.IsNil)
	_, err = parser.Parse(fsys)
	c.Assert(err, qt.ErrorMatches, `dbmate migration "1_empty.sql" has an empty up section`)
}

func TestDbmateParserIgnoresFilesWithoutUpDirective(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_something.sql": {Data: []byte("CREATE TABLE t (id int);\n")},
	}

	parser, err := importer.ParserByName("dbmate")
	c.Assert(err, qt.IsNil)
	_, err = parser.Parse(fsys)
	c.Assert(err, qt.ErrorMatches, `no dbmate migration files \(<version>_<name>\.sql with -- migrate:up\) found`)
}

func TestDbmateImportWritesPtahLayout(t *testing.T) {
	c := qt.New(t)
	outDir := t.TempDir()

	result, err := importer.Import(dbmateFS(), nil, outDir, importer.Options{})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Files, qt.Contains, "0000000001_v20240101120000_create_users.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000001_v20240101120000_create_users.down.sql")
}

func TestSupportedToolsIncludeDbmate(t *testing.T) {
	c := qt.New(t)
	c.Assert(importer.SupportedTools(), qt.Contains, "dbmate")
}
