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

	migrations, err := parseMigrations(t, parser, fsys)
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
	_, err = parseMigrations(t, parser, fsys)
	c.Assert(err, qt.ErrorMatches, `dbmate migration "1_empty.sql" has an empty up section`)
}

func TestDbmateParserIgnoresFilesWithoutUpDirective(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		"1_something.sql": {Data: []byte("CREATE TABLE t (id int);\n")},
	}

	parser, err := importer.ParserByName("dbmate")
	c.Assert(err, qt.IsNil)
	_, err = parseMigrations(t, parser, fsys)
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

// `transaction:false` is how a dbmate author says a statement cannot run
// inside a transaction, which is what CREATE INDEX CONCURRENTLY requires. The
// option is dropped from the executable SQL, correctly, and what it meant has
// to survive that: a converted migration that lost it runs inside a
// transaction and fails on a server that refuses it there.
//
// dbmate scopes the option to one direction and Ptah scopes no_transaction to
// the migration, so the rows cover a file asking for it on either side and one
// asking for nothing at all.
func TestDbmateImportKeepsTransactionFalse(t *testing.T) {
	tests := []struct {
		name          string
		up            string
		down          string
		wantDirective bool
	}{
		{name: "both directions", up: "-- migrate:up transaction:false", down: "-- migrate:down transaction:false", wantDirective: true},
		{name: "up only", up: "-- migrate:up transaction:false", down: "-- migrate:down", wantDirective: true},
		{name: "down only", up: "-- migrate:up", down: "-- migrate:down transaction:false", wantDirective: true},
		{name: "neither", up: "-- migrate:up", down: "-- migrate:down", wantDirective: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			source := test.up + "\nCREATE UNIQUE INDEX i ON users (email);\n" +
				test.down + "\nDROP INDEX i;\n"
			outDir := t.TempDir()
			_, err := importer.Import(
				fstest.MapFS{"20240215093000_index.sql": {Data: []byte(source)}}, nil, outDir, importer.Options{})
			c.Assert(err, qt.IsNil)

			for _, direction := range []string{"up", "down"} {
				body, err := os.ReadFile(filepath.Join(outDir,
					"0000000001_v20240215093000_index."+direction+".sql"))
				c.Assert(err, qt.IsNil)
				c.Assert(strings.Contains(string(body), "-- +ptah no_transaction"), qt.Equals, test.wantDirective,
					qt.Commentf("%s direction, body %q", direction, string(body)))
			}
		})
	}
}
