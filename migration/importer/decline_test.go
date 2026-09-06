package importer_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/importer"
)

// Every file under the source tree is accounted for -- stokaro/ptah#2231.
//
// The importer used to drop whatever its parser did not recognize with
// `continue // ignore non-migration files`, in all five parsers. A migration one
// directory down, a name off by one character, or an extension in the wrong case
// left no trace in the output, and ptah.sum was then written over the surviving
// subset -- so the truncated directory validated clean and nothing downstream
// could establish that SQL had been lost.
//
// The accounting is done by walking the tree rather than by asking each parser
// to remember, so a parser that forgets to record a file it used over-reports
// rather than under-reports.
func TestAccountForSource_NamesEveryFileTheImportDidNotConvert(t *testing.T) {
	tests := []struct {
		name     string
		tool     string
		fsys     fstest.MapFS
		declined map[string]string // path -> a fragment of the reason
	}{
		{
			name: "a migration one directory down",
			tool: "golang-migrate",
			fsys: fstest.MapFS{
				"000001_create.up.sql":     {Data: []byte("CREATE TABLE t (id INTEGER);")},
				"000001_create.down.sql":   {Data: []byte("DROP TABLE t;")},
				"tenant/000002_add.up.sql": {Data: []byte("ALTER TABLE t ADD c TEXT;")},
			},
			declined: map[string]string{
				"tenant/000002_add.up.sql": "reads only the top level",
			},
		},
		{
			name: "a name missing its direction segment, and one shouting its extension",
			tool: "golang-migrate",
			fsys: fstest.MapFS{
				"000001_create.up.sql":   {Data: []byte("CREATE TABLE t (id INTEGER);")},
				"000001_create.down.sql": {Data: []byte("DROP TABLE t;")},
				"000002_add_index.sql":   {Data: []byte("CREATE INDEX i ON t (id);")},
				"000003_add.up.SQL":      {Data: []byte("CREATE INDEX j ON t (id);")},
			},
			declined: map[string]string{
				"000002_add_index.sql": "is not a golang-migrate migration file name",
				"000003_add.up.SQL":    "is not a golang-migrate migration file name",
			},
		},
		{
			name: "a Flyway baseline and a Flyway callback are named, not lumped in",
			tool: "flyway",
			fsys: fstest.MapFS{
				"V1__create.sql":          {Data: []byte("CREATE TABLE t (id INTEGER);")},
				"B3__squash_baseline.sql": {Data: []byte("SELECT 1;")},
				"afterMigrate__log.sql":   {Data: []byte("SELECT 2;")},
			},
			declined: map[string]string{
				"B3__squash_baseline.sql": "Flyway baseline",
				"afterMigrate__log.sql":   "Flyway callback",
			},
		},
		{
			name: "a Goose name with no marker is not the same as an unrecognized name",
			tool: "goose",
			fsys: fstest.MapFS{
				"00001_create.sql": {Data: []byte("-- +goose Up\nCREATE TABLE t (id INTEGER);\n")},
				"00002_add.sql":    {Data: []byte("CREATE INDEX i ON t (id);\n")},
			},
			declined: map[string]string{
				"00002_add.sql": "no -- +goose Up marker",
			},
		},
		{
			name: "a dbmate name with no directive",
			tool: "dbmate",
			fsys: fstest.MapFS{
				"20240101000000_create.sql": {Data: []byte("-- migrate:up\nCREATE TABLE t (id INTEGER);\n")},
				"20240102000000_add.sql":    {Data: []byte("CREATE INDEX i ON t (id);\n")},
			},
			declined: map[string]string{
				"20240102000000_add.sql": "no -- migrate:up directive",
			},
		},
		{
			name: "a README is reported too, because the importer cannot tell it from a migration",
			tool: "golang-migrate",
			fsys: fstest.MapFS{
				"000001_create.up.sql":   {Data: []byte("CREATE TABLE t (id INTEGER);")},
				"000001_create.down.sql": {Data: []byte("DROP TABLE t;")},
				"README.md":              {Data: []byte("how to run these\n")},
			},
			declined: map[string]string{
				"README.md": "is not a golang-migrate migration file name",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			parser, err := importer.ParserByName(test.tool)
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(test.fsys)
			c.Assert(err, qt.IsNil)
			declined, err := importer.AccountForSource(test.fsys, parser, parsed)
			c.Assert(err, qt.IsNil)

			byPath := make(map[string]string, len(declined))
			for _, entry := range declined {
				byPath[entry.Path] = entry.Reason
			}
			c.Assert(byPath, qt.HasLen, len(test.declined))
			for path, fragment := range test.declined {
				c.Assert(byPath[path], qt.Contains, fragment,
					qt.Commentf("declined %q", path))
			}
		})
	}
}

// A declined file that cannot carry SQL does not block the import.
//
// The refusal exists to stop ptah.sum certifying a directory whose SQL was
// dropped. A migrations directory almost always holds a README, and refusing
// every import that saw one would make the guard something users route around
// rather than read -- so the guard is drawn at the material this command
// converts.
func TestDeclinedFile_OnlySQLBlocksTheChecksum(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		wantBlocks bool
	}{
		{name: "a dropped migration blocks", path: "tenant/000002_add.up.sql", wantBlocks: true},
		{name: "a shouted extension still blocks", path: "000003_add.up.SQL", wantBlocks: true},
		{name: "a README does not", path: "README.md", wantBlocks: false},
		{name: "a git keepfile does not", path: ".gitkeep", wantBlocks: false},
		{name: "a Flyway script migration does not", path: "V3__load.ps1", wantBlocks: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fstest.MapFS{
				"000001_create.up.sql":   {Data: []byte("CREATE TABLE t (id INTEGER);")},
				"000001_create.down.sql": {Data: []byte("DROP TABLE t;")},
				test.path:                {Data: []byte("anything")},
			}
			parser, err := importer.ParserByName("golang-migrate")
			c.Assert(err, qt.IsNil)

			parsed, err := parser.Parse(fsys)
			c.Assert(err, qt.IsNil)
			declined, err := importer.AccountForSource(fsys, parser, parsed)
			c.Assert(err, qt.IsNil)

			// Reported either way: the report is not what the guard gates.
			c.Assert(declined, qt.HasLen, 1)
			c.Assert(declined[0].Path, qt.Equals, test.path)

			blocking := importer.BlockingDeclines(declined)
			c.Assert(len(blocking) > 0, qt.Equals, test.wantBlocks)
		})
	}
}
