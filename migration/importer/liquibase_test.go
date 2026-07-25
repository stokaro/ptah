package importer_test

import (
	"os"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/importer"
)

const liquibaseFormattedSQL = `--liquibase formatted sql

--changeset alice:create-users
CREATE TABLE users (id integer PRIMARY KEY);
--rollback DROP TABLE users;

--changeset bob:add-email
ALTER TABLE users ADD email text;
-- a normal comment stays in the up
--rollback ALTER TABLE users DROP COLUMN email;

--changeset alice:seed
INSERT INTO users (id) VALUES (1);
`

func liquibaseFS() fstest.MapFS {
	return fstest.MapFS{
		"changelog.sql": {Data: []byte(liquibaseFormattedSQL)},
		"README.md":     {Data: []byte("# migrations")},
	}
}

func TestLiquibaseDetectAndResolve(t *testing.T) {
	c := qt.New(t)

	parser, err := importer.DetectParser(liquibaseFS())
	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "liquibase")

	byName, err := importer.ParserByName("liquibase")
	c.Assert(err, qt.IsNil)
	c.Assert(byName.Name(), qt.Equals, "liquibase")
}

// TestLiquibaseParse checks that changesets are split by `--changeset`, that
// `--rollback` lines become the down, that a normal SQL comment is kept in the
// up, that a changeset without a rollback has an empty down, and that changesets
// are assigned sequential versions with the author:id carried into the name.
func TestLiquibaseParse(t *testing.T) {
	c := qt.New(t)
	parser, _ := importer.ParserByName("liquibase")

	migrations, err := parser.Parse(liquibaseFS())
	c.Assert(err, qt.IsNil)
	normalized, err := importer.Normalize(migrations)
	c.Assert(err, qt.IsNil)
	c.Assert(normalized, qt.HasLen, 3)

	c.Assert(normalized[0].Version, qt.Equals, int64(1))
	c.Assert(normalized[0].Name, qt.Equals, "alice_create_users")
	c.Assert(normalized[0].UpSQL, qt.Contains, "CREATE TABLE users")
	c.Assert(normalized[0].DownSQL, qt.Equals, "DROP TABLE users;")

	c.Assert(normalized[1].Version, qt.Equals, int64(2))
	c.Assert(normalized[1].Name, qt.Equals, "bob_add_email")
	c.Assert(normalized[1].UpSQL, qt.Contains, "ALTER TABLE users ADD email")
	c.Assert(normalized[1].UpSQL, qt.Contains, "-- a normal comment stays in the up") // non-directive comment kept
	c.Assert(normalized[1].UpSQL, qt.Not(qt.Contains), "--rollback")                  // rollback directive not in up
	c.Assert(normalized[1].DownSQL, qt.Equals, "ALTER TABLE users DROP COLUMN email;")

	// Third changeset has no rollback -> empty down (placeholder written on emit).
	c.Assert(normalized[2].Version, qt.Equals, int64(3))
	c.Assert(normalized[2].Name, qt.Equals, "alice_seed")
	c.Assert(normalized[2].DownSQL, qt.Equals, "")
}

func TestLiquibaseImportEndToEnd(t *testing.T) {
	c := qt.New(t)
	out := t.TempDir()

	result, err := importer.Import(liquibaseFS(), nil, out, importer.Options{})
	c.Assert(err, qt.IsNil)
	// 3 changesets -> 3 up + 3 down Ptah files.
	c.Assert(result.Files, qt.HasLen, 6)
	c.Assert(result.SumFile, qt.Equals, "ptah.sum")
	c.Assert(result.Files, qt.Contains, "0000000001_alice_create_users.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000002_bob_add_email.up.sql")
	c.Assert(result.Files, qt.Contains, "0000000003_alice_seed.up.sql")

	down, err := os.ReadFile(filepath.Join(out, "0000000001_alice_create_users.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Contains, "DROP TABLE users;")

	placeholder, err := os.ReadFile(filepath.Join(out, "0000000003_alice_seed.down.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(string(placeholder), qt.Contains, "No rollback")
}

// TestLiquibaseRejectsXMLChangelog checks that an XML/YAML/JSON changelog is
// detected as Liquibase but rejected with an actionable message rather than
// silently ignored or reported as an unknown tool.
func TestLiquibaseRejectsXMLChangelog(t *testing.T) {
	c := qt.New(t)

	xmlFS := fstest.MapFS{
		"db.changelog.xml": {Data: []byte(`<?xml version="1.0" encoding="UTF-8"?>
<databaseChangeLog>
  <changeSet id="1" author="alice">
    <createTable tableName="users"/>
  </changeSet>
</databaseChangeLog>
`)},
	}
	// Auto-detect picks Liquibase (so the user gets the specific message)...
	parser, err := importer.DetectParser(xmlFS)
	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "liquibase")
	// ...and Parse rejects with an actionable, format-naming error.
	_, err = parser.Parse(xmlFS)
	c.Assert(err, qt.ErrorMatches, `.*XML/YAML/JSON changelogs are not yet supported.*db\.changelog\.xml.*`)
}

func TestLiquibaseParseErrors(t *testing.T) {
	tests := []struct {
		name string
		fsys fstest.MapFS
		re   string
	}{
		{
			name: "duplicate changeset",
			fsys: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n" +
					"--changeset alice:1\nSELECT 1;\n" +
					"--changeset alice:1\nSELECT 2;\n")}},
			re: `.*duplicate liquibase changeset alice:1 .*`,
		},
		{
			name: "changeset missing author:id",
			fsys: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n--changeset just-an-id\nSELECT 1;\n")}},
			re: `.*missing author:id.*`,
		},
		{
			name: "bare changeset marker errors, not silently merged",
			fsys: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n--changeset alice:1\nSELECT 1;\n--changeset\nSELECT 2;\n")}},
			re: `.*missing author:id.*`,
		},
		{
			name: "formatted sql alongside xml changelog rejected",
			fsys: fstest.MapFS{
				"changelog.sql": {Data: []byte("--liquibase formatted sql\n--changeset a:b\nSELECT 1;\n")},
				"master.xml":    {Data: []byte("<databaseChangeLog></databaseChangeLog>")},
			},
			re: `.*XML/YAML/JSON changelogs are not yet supported.*master\.xml.*`,
		},
		{
			name: "changeset without SQL",
			fsys: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\n--changeset a:b\n--rollback DROP TABLE t;\n")}},
			re: `.*changeset a:b .* has no SQL.*`,
		},
		{
			name: "sql before first changeset",
			fsys: fstest.MapFS{"changelog.sql": {Data: []byte(
				"--liquibase formatted sql\nCREATE TABLE stray (id int);\n--changeset a:b\nSELECT 1;\n")}},
			re: `.*has SQL before the first --changeset.*`,
		},
		{
			name: "header only, no changesets",
			fsys: fstest.MapFS{"changelog.sql": {Data: []byte("--liquibase formatted sql\n-- just a comment\n")}},
			re:   `.*contain no --changeset markers.*`,
		},
		{
			name: "no liquibase files",
			fsys: fstest.MapFS{"schema.sql": {Data: []byte("CREATE TABLE t (id int);")}},
			re:   `.*no liquibase formatted-SQL changelogs.*`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			parser, _ := importer.ParserByName("liquibase")
			_, err := parser.Parse(tt.fsys)
			c.Assert(err, qt.ErrorMatches, tt.re)
		})
	}
}

// TestLiquibaseDistinguishedFromGoose proves the formatted-SQL header, not a file
// name, drives detection, and that a Goose directory is not read as Liquibase.
func TestLiquibaseDistinguishedFromGoose(t *testing.T) {
	c := qt.New(t)

	liquibase, err := importer.DetectParser(liquibaseFS())
	c.Assert(err, qt.IsNil)
	c.Assert(liquibase.Name(), qt.Equals, "liquibase")

	goose, err := importer.DetectParser(gooseFS())
	c.Assert(err, qt.IsNil)
	c.Assert(goose.Name(), qt.Equals, "goose")
}

// TestLiquibaseChangelogDetectionIgnoresProseMention guards against a stray
// textual "databaseChangeLog" mention claiming a directory: a golang-migrate
// directory with a JSON file that only mentions the word in prose must still
// resolve unambiguously to golang-migrate.
func TestLiquibaseChangelogDetectionIgnoresProseMention(t *testing.T) {
	c := qt.New(t)

	parser, err := importer.DetectParser(fstest.MapFS{
		"1_init.up.sql": {Data: []byte("CREATE TABLE t (id int);")},
		"notes.json":    {Data: []byte(`{"note": "migrated away from databaseChangeLog files"}`)},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(parser.Name(), qt.Equals, "golang-migrate")
}
