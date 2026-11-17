package migratelog_test

import (
	"encoding/json"
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// repeatableFixture is a SQLite database a directory with one versioned and
// one repeatable migration ran against, and its URL.
func repeatableFixture(c *qt.C) string {
	c.Helper()
	url := "sqlite://" + filepath.Join(c.TempDir(), "repeatable.db")
	conn, err := dbschema.ConnectToDatabase(c.Context(), url)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })
	fsys := fstest.MapFS{
		"0000000001_a.up.sql":   {Data: []byte("CREATE TABLE a (id INTEGER PRIMARY KEY);\n")},
		"0000000001_a.down.sql": {Data: []byte("DROP TABLE a;\n")},
		"R__a_ids.sql":          {Data: []byte("DROP VIEW IF EXISTS a_ids;\nCREATE VIEW a_ids AS SELECT id FROM a;\n")},
	}
	mig, err := migrator.NewFSMigrator(conn, fsys, migrator.WithRepeatableMigrations())
	c.Assert(err, qt.IsNil)
	c.Assert(mig.WithActor("release-bot").MigrateUp(c.Context()), qt.IsNil)
	return url
}

// A repeatable migration has no version, so the table names it by the file
// its identity comes from.
func TestLogTable_NamesARepeatableMigration(t *testing.T) {
	c := qt.New(t)
	url := repeatableFixture(c)

	stdout := runLog(c, "--db-url", url)

	c.Assert(stdout, qt.Matches,
		`STARTED +VERSION +OPERATION +OUTCOME +ACTOR +SOURCE\n`+
			`\S+ +R__a_ids +up +applied +release-bot +provided\n`+
			`\S+ +1 +up +applied +release-bot +provided\n`)
}

func TestLogJSON_NamesARepeatableMigration(t *testing.T) {
	c := qt.New(t)
	url := repeatableFixture(c)

	stdout := runLog(c, "--db-url", url, "--json")

	var records []map[string]any
	c.Assert(json.Unmarshal([]byte(stdout), &records), qt.IsNil, qt.Commentf("%s", stdout))
	c.Assert(records, qt.HasLen, 2)
	repeatable, versioned := records[0], records[1]
	c.Assert(repeatable["repeatable"], qt.Equals, "a_ids")
	c.Assert(repeatable["version"], qt.Equals, float64(0))
	c.Assert(repeatable["outcome"], qt.Equals, "applied")
	c.Assert(repeatable["checksum"], qt.Matches, `h1:.+`)
	c.Assert(versioned["version"], qt.Equals, float64(1))
	_, hasRepeatable := versioned["repeatable"]
	c.Assert(hasRepeatable, qt.IsFalse)
}
