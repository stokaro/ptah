//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// A migration replayed into a dev database the operator named runs in the
// database realm, where the replay guard refuses a statement that writes
// another database on the same server. Each migration below writes one in a
// spelling the servers take, and the database it writes is read back after
// the run: on a guard that misses the spelling, the replay writes it and the
// cleanup of the dev database leaves the write behind.
//
// A call of a stored function of another database is refused by the guard
// too, but no row here can reach it: the cleanup before the replay already
// refuses a server holding a routine outside the dev database.

// crossDatabaseReplays are migrations that write the database {other} names,
// with what the refusal names.
var crossDatabaseReplays = []struct {
	name      string
	migration string
	refused   string
}{
	{name: "an INSERT without INTO", migration: "INSERT {other}.t VALUES (2);\n", refused: `cross-database target "{other}.t"`},
	{name: "a DELETE naming its target by alias", migration: "DELETE FROM x USING {other}.t AS x;\n", refused: `cross-database target "{other}.t"`},
	{
		name:      "an ALTER TABLE renaming into another database",
		migration: "CREATE TABLE moved (id int PRIMARY KEY);\nALTER TABLE moved RENAME {other}.moved;\n",
		refused:   `cross-database target "{other}.moved"`,
	},
	{
		name:      "a write through a view over another database",
		migration: "CREATE VIEW v AS SELECT id FROM {other}.t;\nINSERT INTO v VALUES (2);\n",
		refused:   `view "v" over cross-database table "{other}.t"`,
	},
}

// otherDatabaseState answers what a run can change in database: its tables,
// and the rows of t.
func otherDatabaseState(c *qt.C, scratch mysqlScratch, database string) string {
	c.Helper()
	var state string
	c.Assert(scratch.admin.QueryRowContext(c.Context(), `
		SELECT CONCAT('tables=',
			(SELECT GROUP_CONCAT(TABLE_NAME ORDER BY TABLE_NAME) FROM information_schema.TABLES WHERE TABLE_SCHEMA = ?),
			' rows=',
			COALESCE((SELECT GROUP_CONCAT(id ORDER BY id) FROM `+"`"+database+"`"+`.t), ''))`,
		database).Scan(&state), qt.IsNil)
	return state
}

// crossDatabaseMigration writes a hashed migration directory holding
// migration, with {other} standing for the quoted name of database, and
// returns its path.
func crossDatabaseMigration(c *qt.C, migration, database string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_reach.sql"),
		[]byte(strings.ReplaceAll(migration, "{other}", "`"+database+"`")), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

func TestMigrateValidateRefusesAWriteToAnotherDatabaseE2E(t *testing.T) {
	engines := []struct {
		name  string
		admin dbtarget.Engine
	}{
		{name: "MySQL", admin: dbtarget.MySQLAdmin},
		{name: "MariaDB", admin: dbtarget.MariaDBAdmin},
	}
	for _, engine := range engines {
		for _, test := range crossDatabaseReplays {
			t.Run(engine.name+"/"+test.name, func(t *testing.T) {
				c := qt.New(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				other, _ := scratch.builtAs(c, "xdb_other", "CREATE TABLE t (id int PRIMARY KEY);\nINSERT INTO t VALUES (1);")
				_, devURL := scratch.database(c, "xdb_dev")
				dir := crossDatabaseMigration(c, test.migration, other)

				output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", devURL)

				refused := strings.ReplaceAll(test.refused, "{other}", other)
				c.Assert(err, qt.ErrorMatches, `(?s).*migration replay rejects `+regexp.QuoteMeta(refused)+` .*`,
					qt.Commentf("%s", output))
				c.Assert(otherDatabaseState(c, scratch, other), qt.Equals, "tables=t rows=1")
			})
		}
	}
}

// TestMigrateValidateRefusesASequenceOfAnotherDatabaseE2E is the same for a
// MariaDB sequence of another database, which NEXTVAL advances. The sequence
// caches 1000 values, so an advance moves the next value it has not cached
// from 1 to 1001.
func TestMigrateValidateRefusesASequenceOfAnotherDatabaseE2E(t *testing.T) {
	c := qt.New(t)
	scratch := newMySQLFamilyScratch(c, dbtarget.MariaDBAdmin)
	other, _ := scratch.builtAs(c, "xdb_other", "CREATE SEQUENCE s;")
	_, devURL := scratch.database(c, "xdb_dev")
	dir := crossDatabaseMigration(c, "SELECT NEXTVAL({other}.s);\n", other)

	output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", devURL)

	c.Assert(err, qt.ErrorMatches,
		`(?s).*migration replay rejects cross-database target "`+regexp.QuoteMeta(other+".s")+`".*`,
		qt.Commentf("%s", output))
	var next int64
	c.Assert(scratch.admin.QueryRowContext(c.Context(),
		"SELECT next_not_cached_value FROM `"+other+"`.s").Scan(&next), qt.IsNil)
	c.Assert(next, qt.Equals, int64(1))
}
