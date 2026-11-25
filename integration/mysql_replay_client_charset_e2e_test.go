//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/envbool/envbooltest"
	"ptah.run/internal/migratesum"
	"ptah.run/internal/replaytrust"
	"ptah.run/migration/migrationfile"
)

// The replay guard reads each statement as UTF-8. A dev session whose client
// character set is outside that family reads the same bytes by other rules:
// under latin1 the byte 0xA0 is white space, so the migration below defines a
// trigger the guard reads as no statement it knows, and the trigger writes
// another database when the replay inserts its row. A charset or a collation
// parameter of the dev URL gives the session that character set, and the
// replay is refused before any statement runs.

// latin1DevURLs are the dev URL parameters that give the session latin1.
var latin1DevURLs = []struct {
	name, parameter, value string
}{
	{name: "a charset parameter", parameter: "charset", value: "latin1"},
	{name: "a collation parameter", parameter: "collation", value: "latin1_swedish_ci"},
}

// latin1Engines are the MySQL-family servers, each with the dialect the guard
// that refuses names.
var latin1Engines = []struct {
	name    string
	admin   dbtarget.Engine
	dialect string
}{
	{name: "MySQL", admin: dbtarget.MySQLAdmin, dialect: "mysql"},
	{name: "MariaDB", admin: dbtarget.MariaDBAdmin, dialect: "mariadb"},
}

// latin1Trigger is a migration that, on a latin1 session, creates a trigger
// writing 81 into the t of the database {other} names, and fires it.
const latin1Trigger = "CREATE TABLE t (id INT);\n" +
	"CREATE\xa0TRIGGER tr8 AFTER INSERT ON t FOR EACH ROW INSERT INTO {other}.t VALUES (81);\n" +
	"INSERT INTO t VALUES (1);\n"

// charsetMigration writes a hashed migration directory holding migration,
// with {other} standing for the quoted name of database, and returns its path.
func charsetMigration(c *qt.C, migration, database string) string {
	c.Helper()
	dir := filepath.Join(c.TempDir(), "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(dir, "20260101000000_charset.sql"),
		[]byte(strings.ReplaceAll(migration, "{other}", "`"+database+"`")), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrationfile.DirFormatAtlas)
	c.Assert(err, qt.IsNil)
	return dir
}

// rowsOfT reads the ids of database's t back, in order.
func rowsOfT(c *qt.C, scratch mysqlScratch, database string) string {
	c.Helper()
	var rows string
	c.Assert(scratch.admin.QueryRowContext(c.Context(),
		"SELECT COALESCE(GROUP_CONCAT(id ORDER BY id), '') FROM `"+database+"`.t").Scan(&rows), qt.IsNil)
	return rows
}

func TestMigrateValidateRefusesALatin1DevSessionE2E(t *testing.T) {
	for _, engine := range latin1Engines {
		for _, url := range latin1DevURLs {
			t.Run(engine.name+"/"+url.name, func(t *testing.T) {
				c := qt.New(t)
				envbooltest.Unset(replaytrust.EnvVar)(t)
				scratch := newMySQLFamilyScratch(c, engine.admin)
				other, _ := scratch.builtAs(c, "cs_other", "CREATE TABLE t (id int PRIMARY KEY);\nINSERT INTO t VALUES (1);")
				_, devURL := scratch.database(c, "cs_dev")
				dir := charsetMigration(c, latin1Trigger, other)

				output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", withURLParameter(devURL, url.parameter, url.value))

				c.Assert(err, qt.ErrorMatches, `(?s).*the dev session's client character set is "latin1", and the `+engine.dialect+
					` replay guard reads each statement as UTF-8: .*; if you trust this SQL to run with the dev connection's privileges, `+
					`set PTAH_TRUST_REPLAY_SQL=1.*`, qt.Commentf("%s", output))
				c.Assert(rowsOfT(c, scratch, other), qt.Equals, "1")
			})
		}
	}
}

// TestMigrateValidateTrustsALatin1DevSessionE2E is the trusted side: a run
// that trusts its SQL replays on the latin1 session, and the history it
// replays here needs nothing else lifted. The control is the same history
// without trust, refused for the session alone.
func TestMigrateValidateTrustsALatin1DevSessionE2E(t *testing.T) {
	const history = "CREATE TABLE t (id INT);\nINSERT INTO t VALUES (1);\n"
	for _, engine := range latin1Engines {
		t.Run(engine.name+"/trusted", func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(replaytrust.EnvVar, "1")(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			_, devURL := scratch.database(c, "cs_dev")
			dir := charsetMigration(c, history, "")

			output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", withURLParameter(devURL, "charset", "latin1"))

			c.Assert(err, qt.IsNil, qt.Commentf("%s", output))
		})
		t.Run(engine.name+"/not trusted", func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(replaytrust.EnvVar)(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			_, devURL := scratch.database(c, "cs_dev")
			dir := charsetMigration(c, history, "")

			output, err := runCompatVerb("migrate", "validate", "--dir", "file://"+dir, "--dev-url", withURLParameter(devURL, "charset", "latin1"))

			c.Assert(err, qt.ErrorMatches, `(?s).*the dev session's client character set is "latin1".*`, qt.Commentf("%s", output))
		})
	}
}
