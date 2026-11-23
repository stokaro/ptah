package migratebaseline_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// --shadow-db replays the history being baselined as each file ran, with no
// mode stated. SQLite refuses VACUUM inside a transaction, so a history whose
// migration 1 holds one beside another statement ran without a transaction,
// and replays without one.

// vacuumBaselineTarget writes a history whose migration 1 holds a VACUUM, and a
// SQLite target that holds its schema and no revision row. It returns the
// directory and the target's URL.
func vacuumBaselineTarget(c *qt.C) (dir, dbURL string) {
	c.Helper()
	dir = c.TempDir()
	for name, body := range map[string]string{
		"0000000001_items.up.sql":   "CREATE TABLE items (id INTEGER PRIMARY KEY);\nVACUUM;\n",
		"0000000001_items.down.sql": "DROP TABLE items;\n",
	} {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600), qt.IsNil)
	}
	dbURL = atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "target.db"))
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(c.Context(), "CREATE TABLE items (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	return dir, dbURL
}

func TestMigrateBaselineShadowDB_HappyPathReplaysEachFileAsItRan(t *testing.T) {
	c := qt.New(t)
	dir, dbURL := vacuumBaselineTarget(c)

	out, err := runBaseline("--db-url", dbURL, "--migrations-dir", dir,
		"--shadow-db", atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "shadow.db")))

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
}

// The replay takes no transaction mode, so --tx-mode is refused before the
// directory is read.
func TestMigrateBaselineShadowDB_FailurePathTakesNoTxMode(t *testing.T) {
	c := qt.New(t)
	dir, dbURL := vacuumBaselineTarget(c)

	out, err := runBaseline("--db-url", dbURL, "--migrations-dir", dir,
		"--shadow-db", atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "shadow.db")), "--tx-mode", "none")

	c.Assert(err, qt.ErrorMatches, `unknown flag: --tx-mode`, qt.Commentf("%s", out))
}
