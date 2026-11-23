package migratebaseline_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
)

// --shadow-db replays the history being baselined in --tx-mode, the mode it
// was applied in. SQLite refuses VACUUM inside a transaction, so a history
// whose migration 1 holds one was applied under none and replays only there.

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

func TestMigrateBaselineShadowDB_HappyPathReplaysInTheTxMode(t *testing.T) {
	c := qt.New(t)
	dir, dbURL := vacuumBaselineTarget(c)

	out, err := runBaseline("--db-url", dbURL, "--migrations-dir", dir,
		"--shadow-db", atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "shadow.db")), "--tx-mode", "none")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
}

// Without --tx-mode the history replays a file to a transaction and the
// VACUUM is refused; a value that names no mode is refused before the
// directory is read.
func TestMigrateBaselineShadowDB_FailurePathReplaysInTheTxMode(t *testing.T) {
	for _, test := range []struct {
		name    string
		args    []string
		wantErr string
	}{
		{name: "no mode", wantErr: `(?s).*0000000001_items\.up\.sql cannot run inside a transaction: line 2 VACUUM is refused inside a transaction block.*`},
		{name: "a mode that does not exist", args: []string{"--tx-mode", "each"}, wantErr: `invalid tx-mode "each": expected file, all, or none`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir, dbURL := vacuumBaselineTarget(c)

			out, err := runBaseline(append([]string{"--db-url", dbURL, "--migrations-dir", dir,
				"--shadow-db", atlasurl.SQLiteURLFromPath(filepath.Join(c.TempDir(), "shadow.db"))}, test.args...)...)

			c.Assert(err, qt.ErrorMatches, test.wantErr, qt.Commentf("%s", out))
		})
	}
}
