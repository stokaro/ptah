package migrateup_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/migrateup"
)

// The drift tests apply a directory whose repeatable migration builds a view,
// add a second versioned migration, and run up with --check-drift. The view
// is the object only the replay of the repeatable migration creates.

const driftRepeatableView = "DROP VIEW IF EXISTS user_emails;\nCREATE VIEW user_emails AS SELECT email FROM drift_users;\n"

// appliedRepeatableDrift applies the directory to a new SQLite database, adds
// the second versioned migration, runs each statement of outOfBand on the
// database, and returns the directory, the database and a shadow URL.
func appliedRepeatableDrift(c *qt.C, outOfBand ...string) (dir, dbPath, shadowURL string) {
	c.Helper()
	dir = c.TempDir()
	files := map[string]string{
		"0000000001_users.up.sql":   "CREATE TABLE drift_users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);\n",
		"0000000001_users.down.sql": "DROP TABLE drift_users;\n",
		"R__user_emails.sql":        driftRepeatableView,
	}
	for name, content := range files {
		c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600), qt.IsNil)
	}
	dbPath = filepath.Join(c.TempDir(), "app.db")
	_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir)
	c.Assert(err, qt.IsNil)
	seqWriteMigrations(c, dir, 2)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	for _, statement := range outOfBand {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	return dir, dbPath, "sqlite://" + filepath.Join(c.TempDir(), "shadow.db")
}

// viewSQL reads the definition SQLite stores for the view.
func viewSQL(c *qt.C, dbPath string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var definition string
	c.Assert(conn.QueryRowContext(c.Context(),
		`SELECT sql FROM sqlite_master WHERE type = 'view' AND name = 'user_emails'`).Scan(&definition), qt.IsNil)
	return definition
}

func TestCheckDriftRepeatables_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath, shadowURL := appliedRepeatableDrift(c)

	out, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
		"--check-drift", "--shadow-db", shadowURL)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Contains,
		"Drift check: the database matches the schema its 1 applied migration and 1 repeatable migration produce\n")
	c.Assert(seqRevisions(c, dbPath), qt.DeepEquals, []int64{1, 2})
}

func TestCheckDriftRepeatables_FailurePath(t *testing.T) {
	t.Run("a view the repeatable migration created and the database lost is drift", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath, shadowURL := appliedRepeatableDrift(c, "DROP VIEW user_emails")

		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
			"--check-drift", "--shadow-db", shadowURL)

		c.Assert(err, qt.ErrorAs, new(*migrateup.DriftError))
		c.Assert(err, qt.ErrorMatches, `error running migrations: the database schema differs from the schema its 1 applied migration and `+
			`1 repeatable migration produce, in 1 place, so no pending migration was run; first: missing view user_emails`)
		c.Assert(seqRevisions(c, dbPath), qt.DeepEquals, []int64{1})
	})

	t.Run("a repeatable migration that changed after it applied cannot be replayed", func(t *testing.T) {
		c := qt.New(t)
		dir, dbPath, shadowURL := appliedRepeatableDrift(c)
		changed := "DROP VIEW IF EXISTS user_emails;\nCREATE VIEW user_emails AS SELECT id, email FROM drift_users;\n"
		c.Assert(os.WriteFile(filepath.Join(dir, "R__user_emails.sql"), []byte(changed), 0o600), qt.IsNil)

		_, err := runUpThroughRoot("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir,
			"--check-drift", "--shadow-db", shadowURL)

		c.Assert(err, qt.ErrorAs, new(*migrateup.DriftCheckUnavailableError))
		c.Assert(err, qt.ErrorMatches, `error running migrations: the drift check could not compare the database with its applied history, `+
			`so no pending migration was run: the applied history cannot be replayed: repeatable migration user_emails: `+
			`R__user_emails\.sql changed after it applied, and the content the database ran is not in the directory`)
		c.Assert(seqRevisions(c, dbPath), qt.DeepEquals, []int64{1})
		c.Assert(viewSQL(c, dbPath), qt.Equals, "CREATE VIEW user_emails AS SELECT email FROM drift_users")
	})
}
