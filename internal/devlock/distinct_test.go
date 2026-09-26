package devlock_test

import (
	"errors"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/devlock"
)

// errIsTheDevDatabase is the refusal the tests hand EnsureDistinct, so an
// assertion can tell it from any error EnsureDistinct writes itself.
var errIsTheDevDatabase = errors.New("the dev database is this database")

// connectSQLiteFile opens a SQLite database file, closed when the test ends.
func connectSQLiteFile(c *qt.C, path string) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), atlasurl.SQLiteURLFromPath(path))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return conn
}

func TestEnsureDistinct_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		protected func(dir string, other *dbschema.DatabaseConnection) []devlock.Protected
	}{
		{
			name: "another database by connection",
			protected: func(_ string, other *dbschema.DatabaseConnection) []devlock.Protected {
				return []devlock.Protected{{Conn: other, Refusal: errIsTheDevDatabase}}
			},
		},
		{
			name: "another database by URL",
			protected: func(dir string, _ *dbschema.DatabaseConnection) []devlock.Protected {
				return []devlock.Protected{{URL: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "other.db")), Refusal: errIsTheDevDatabase}}
			},
		},
		{
			name: "an entry naming nothing",
			protected: func(string, *dbschema.DatabaseConnection) []devlock.Protected {
				return []devlock.Protected{{URL: "  ", Refusal: errIsTheDevDatabase}}
			},
		},
		{
			name:      "nothing to protect",
			protected: func(string, *dbschema.DatabaseConnection) []devlock.Protected { return nil },
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			dev := connectSQLiteFile(c, filepath.Join(dir, "dev.db"))
			other := connectSQLiteFile(c, filepath.Join(dir, "other.db"))

			err := devlock.EnsureDistinct(c.Context(), dev, test.protected(dir, other)...)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestEnsureDistinct_FailurePath refuses a dev database that is a protected
// one, whichever way the protected database is given and wherever it sits in
// the list, with the refusal its entry carries.
func TestEnsureDistinct_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		protected func(dir string, same *dbschema.DatabaseConnection) []devlock.Protected
	}{
		{
			name: "the same database by connection",
			protected: func(_ string, same *dbschema.DatabaseConnection) []devlock.Protected {
				return []devlock.Protected{{Conn: same, Refusal: errIsTheDevDatabase}}
			},
		},
		{
			name: "the same database by URL",
			protected: func(dir string, _ *dbschema.DatabaseConnection) []devlock.Protected {
				return []devlock.Protected{{URL: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db")), Refusal: errIsTheDevDatabase}}
			},
		},
		{
			name: "the same database after a distinct one",
			protected: func(dir string, same *dbschema.DatabaseConnection) []devlock.Protected {
				return []devlock.Protected{
					{URL: atlasurl.SQLiteURLFromPath(filepath.Join(dir, "other.db")), Refusal: errors.New("not this one")},
					{Conn: same, Refusal: errIsTheDevDatabase},
				}
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			dev := connectSQLiteFile(c, filepath.Join(dir, "dev.db"))
			same := connectSQLiteFile(c, filepath.Join(dir, "dev.db"))

			err := devlock.EnsureDistinct(c.Context(), dev, test.protected(dir, same)...)

			c.Assert(err, qt.ErrorIs, errIsTheDevDatabase)
		})
	}
}

// TestEnsureDistinct_FailsClosedWhenAProtectedDatabaseCannotBeReached keeps a
// comparison that did not happen from reading as one that found two
// databases.
func TestEnsureDistinct_FailsClosedWhenAProtectedDatabaseCannotBeReached(t *testing.T) {
	c := qt.New(t)
	dev := connectSQLiteFile(c, filepath.Join(c.TempDir(), "dev.db"))

	err := devlock.EnsureDistinct(c.Context(), dev, devlock.Protected{
		URL:     "db2://localhost/app",
		Refusal: errIsTheDevDatabase,
	})

	c.Assert(err, qt.ErrorMatches, `compare the dev database with a database it must not be: .*unsupported database dialect: db2`)
}
