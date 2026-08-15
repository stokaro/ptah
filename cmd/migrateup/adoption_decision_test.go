package migrateup_test

import (
	"context"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlasurl"
)

// TestMigrateUpAdoptsNothingAndSaysSoByFailing pins the decision recorded at
// [migrateup.NewMigrateUpCommand]: this surface has no adoption gate, so a
// database that already holds objects this history never created is not
// refused. The run starts, and the first statement whose object exists stops
// it.
//
// The test exists so the decision cannot change by accident. A gate added here
// would turn this failure into a refusal before anything ran, and this reddens
// -- sending whoever added it to the reasoning rather than to a merge conflict.
//
// The compatibility surface answers the opposite way, and deliberately: see
// TestCompatMigrateApply* for the gate stokaro/ptah#1252 measured against the
// pinned community binary.
func TestMigrateUpAdoptsNothingAndSaysSoByFailing(t *testing.T) {
	c := qt.New(t)
	migrationsDir := writeUpMigrations(t)
	dbPath := filepath.Join(t.TempDir(), "adoption.db")
	createUsersTableOutsideMigrations(c, dbPath)

	out, err := runUp("--db-url", atlasurl.SQLiteURLFromPath(dbPath), "--migrations-dir", migrationsDir)

	// Not a refusal: the run reached the migration and executed its statement.
	c.Assert(err, qt.ErrorMatches, `(?s).*failed to apply migration 1.*users already exists.*`,
		qt.Commentf("%s", out))
	c.Assert(queryCurrentVersion(c, dbPath), qt.Equals, int64(0))
}

// createUsersTableOutsideMigrations gives the database the one object the first
// migration creates, without recording any revision for it -- the shape an
// operator adopting an existing schema arrives with.
func createUsersTableOutsideMigrations(c *qt.C, dbPath string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), atlasurl.SQLiteURLFromPath(dbPath))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	_, err = conn.ExecContext(context.Background(), "CREATE TABLE users (id INTEGER PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
}
