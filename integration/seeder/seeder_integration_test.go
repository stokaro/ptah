//go:build integration

package seeder_test

import (
	"os"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/seeder"
)

func TestApply_Integration(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), requireSeederDatabaseURL(t))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	_, err = conn.ExecContext(t.Context(), "DROP TABLE IF EXISTS schema_seeds")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), "DROP TABLE IF EXISTS seed_countries")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(t.Context(), "CREATE TABLE seed_countries (code VARCHAR(8) PRIMARY KEY, name VARCHAR(255) NOT NULL)")
	c.Assert(err, qt.IsNil)

	fsys := fstest.MapFS{
		"010_countries.all.sql":       {Data: []byte("INSERT INTO seed_countries (code, name) VALUES ('CZ', 'Czechia');")},
		"020_test_countries.test.sql": {Data: []byte("INSERT INTO seed_countries (code, name) VALUES ('US', 'United States');")},
		"020_dev_countries.dev.sql":   {Data: []byte("INSERT INTO seed_countries (code, name) VALUES ('DE', 'Germany');")},
	}

	_, err = seeder.Apply(t.Context(), conn, fsys, seeder.Options{Env: "test", ProtectedTables: []string{"seed_countries"}})
	c.Assert(err, qt.ErrorMatches, `refusing to seed target database because protected tables exist: seed_countries; pass --allow-prod to override`)

	result, err := seeder.Apply(t.Context(), conn, fsys, seeder.Options{Env: "test"})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Total, qt.Equals, 2)
	c.Assert(seedNames(result.Applied), qt.DeepEquals, []string{"010_countries.all.sql", "020_test_countries.test.sql"})

	result, err = seeder.Apply(t.Context(), conn, fsys, seeder.Options{Env: "test"})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Applied, qt.HasLen, 0)
	c.Assert(seedNames(result.Skipped), qt.DeepEquals, []string{"010_countries.all.sql", "020_test_countries.test.sql"})

	result, err = seeder.Apply(t.Context(), conn, fsys, seeder.Options{Env: "test", Force: true, Idempotent: true})
	c.Assert(err, qt.IsNil)
	c.Assert(seedNames(result.Applied), qt.DeepEquals, []string{"010_countries.all.sql", "020_test_countries.test.sql"})

	var rowCount int
	c.Assert(conn.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM seed_countries").Scan(&rowCount), qt.IsNil)
	c.Assert(rowCount, qt.Equals, 2)

	var seedCount int
	c.Assert(conn.QueryRowContext(t.Context(), "SELECT COUNT(*) FROM schema_seeds").Scan(&seedCount), qt.IsNil)
	c.Assert(seedCount, qt.Equals, 2)
}

func requireSeederDatabaseURL(t *testing.T) string {
	t.Helper()
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	return dbURL
}

func seedNames(seeds []seeder.SeedFile) []string {
	names := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		names = append(names, seed.Filename)
	}
	return names
}
