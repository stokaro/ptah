package migrateup_test

import (
	"database/sql"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

const operationID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// logReferences reads run_reference and actor_origin off every log row.
func logReferences(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(), "SELECT run_reference, actor_origin FROM schema_migrations_log ORDER BY seq")
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var found []string
	for rows.Next() {
		var reference, origin sql.NullString
		c.Assert(rows.Scan(&reference, &origin), qt.IsNil)
		found = append(found, reference.String+" "+origin.String)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return found
}

// The reference the caller passed is on every row the run wrote, and the
// document reports it as stored.
func TestMigrateUpJSONReportsTheRunReference_HappyPath(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(c.TempDir(), "reference.db")

	out, human, err := runUpSplit("--db-url", "sqlite://"+dbPath, "--migrations-dir", writeUpMigrations(t),
		"--actor", "alice@example.com", "--actor-origin", "admission", "--run-reference", operationID, "--json")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", human))
	result := decodeRunResult(c, out)
	c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeApplied)
	c.Assert(result.MigrationLog, qt.DeepEquals, &migrator.MigrationLogReport{Written: 4, RunReference: operationID})
	reference := operationID + " admission"
	c.Assert(logReferences(c, dbPath), qt.DeepEquals, []string{reference, reference, reference, reference})
}

// A value the log could not store as given is refused before the database is
// opened, so nothing is applied, no log is written, and the SQLite file is
// never created.
func TestMigrateUpRefusesARunAttributionTheLogCannotStore_FailurePath(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		wantError string
	}{
		{
			name:      "a reference with a space",
			args:      []string{"--run-reference", "run 7"},
			wantError: `--run-reference: invalid run attribution: .*`,
		},
		{
			name:      "an origin with no actor",
			args:      []string{"--actor-origin", "admission"},
			wantError: `--actor-origin says where the --actor name came from, and no --actor was given`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(c.TempDir(), "refused.db")
			args := append([]string{"--db-url", "sqlite://" + dbPath, "--migrations-dir", writeUpMigrations(t), "--json"}, test.args...)

			out, _, err := runUpSplit(args...)

			c.Assert(err, qt.ErrorMatches, test.wantError)
			result := decodeRunResult(c, out)
			c.Assert(result.Outcome, qt.Equals, migrator.RunOutcomeFailed)
			c.Assert(result.Applied, qt.HasLen, 0)
			c.Assert(result.MigrationLog, qt.IsNil)
			_, statErr := os.Stat(dbPath)
			c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
		})
	}
}
