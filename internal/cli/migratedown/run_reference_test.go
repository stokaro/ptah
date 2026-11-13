package migratedown_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

const operationID = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// downReferences reads run_reference and actor_origin off the rollback's rows.
func downReferences(c *qt.C, dbPath string) []string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	rows, err := conn.QueryContext(c.Context(),
		"SELECT run_reference, actor_origin FROM schema_migrations_log WHERE operation = 'down' ORDER BY seq")
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

// A rollback stores the caller's reference and origin on its rows as an apply
// does.
func TestMigrateDownStoresTheRunReference_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := seedForLostRollback(c, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm",
		"--actor", "alice@example.com", "--actor-origin", "admission", "--run-reference", operationID)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	reference := operationID + " admission"
	c.Assert(downReferences(c, dbPath), qt.DeepEquals, []string{reference, reference})
}

// A reference the log could not store as given is refused before the
// rollback, which leaves the applied migration where it was.
func TestMigrateDownRefusesARunReferenceTheLogCannotStore_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir, dbPath := seedForLostRollback(c, "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")

	out, err := runDown("--db-url", "sqlite://"+dbPath, "--migrations-dir", dir, "--target", "0", "--confirm",
		"--run-reference", "run 7")

	c.Assert(err, qt.ErrorMatches, `--run-reference: invalid run attribution: .*`, qt.Commentf("%s", out))
	c.Assert(downReferences(c, dbPath), qt.HasLen, 0)
}
