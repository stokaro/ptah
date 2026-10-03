//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A view or trigger body qualifies its columns with the tables and aliases it
// reads. `schema apply --dev-url` rehearses such a plan on the dev database
// before it touches the target, and reads u in u.id as a column's table rather
// than as a database (stokaro/ptah#4027).
const qualifiedColumnsSchema = `CREATE TABLE users (id int PRIMARY KEY, active int);
CREATE TABLE log (v int);
CREATE VIEW active_users AS SELECT u.id, users2.active FROM users u JOIN users AS users2 ON users2.id = u.id WHERE u.active = 1;
CREATE TRIGGER users_audit AFTER INSERT ON users FOR EACH ROW INSERT INTO log (v) SELECT s.id FROM users s WHERE s.id = NEW.id;
`

// TestSchemaApplyRehearsesQualifiedColumnsOnTheDevDatabaseE2E applies the
// schema with --dev-url to an empty database, inserts a row, and reads back
// what the view and the trigger did.
func TestSchemaApplyRehearsesQualifiedColumnsOnTheDevDatabaseE2E(t *testing.T) {
	for _, engine := range mysqlDevServerEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.admin)
			name, target := scratch.database(c, "qualified_target")
			_, dev := newMySQLFamilyScratch(c, engine.dev).database(c, "qualified_dev")
			file := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(file, []byte(qualifiedColumnsSchema), 0o600), qt.IsNil)

			out, err := runPtahNativeWithError("schema", "apply", "--db-url", target, "--schema-file", file,
				"--dev-url", dev, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			conn, err := sql.Open("mysql", mySQLDSNForDatabase(c, scratch.adminDSN, name))
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(conn.Close(), qt.IsNil) }()
			_, err = conn.ExecContext(c.Context(), "INSERT INTO users VALUES (1, 1), (2, 0)")
			c.Assert(err, qt.IsNil)
			var active, logged int
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM active_users").Scan(&active), qt.IsNil)
			c.Assert(conn.QueryRowContext(c.Context(), "SELECT COUNT(*) FROM log").Scan(&logged), qt.IsNil)

			c.Assert(active, qt.Equals, 1)
			c.Assert(logged, qt.Equals, 2)
		})
	}
}
