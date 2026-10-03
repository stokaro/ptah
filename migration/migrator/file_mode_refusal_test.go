package migrator_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/migration/migrator"
)

// TestFileModeRefusal_HappyPath covers a database whose DDL a transaction
// holds: a trigger definition, which MySQL and MariaDB refuse in transaction
// mode file, runs inside one on SQLite, so nothing is refused.
func TestFileModeRefusal_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(c.Context(), "sqlite://"+filepath.Join(c.TempDir(), "refusal.db"))
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	err = migrator.FileModeRefusal(c.Context(), conn,
		"CREATE TABLE t (id INTEGER PRIMARY KEY);\n"+
			"CREATE TRIGGER t_audit AFTER INSERT ON t BEGIN SELECT 1; END;\n")

	c.Assert(err, qt.IsNil)
}

// TestFileModeRefusal_FailurePath is the question asked without a database to
// answer it.
func TestFileModeRefusal_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := migrator.FileModeRefusal(c.Context(), nil, "CREATE TABLE t (id INTEGER PRIMARY KEY);")

	c.Assert(err, qt.ErrorMatches, "file mode refusal requires a database connection")
}
