package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// singleStatementTrigger is a MySQL trigger whose body is one statement rather
// than a BEGIN ... END block.
const singleStatementTrigger = "CREATE TABLE a (id int PRIMARY KEY, n int);\n" +
	"CREATE TRIGGER tr BEFORE INSERT ON a FOR EACH ROW SET NEW.n = NEW.id;"

// TestRead_SingleStatementTrigger_HappyPath reads a trigger whose body is one
// statement, which MySQL and MariaDB run and Atlas CE v1.3.0 reports synced
// with the database the file builds. The reader refused it for want of BEGIN
// (stokaro/ptah#3915).
func TestRead_SingleStatementTrigger_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb", ""} {
		t.Run("dialect "+dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(singleStatementTrigger), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Triggers, qt.HasLen, 1)
			c.Assert(database.Triggers[0].Body, qt.Equals, "SET NEW.n = NEW.id")
		})
	}
}

// TestRead_SingleStatementTrigger_FailurePath keeps SQLite's BEGIN ... END:
// its trigger body is always a block.
func TestRead_SingleStatementTrigger_FailurePath(t *testing.T) {
	c := qt.New(t)

	_, _, err := sqlschema.Read([]byte(singleStatementTrigger), "sqlite")

	c.Assert(err, qt.ErrorMatches, `(?s).*expected trigger body BEGIN.*`)
}
