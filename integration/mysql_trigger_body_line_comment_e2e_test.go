//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A MySQL trigger body may end in a line comment, written before the
// statement's semicolon; the catalog keeps the comment as part of the body.
// Written on the comment's line, the migration file's semicolon is part of the
// comment, the next statement runs into the trigger, and MySQL 8.4 refuses the
// file with Error 1064 near the next CREATE TRIGGER.

// triggerBodyLineCommentSchema ends trg's body with a line comment. The
// trigger after it is the statement the comment would swallow.
const triggerBodyLineCommentSchema = `CREATE TABLE t (id int PRIMARY KEY, a int);
CREATE TABLE u (id int PRIMARY KEY);
CREATE TRIGGER trg BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1 -- note
;
CREATE TRIGGER trg2 BEFORE INSERT ON u FOR EACH ROW SET NEW.id = NEW.id + 0;
`

// TestMigrationsUpTriggerBodyLineCommentE2E generates a migration for an
// empty database with `ptah migrations generate`, runs it with `ptah
// migrations up`, and reads both triggers back from the catalog.
func TestMigrationsUpTriggerBodyLineCommentE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, url := scratch.database(c, "trigger_comment")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(triggerBodyLineCommentSchema), 0o600), qt.IsNil)
			migrations := filepath.Join(c.TempDir(), "migrations")

			runPtahNative(c, "migrations", "generate", "--db-url", url, "--schema-file", schema,
				"--migrations-dir", migrations, "--name", "init")
			out, err := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))

			rows, err := scratch.admin.QueryContext(c.Context(),
				"SELECT TRIGGER_NAME FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? ORDER BY TRIGGER_NAME", name)
			c.Assert(err, qt.IsNil)
			defer func() { c.Check(rows.Close(), qt.IsNil) }()
			var triggers []string
			for rows.Next() {
				var trigger string
				c.Assert(rows.Scan(&trigger), qt.IsNil)
				triggers = append(triggers, trigger)
			}
			c.Assert(rows.Err(), qt.IsNil)
			c.Assert(triggers, qt.DeepEquals, []string{"trg", "trg2"})
		})
	}
}
