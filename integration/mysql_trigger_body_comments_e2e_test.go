//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// MySQL and MariaDB keep a trigger's body as it arrives, comments included.
// The pinned community binary sends a statement with the comments inside it,
// and stores `SET NEW.a = 1 -- note` with its note. Sent with every comment
// taken out, the stored body loses the note, and `ptah migrations generate`
// compares the declaration's body with it and plans the trigger again on
// every run.

// commentedTriggerSchema declares two triggers whose bodies carry comments:
// one after the last token, before the semicolon, and one inside a block.
const commentedTriggerSchema = `CREATE TABLE t (id int PRIMARY KEY, a int);
CREATE TRIGGER trg_note BEFORE INSERT ON t FOR EACH ROW SET NEW.a = 1 -- note
;
CREATE TRIGGER trg_block BEFORE UPDATE ON t FOR EACH ROW BEGIN
  -- inner note
  SET NEW.a = 2;
END;
`

// TestMigrationsUpKeepsTriggerBodyCommentsE2E generates a migration from the
// schema for an empty database, runs it with `ptah migrations up`, reads the
// stored bodies back, and generates again: the database matches the schema,
// so nothing is planned.
func TestMigrationsUpKeepsTriggerBodyCommentsE2E(t *testing.T) {
	for _, engine := range mysqlCommentEngines {
		t.Run(engine.name, func(t *testing.T) {
			c := qt.New(t)
			scratch := newMySQLFamilyScratch(c, engine.engine)
			name, url := scratch.database(c, "trigger_notes")
			schema := filepath.Join(c.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(schema, []byte(commentedTriggerSchema), 0o600), qt.IsNil)
			migrations := filepath.Join(c.TempDir(), "migrations")

			runPtahNative(c, "migrations", "generate", "--db-url", url, "--schema-file", schema,
				"--migrations-dir", migrations, "--name", "init")
			out, err := runPtahNativeWithError("migrations", "up", "--db-url", url, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			again, err := runPtahNativeWithError("migrations", "generate", "--db-url", url, "--schema-file", schema,
				"--migrations-dir", migrations, "--name", "again")

			c.Assert(triggerBodies(c, scratch, name), qt.DeepEquals, []string{
				"trg_block: BEGIN | -- inner note | SET NEW.a = 2; | END",
				"trg_note: SET NEW.a = 1 -- note",
			})
			c.Assert(err, qt.IsNil, qt.Commentf("%s", again))
			c.Assert(again, qt.Contains, "Schema is synced")
		})
	}
}

// triggerBodies reads the bodies of the triggers in database name, as
// `trigger: body` with each line trimmed and the lines joined by ` | `.
func triggerBodies(c *qt.C, scratch mysqlScratch, name string) []string {
	c.Helper()
	rows, err := scratch.admin.QueryContext(c.Context(),
		"SELECT TRIGGER_NAME, ACTION_STATEMENT FROM information_schema.TRIGGERS WHERE TRIGGER_SCHEMA = ? ORDER BY TRIGGER_NAME", name)
	c.Assert(err, qt.IsNil)
	defer func() { c.Check(rows.Close(), qt.IsNil) }()
	var bodies []string
	for rows.Next() {
		var trigger, body string
		c.Assert(rows.Scan(&trigger, &body), qt.IsNil)
		lines := strings.Split(body, "\n")
		for i := range lines {
			lines[i] = strings.TrimSpace(lines[i])
		}
		bodies = append(bodies, trigger+": "+strings.Join(lines, " | "))
	}
	c.Assert(rows.Err(), qt.IsNil)
	return bodies
}
