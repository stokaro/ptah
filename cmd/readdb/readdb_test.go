package readdb_test

import (
	"bytes"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/cmd/readdb"
)

func TestReadDBCommand_StdoutIsExecutableSQL(t *testing.T) {
	c := qt.New(t)

	sourcePath := filepath.Join(t.TempDir(), "source.db")
	source, err := sql.Open("sqlite", sourcePath)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(source.Close(), qt.IsNil) })
	_, err = source.Exec(`
CREATE TABLE left_nodes (
    id INTEGER PRIMARY KEY,
    right_id INTEGER,
    CONSTRAINT fk_left_right FOREIGN KEY (right_id) REFERENCES right_nodes(id)
);
CREATE TABLE right_nodes (
    id INTEGER PRIMARY KEY,
    left_id INTEGER,
    CONSTRAINT fk_right_left FOREIGN KEY (left_id) REFERENCES left_nodes(id)
);`)
	c.Assert(err, qt.IsNil)

	cmd := readdb.NewReadDBCommand()
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--db-url", "sqlite://" + sourcePath})

	err = cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, `CREATE TABLE "left_nodes"`)
	c.Assert(stdout.String(), qt.Contains, `CREATE TABLE "right_nodes"`)
	c.Assert(stdout.String(), qt.Contains, `REFERENCES "right_nodes" ("id")`)
	c.Assert(stdout.String(), qt.Not(qt.Contains), "Reading schema from database:")
	c.Assert(stdout.String(), qt.Not(qt.Contains), "Connected to sqlite database successfully!")
	c.Assert(stdout.String(), qt.Not(qt.Contains), ";;")
	c.Assert(stderr.String(), qt.Contains, "Reading schema from database:")
	c.Assert(stderr.String(), qt.Contains, "Connected to sqlite database successfully!")

	target, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "target.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(target.Close(), qt.IsNil) })
	_, err = target.Exec(stdout.String())
	c.Assert(err, qt.IsNil, qt.Commentf("rendered SQL:\n%s", stdout.String()))

	var tableCount int
	err = target.QueryRow(`SELECT COUNT(*) FROM sqlite_schema WHERE type = 'table' AND name IN ('left_nodes', 'right_nodes')`).Scan(&tableCount)
	c.Assert(err, qt.IsNil)
	c.Assert(tableCount, qt.Equals, 2)
	c.Assert(strings.Count(stdout.String(), "REFERENCES"), qt.Equals, 2)
}
