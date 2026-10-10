package introspect_test

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite" // registers the SQLite driver the fixture is written with

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/introspect"
)

func TestCanceledIntrospectionPublishesNoGoFiles(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	dir := c.TempDir()
	output := filepath.Join(dir, "generated")
	cmd := introspect.NewIntrospectCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--db-url", atlasurl.SQLiteURLFromPath(filepath.Join(dir, "db.sqlite")), "--out", output})
	err := cmd.ExecuteContext(ctx)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(stdout.String(), qt.Equals, "")
	_, err = os.Stat(output)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

// TestIntrospectionLeavesAVirtualTableOutAndSaysSo writes no struct for a
// SQLite virtual table, which Go annotations cannot declare, and names it on
// standard error. The ordinary table beside it is the control: the models are
// still written.
func TestIntrospectionLeavesAVirtualTableOutAndSaysSo(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	database := filepath.Join(dir, "db.sqlite")
	db, err := sql.Open("sqlite", database)
	c.Assert(err, qt.IsNil)
	_, err = db.ExecContext(c.Context(), `CREATE TABLE users (id INTEGER PRIMARY KEY); CREATE VIRTUAL TABLE docs USING fts5(title, body)`)
	c.Assert(err, qt.IsNil)
	c.Assert(db.Close(), qt.IsNil)
	output := filepath.Join(dir, "generated")
	cmd := introspect.NewIntrospectCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	cmd.SetArgs([]string{"--db-url", atlasurl.SQLiteURLFromPath(database), "--out", output, "--single-file"})

	err = cmd.ExecuteContext(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(stdout.String(), qt.Contains, "Imported 1 table(s)")
	c.Assert(stderr.String(), qt.Equals, `note: "docs" is a SQLite virtual table (module fts5), which Go annotations cannot declare,`+
		` so the models leave it out and a schema built from them leaves it in place; ptah db read writes its CREATE VIRTUAL TABLE statement`+"\n")
	models, err := os.ReadDir(output)
	c.Assert(err, qt.IsNil)
	c.Assert(models, qt.HasLen, 1)
	source, err := os.ReadFile(filepath.Join(output, models[0].Name()))
	c.Assert(err, qt.IsNil)
	c.Assert(string(source), qt.Contains, `//ptah:schema:table name="users"`)
	c.Assert(string(source), qt.Not(qt.Contains), `name="docs"`)
}
