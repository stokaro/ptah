//go:build integration

package integration_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// TestMigrationFileKeepsDeclaredTextInsideItsComments generates a migration
// file from a schema whose table comment holds a line break and a DROP TABLE,
// applies the file, and reads the database back. The text reaches a comment
// on SQLite, which keeps no table comment, and a string literal on PostgreSQL,
// which does. On neither does any part of it run: the table the comment names
// survives, and the PostgreSQL comment reads back whole.
func TestMigrationFileKeepsDeclaredTextInsideItsComments(t *testing.T) {
	const declared = "first\nDROP TABLE victim;\n-- end"
	tests := []struct {
		engine      string
		victimQuery string
		readBack    string
		want        string
	}{
		{engine: "sqlite", victimQuery: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'victim'",
			readBack: "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = 'ro_c'", want: "1"},
		{engine: "postgres", victimQuery: "SELECT count(*) FROM information_schema.tables WHERE table_name = 'victim'",
			readBack: "SELECT obj_description('ro_c'::regclass)", want: declared},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	defer cancel()
	binary := filepath.Join(t.TempDir(), "ptah")
	buildPtah(qt.New(t), ctx, e2eRepoRoot(t), binary)
	for _, test := range tests {
		t.Run(test.engine, func(t *testing.T) {
			c := qt.New(t)
			dbURL := renderOmissionDatabases[test.engine](c)
			dir := c.TempDir()
			victim := "CREATE TABLE victim (id INT PRIMARY KEY);\n"
			victimSchema := filepath.Join(dir, "victim.sql")
			c.Assert(os.WriteFile(victimSchema, []byte(victim), 0o600), qt.IsNil)
			fullSchema := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(fullSchema, []byte(victim+
				"CREATE TABLE ro_c (id INT PRIMARY KEY) COMMENT='"+declared+"';\n"), 0o600), qt.IsNil)
			migrations := filepath.Join(dir, "migrations")
			c.Assert(os.Mkdir(migrations, 0o700), qt.IsNil)

			stdout, stderr, err := runCLIProcess(ctx, dir, binary, "schema", "apply", "--db-url", dbURL, "--schema-file", victimSchema, "--auto-approve")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrations", "generate", "--db-url", dbURL,
				"--schema-file", fullSchema, "--migrations-dir", migrations, "--name", "first")
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			stdout, stderr, err = runCLIProcess(ctx, dir, binary, "migrations", "up", "--db-url", dbURL, "--migrations-dir", migrations)
			c.Assert(err, qt.IsNil, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))

			c.Assert(readOmissionBack(c, ctx, dbURL, test.victimQuery), qt.Equals, "1")
			c.Assert(readOmissionBack(c, ctx, dbURL, test.readBack), qt.Equals, test.want)
		})
	}
}
